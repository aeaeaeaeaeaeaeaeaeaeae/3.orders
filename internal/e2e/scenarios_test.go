package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"awesomeProject9/internal/analytics"
	"awesomeProject9/internal/order"
	"awesomeProject9/internal/worker"
)

var errAnalyticsDown = errors.New("analytics unavailable")

func downPublisher(calls *atomic.Int32) pubFunc {
	return func(context.Context, string, []byte) error {
		calls.Add(1)
		return errAnalyticsDown
	}
}

// 1. Аналитика недоступна, но новые заказы продолжают сохраняться.
func TestScenario1_AnalyticsDown(t *testing.T) {
	e := setup(t)
	user := newUser()
	for i, key := range []string{"k1", "k2", "k3"} {
		e.createOrder(t, user, key, order.Item{SKU: "BRK-1", Quantity: i + 1})
	}

	var calls atomic.Int32
	e.runWorkerUntil(t, downPublisher(&calls), 10, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE attempts >= 1`) == 3
	})

	// Заказы есть, события ждут в outbox с причиной, аналитика не тронута.
	e.expectState(t, state{Orders: 3, OutboxTotal: 3, OutboxPending: 3})
	if n := e.count(t, `SELECT count(*) FROM outbox WHERE last_error = $1`, errAnalyticsDown.Error()); n != 3 {
		t.Fatalf("last_error must be recorded, got %d rows", n)
	}

	// Аналитика вернулась: всё накопленное доезжает.
	e.expireLeases(t)
	e.runWorkerUntil(t, analytics.LocalPublisher{Analytics: e.an}, 10, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	e.expectState(t, state{Orders: 3, OutboxTotal: 3, OutboxPending: 0, Processed: 3, StatOrders: 3, StatItems: 1 + 2 + 3})
}

// Постоянная ошибка отправки: после maxAttempts событие перестаёт браться,
// остаётся в outbox с last_error и может быть переотправлено вручную.
func TestPermanentPublishFailure_StopsAfterMaxAttempts(t *testing.T) {
	e := setup(t)
	e.createOrder(t, newUser(), "k1", order.Item{SKU: "BRK-1", Quantity: 1})

	const maxAttempts = 3
	var calls atomic.Int32
	for e.count(t, `SELECT count(*) FROM outbox WHERE attempts >= $1`, maxAttempts) == 0 {
		before := calls.Load()
		e.expireLeases(t) // не ждём backoff
		e.runWorkerUntil(t, downPublisher(&calls), maxAttempts, func() bool { return calls.Load() > before })
	}

	// Ещё ~20 циклов воркера: «мёртвое» событие больше не берётся.
	before := calls.Load()
	e.expireLeases(t)
	started := time.Now()
	e.runWorkerUntil(t, downPublisher(&calls), maxAttempts, func() bool {
		return time.Since(started) > 200*time.Millisecond
	})
	if calls.Load() != before {
		t.Fatalf("dead event must not be retried, calls %d -> %d", before, calls.Load())
	}
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, OutboxPending: 1})

	// Ручная переотправка после починки.
	if _, err := e.db.Exec(`UPDATE outbox SET attempts = 0, locked_until = NULL WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	e.runWorkerUntil(t, analytics.LocalPublisher{Analytics: e.an}, maxAttempts, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, Processed: 1, StatOrders: 1, StatItems: 1})
}

// 2. OrderService остановился после фиксации заказа до ответа клиенту; клиент повторил запрос.
func TestScenario2_CrashAfterCommitClientRetries(t *testing.T) {
	e := setup(t)
	user := newUser()

	first := e.createOrder(t, user, "k1", order.Item{SKU: "A", Quantity: 1}, order.Item{SKU: "B", Quantity: 2})
	_ = first // ответ «не дошёл» до клиента

	// Повтор с тем же составом (порядок позиций другой).
	retry := e.createOrder(t, user, "k1", order.Item{SKU: "B", Quantity: 2}, order.Item{SKU: "A", Quantity: 1})
	if retry.ID != first.ID {
		t.Fatalf("retry must return the same order: %s != %s", retry.ID, first.ID)
	}
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, OutboxPending: 1})

	e.runWorkerUntil(t, analytics.LocalPublisher{Analytics: e.an}, 10, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, Processed: 1, StatOrders: 1, StatItems: 3})
}

// 3. Одновременно пришли запросы с одним ключом: сначала одинаковые, затем — с разными параметрами.
func TestScenario3_ConcurrentSameKey(t *testing.T) {
	e := setup(t)
	user := newUser()
	const n = 20

	run := func(key string, itemsFor func(i int) []order.Item) ([]string, []error) {
		ids, errs := make([]string, n), make([]error, n)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				<-start
				o, err := e.svc.OrderHandling(context.Background(),
					order.Order{UserID: user, IdempotencyKey: key, Items: itemsFor(i)})
				ids[i], errs[i] = o.ID, err
			})
		}
		close(start)
		wg.Wait()
		return ids, errs
	}

	// Одинаковые параметры: все получают один и тот же заказ.
	same := []order.Item{{SKU: "A", Quantity: 1}}
	ids, errs := run("k1", func(int) []order.Item { return same })
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("request %d got order %s, want %s", i, ids[i], ids[0])
		}
	}
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, OutboxPending: 1})

	// Тот же ключ, другой состав: все отклонены, новых заказов нет.
	_, errs = run("k1", func(int) []order.Item { return []order.Item{{SKU: "A", Quantity: 9}} })
	for i := range n {
		if !errors.Is(errs[i], order.ErrIdempotencyMismatch) {
			t.Fatalf("request %d: want ErrIdempotencyMismatch, got %v", i, errs[i])
		}
	}

	// Гонка на новом ключе с разными составами: ровно один победитель.
	ids, errs = run("k2", func(i int) []order.Item { return []order.Item{{SKU: "A", Quantity: 1 + i%2}} })
	winners := map[string]bool{}
	for i := range n {
		switch {
		case errs[i] == nil:
			winners[ids[i]] = true
		case !errors.Is(errs[i], order.ErrIdempotencyMismatch):
			t.Fatalf("request %d: unexpected error %v", i, errs[i])
		}
	}
	if len(winners) != 1 {
		t.Fatalf("want exactly one order for k2, got %d", len(winners))
	}
	e.expectState(t, state{Orders: 2, OutboxTotal: 2, OutboxPending: 2})
}

// Разные новые заказы с одинаковым составом дают разные события и считаются оба.
func TestSameItemsDifferentOrders_AreDifferentEvents(t *testing.T) {
	e := setup(t)
	user := newUser()
	e.createOrder(t, user, "k1", order.Item{SKU: "A", Quantity: 1})
	e.createOrder(t, user, "k2", order.Item{SKU: "A", Quantity: 1})

	if n := e.count(t, `SELECT count(DISTINCT event_id) FROM outbox`); n != 2 {
		t.Fatalf("want 2 distinct events, got %d", n)
	}
	e.runWorkerUntil(t, analytics.LocalPublisher{Analytics: e.an}, 10, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	e.expectState(t, state{Orders: 2, OutboxTotal: 2, Processed: 2, StatOrders: 2, StatItems: 2})
}

// 4. Отправитель передал событие, но не сохранил подтверждение публикации.
func TestScenario4_PublishedButNotMarked(t *testing.T) {
	e := setup(t)
	e.createOrder(t, newUser(), "k1", order.Item{SKU: "A", Quantity: 2})
	ctx := context.Background()

	// Воркер арендовал событие и отправил его...
	events, err, _ := e.wrepo.GetBatch(ctx, 10, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("GetBatch: %v, %d events", err, len(events))
	}
	pub := analytics.LocalPublisher{Analytics: e.an}
	if err := pub.Publish(ctx, events[0].EventID, events[0].Payload); err != nil {
		t.Fatal(err)
	}
	// ...и упал до MarkPublished.
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, OutboxPending: 1, Processed: 1, StatOrders: 1, StatItems: 2})

	// Пока аренда действует, другие воркеры событие не берут.
	again, err, _ := e.wrepo.GetBatch(ctx, 10, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("leased event must be skipped: %v, %d events", err, len(again))
	}

	// Аренда истекла: событие отправлено повторно, дубль отсечён.
	e.expireLeases(t)
	var calls atomic.Int32
	counting := pubFunc(func(ctx context.Context, id string, p []byte) error {
		calls.Add(1)
		return pub.Publish(ctx, id, p)
	})
	e.runWorkerUntil(t, counting, 10, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	if calls.Load() != 1 {
		t.Fatalf("want exactly one redelivery, got %d", calls.Load())
	}
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, Processed: 1, StatOrders: 1, StatItems: 2})
}

// 5. Аналитика применила событие и остановилась до подтверждения доставки.
func TestScenario5_AppliedButAckLost(t *testing.T) {
	e := setup(t)
	e.createOrder(t, newUser(), "k1", order.Item{SKU: "A", Quantity: 3})

	var calls atomic.Int32
	ackLost := pubFunc(func(ctx context.Context, _ string, p []byte) error {
		if err := e.an.Apply(ctx, p); err != nil {
			return err
		}
		if calls.Add(1) == 1 {
			return errors.New("connection reset before ack") // применено, но отправитель этого не узнал
		}
		return nil
	})

	e.runWorkerUntil(t, ackLost, 10, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	if calls.Load() != 2 {
		t.Fatalf("want 2 deliveries, got %d", calls.Load())
	}
	if n := e.count(t, `SELECT attempts FROM outbox`); n != 1 {
		t.Fatalf("want attempts = 1, got %d", n)
	}
	e.expectState(t, state{Orders: 1, OutboxTotal: 1, Processed: 1, StatOrders: 1, StatItems: 3})
}

// 6. Повтор доставки, неразбираемое событие, нарушение бизнес-контракта.
func TestScenario6_DuplicateMalformedContractViolation(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	apply := func(p []byte) {
		t.Helper()
		if err := e.an.Apply(ctx, p); err != nil {
			t.Fatalf("Apply must accept the message, got %v", err)
		}
	}
	lastReason := func() string {
		dl := e.an.DeadLetters()
		return dl[len(dl)-1].Reason
	}

	// Повтор: учтён один раз.
	p := eventJSON(t, validEvent())
	apply(p)
	apply(p)
	e.expectState(t, state{Processed: 1, StatOrders: 1, StatItems: 2})

	// Не разбирается: отложено вместе с исходными байтами для повторного разбора.
	bad := []byte(`{"event_id": "oops", not json`)
	apply(bad)
	if lastReason() != analytics.ReasonMalformed || string(e.an.DeadLetters()[0].Raw) != string(bad) {
		t.Fatalf("malformed event must be parked with its raw bytes: %+v", e.an.DeadLetters())
	}

	// Нарушен контракт.
	ev := validEvent()
	ev["items"] = []map[string]any{{"sku": "A", "quantity": 0}}
	apply(eventJSON(t, ev))
	if lastReason() != analytics.ReasonContractViolation {
		t.Fatalf("want contract_violation, got %s", lastReason())
	}

	// Неизвестная версия схемы.
	ev = validEvent()
	ev["schema_version"] = 2
	apply(eventJSON(t, ev))
	if lastReason() != analytics.ReasonUnknownVersion {
		t.Fatalf("want unknown_version, got %s", lastReason())
	}

	// Счётчики не изменились ни от одного плохого сообщения.
	e.expectState(t, state{Processed: 1, DeadLetters: 3, StatOrders: 1, StatItems: 2})
}

// 6 (сквозной). Плохое событие из outbox паркуется и не ретраится бесконечно.
func TestScenario6_BadEventFromOutboxIsParked(t *testing.T) {
	e := setup(t)
	ev := validEvent()
	ev["items"] = []map[string]any{}
	if _, err := e.db.Exec(`INSERT INTO outbox (event_id, payload) VALUES ($1, $2)`,
		ev["event_id"], eventJSON(t, ev)); err != nil {
		t.Fatal(err)
	}

	e.runWorkerUntil(t, analytics.LocalPublisher{Analytics: e.an}, 10, func() bool {
		return e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) == 0
	})
	if n := e.count(t, `SELECT attempts FROM outbox`); n != 0 {
		t.Fatalf("permanent error must not be retried, attempts = %d", n)
	}
	e.expectState(t, state{OutboxTotal: 1, DeadLetters: 1})
}

// 7. Аналитика отстаёт, очередь растёт, а новые заказы продолжают поступать.
func TestScenario7_AnalyticsLagging(t *testing.T) {
	e := setup(t)
	user := newUser()
	ctx := context.Background()

	// Медленная аналитика: 20 мс на событие.
	slow := pubFunc(func(ctx context.Context, id string, p []byte) error {
		time.Sleep(20 * time.Millisecond)
		return e.an.Apply(ctx, p)
	})

	ctxW, cancel := context.WithCancel(ctx)
	w := worker.NewWorker(e.wrepo, slow, 10, 10, 10*time.Millisecond)
	done := make(chan struct{})
	go func() { w.Run(ctxW); close(done) }()
	defer func() { cancel(); <-done }()

	// Пока воркер не успевает, заказы создаются без задержки.
	const total = 60
	for i := range total {
		start := time.Now()
		e.createOrder(t, user, fmt.Sprintf("k%d", i), order.Item{SKU: "A", Quantity: 1})
		if d := time.Since(start); d > time.Second {
			t.Fatalf("order %d took %s: order path must not depend on analytics", i, d)
		}
	}
	pending := e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`)
	t.Logf("backlog right after the burst: %d of %d", pending, total)
	if pending == 0 {
		t.Fatal("expected a backlog while analytics is slow")
	}

	// Пачка ограничена: за раз воркер арендует не больше batchSize.
	leased := e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL AND locked_until > now()`)
	if leased > 10 {
		t.Fatalf("worker leased %d events at once, batch limit is 10", leased)
	}

	// Очередь рассасывается, ничего не потеряно.
	deadline := time.Now().Add(15 * time.Second)
	for e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("backlog was not drained")
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.expectState(t, state{Orders: total, OutboxTotal: total, Processed: total, StatOrders: total, StatItems: total})
}
