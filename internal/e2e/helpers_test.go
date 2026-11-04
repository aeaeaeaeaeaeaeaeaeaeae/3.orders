// Package e2e — интеграционные тесты сценариев сбоя на реальном PostgreSQL.
//
// Запуск:  TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/orders_test?sslmode=disable go test ./internal/e2e/ -v
// Без TEST_DATABASE_URL тесты пропускаются.
//
// Каждая проверка смотрит на состояние таблиц orders и outbox и на счётчики
// аналитики, а не на коды ответов.
package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"awesomeProject9/internal/analytics"
	"awesomeProject9/internal/order"
	"awesomeProject9/internal/worker"
	"awesomeProject9/migrations"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type env struct {
	db    *sql.DB
	svc   *order.OrderService
	an    *analytics.Analytics
	wrepo *worker.WorkerRepo
}

func setup(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`TRUNCATE orders, outbox`); err != nil {
		t.Fatal(err)
	}
	return &env{
		db:    db,
		svc:   order.NewOrderService(order.NewOrderRepository(db, 3*time.Second)),
		an:    analytics.New(),
		wrepo: worker.NewWorkerRepo(db, 3*time.Second),
	}
}

func (e *env) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// state — снимок «заказ / событие / аналитика» для проверок и логов.
type state struct {
	Orders, OutboxTotal, OutboxPending, Processed, DeadLetters int
	StatOrders, StatItems                                      int64
}

func (e *env) state(t *testing.T) state {
	t.Helper()
	s := state{
		Orders:        e.count(t, `SELECT count(*) FROM orders`),
		OutboxTotal:   e.count(t, `SELECT count(*) FROM outbox`),
		OutboxPending: e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`),
		Processed:     e.an.Processed(),
		DeadLetters:   len(e.an.DeadLetters()),
	}
	s.StatOrders, s.StatItems = e.an.Stats()
	t.Logf("state: %+v", s)
	return s
}

func (e *env) expectState(t *testing.T, want state) {
	t.Helper()
	if got := e.state(t); got != want {
		t.Fatalf("state mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

// expireLeases эмулирует ход времени: истекают аренды и backoff.
func (e *env) expireLeases(t *testing.T) {
	t.Helper()
	if _, err := e.db.Exec(`UPDATE outbox SET locked_until = NULL WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
}

// runWorkerUntil крутит настоящий воркер, пока не выполнится условие.
func (e *env) runWorkerUntil(t *testing.T, pub worker.Publisher, maxAttempts int, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := worker.NewWorker(e.wrepo, pub, 10, maxAttempts, 10*time.Millisecond)
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("worker did not reach the expected state in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) createOrder(t *testing.T, user, key string, items ...order.Item) order.Order {
	t.Helper()
	o, err := e.svc.OrderHandling(context.Background(), order.Order{UserID: user, IdempotencyKey: key, Items: items})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	return o
}

type pubFunc func(ctx context.Context, id string, payload []byte) error

func (f pubFunc) Publish(ctx context.Context, id string, payload []byte) error {
	return f(ctx, id, payload)
}

func newUser() string { return uuid.NewString() }

func eventJSON(t *testing.T, ev map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func validEvent() map[string]any {
	return map[string]any{
		"event_id":       uuid.NewString(),
		"schema_version": 1,
		"order_id":       uuid.NewString(),
		"occurred_at":    time.Now().UTC(),
		"items":          []map[string]any{{"sku": "BRK-1", "quantity": 2}},
	}
}
