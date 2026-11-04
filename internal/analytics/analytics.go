// Package analytics — симуляция сервиса аналитики в памяти.
//
// Считает число заказов и количество товаров, отсекает повторы по event_id,
// а неразбираемые события и события с нарушенным контрактом откладывает
// в DeadLetters вместе с исходными байтами, чтобы их можно было разобрать повторно.
package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	ReasonMalformed         = "malformed"
	ReasonUnknownVersion    = "unknown_version"
	ReasonContractViolation = "contract_violation"
)

// Event — контракт order.created со стороны получателя.
type Event struct {
	EventID       string    `json:"event_id"`
	SchemaVersion int       `json:"schema_version"`
	OrderID       string    `json:"order_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	Items         []Item    `json:"items"`
}

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type DeadLetter struct {
	Raw    []byte
	Reason string
	Err    string
}

type Analytics struct {
	mu          sync.Mutex
	processed   map[string]struct{}
	ordersCount int64
	itemsQty    int64
	deadLetters []DeadLetter
}

func New() *Analytics {
	return &Analytics{processed: map[string]struct{}{}}
}

// Apply применяет одно доставленное событие. nil — сообщение принято:
// учтено, отброшено как дубль или отложено в DeadLetters.
func (a *Analytics) Apply(_ context.Context, payload []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var ev Event
	if err := json.Unmarshal(payload, &ev); err != nil || ev.EventID == "" {
		a.park(payload, ReasonMalformed, fmt.Errorf("cannot decode event: %v", err))
		return nil
	}
	if ev.SchemaVersion != 1 {
		a.park(payload, ReasonUnknownVersion, fmt.Errorf("schema_version %d", ev.SchemaVersion))
		return nil
	}
	if err := validate(ev); err != nil {
		a.park(payload, ReasonContractViolation, err)
		return nil
	}

	if _, dup := a.processed[ev.EventID]; dup {
		return nil
	}
	a.processed[ev.EventID] = struct{}{}
	a.ordersCount++
	for _, it := range ev.Items {
		a.itemsQty += int64(it.Quantity)
	}
	return nil
}

// Stats возвращает текущие счётчики.
func (a *Analytics) Stats() (orders, items int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ordersCount, a.itemsQty
}

// Processed — сколько разных событий учтено.
func (a *Analytics) Processed() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.processed)
}

// DeadLetters возвращает копию отложенных сообщений. Повторный разбор —
// это снова Apply(dl.Raw) после исправления потребителя.
func (a *Analytics) DeadLetters() []DeadLetter {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]DeadLetter(nil), a.deadLetters...)
}

func (a *Analytics) park(raw []byte, reason string, err error) {
	a.deadLetters = append(a.deadLetters, DeadLetter{Raw: raw, Reason: reason, Err: err.Error()})
}

func validate(ev Event) error {
	if ev.OrderID == "" {
		return errors.New("order_id is required")
	}
	if len(ev.Items) == 0 {
		return errors.New("items must not be empty")
	}
	for _, it := range ev.Items {
		if it.SKU == "" || it.Quantity <= 0 {
			return fmt.Errorf("invalid item sku=%q quantity=%d", it.SKU, it.Quantity)
		}
	}
	return nil
}
