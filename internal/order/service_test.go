package order

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeRepo повторяет контракт OrderRepo: уникальность по (user_id, idempotency_key).
type fakeRepo struct {
	mu     sync.Mutex
	orders map[string]Order
	outbox []OutboxRecord
}

func newFakeRepo() *fakeRepo { return &fakeRepo{orders: map[string]Order{}} }

func (f *fakeRepo) FindByIdempotencyKey(_ context.Context, userID, key string) (Order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.orders[userID+"/"+key]
	if !ok {
		return Order{}, ErrNotFound
	}
	return o, nil
}

func (f *fakeRepo) OutboxOrder(_ context.Context, rec OutboxRecord, o Order) (Order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := o.UserID + "/" + o.IdempotencyKey
	if _, ok := f.orders[k]; ok {
		return Order{}, ErrConflict
	}
	f.orders[k] = o
	f.outbox = append(f.outbox, rec)
	return o, nil
}

func TestRequestHash_IgnoresOrderAndMergesDuplicates(t *testing.T) {
	a, _ := requestHash([]Item{{"A", 1}, {"B", 2}})
	b, _ := requestHash([]Item{{"B", 2}, {"A", 1}})
	c, _ := requestHash([]Item{{"B", 1}, {"A", 1}, {"B", 1}})
	if !bytes.Equal(a, b) || !bytes.Equal(a, c) {
		t.Fatal("same composition must give the same hash")
	}
	d, _ := requestHash([]Item{{"A", 1}, {"B", 3}})
	if bytes.Equal(a, d) {
		t.Fatal("different composition must give a different hash")
	}
}

func TestOrderHandling_RetryReturnsSameOrder(t *testing.T) {
	repo := newFakeRepo()
	svc := NewOrderService(repo)
	ctx := context.Background()
	in := Order{UserID: "u1", IdempotencyKey: "k1", Items: []Item{{"A", 1}, {"B", 2}}}

	first, err := svc.OrderHandling(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	// повтор с тем же составом в другом порядке
	in.Items = []Item{{"B", 2}, {"A", 1}}
	second, err := svc.OrderHandling(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("retry created a new order: %s != %s", first.ID, second.ID)
	}
	if len(repo.orders) != 1 || len(repo.outbox) != 1 {
		t.Fatalf("want 1 order and 1 event, got %d and %d", len(repo.orders), len(repo.outbox))
	}
}

func TestOrderHandling_SameKeyDifferentItemsRejected(t *testing.T) {
	repo := newFakeRepo()
	svc := NewOrderService(repo)
	ctx := context.Background()

	if _, err := svc.OrderHandling(ctx, Order{UserID: "u1", IdempotencyKey: "k1", Items: []Item{{"A", 1}}}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.OrderHandling(ctx, Order{UserID: "u1", IdempotencyKey: "k1", Items: []Item{{"A", 5}}})
	if !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("want ErrIdempotencyMismatch, got %v", err)
	}
	if len(repo.orders) != 1 {
		t.Fatalf("want 1 order, got %d", len(repo.orders))
	}
}

func TestOrderHandling_EventCarriesOrderID(t *testing.T) {
	repo := newFakeRepo()
	svc := NewOrderService(repo)

	o, err := svc.OrderHandling(context.Background(), Order{UserID: "u1", IdempotencyKey: "k1", Items: []Item{{"A", 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(repo.outbox[0].Payload, []byte(o.ID)) {
		t.Fatal("event payload must contain the order id")
	}
}
