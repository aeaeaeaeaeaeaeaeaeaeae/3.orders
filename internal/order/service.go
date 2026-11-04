package order

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

type OrderRepository interface {
	FindByIdempotencyKey(ctx context.Context, userId, idempotencyKey string) (Order, error)
	OutboxOrder(ctx context.Context, outboxEvent OutboxRecord, order Order) (Order, error)
}

type OrderService struct {
	repo OrderRepository
}

func NewOrderService(repo OrderRepository) *OrderService {
	return &OrderService{repo: repo}
}

func requestHash(items []Item) ([]byte, error) {
	merged := make(map[string]int, len(items))
	for _, it := range items {
		merged[it.SKU] += it.Quantity
	}

	canon := make([]Item, 0, len(merged))
	for sku, qty := range merged {
		canon = append(canon, Item{SKU: sku, Quantity: qty})
	}
	sort.Slice(canon, func(i, j int) bool { return canon[i].SKU < canon[j].SKU })

	b, err := json.Marshal(canon)
	if err != nil {
		return nil, err
	}

	sum := sha256.Sum256(b)
	return sum[:], nil
}

func createEvent(order Order) (OutboxRecord, error) {
	now := time.Now()
	eventId := uuid.NewString()
	event := OrderCreated{
		EventID:       eventId,
		SchemaVersion: 1,
		OrderID:       order.ID,
		OccurredAt:    now,
		Items:         order.Items,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return OutboxRecord{}, fmt.Errorf("%w: %w", ErrJsonUnmarshal, err)
	}

	outboxEvent := OutboxRecord{
		EventID: eventId,
		Payload: payload,
	}

	return outboxEvent, nil
}

func (o *OrderService) OrderHandling(ctx context.Context, order Order) (Order, error) {
	hash, err := requestHash(order.Items)
	if err != nil {
		return Order{}, err
	}

	order.ID = uuid.NewString()
	outboxRecord, err := createEvent(order)
	if err != nil {
		return Order{}, err
	}
	order.RequestHash = hash
	outboxOrder, outboxErr := o.repo.OutboxOrder(ctx, outboxRecord, order)
	if outboxErr != nil {
		if errors.Is(outboxErr, ErrConflict) {

			foundOrder, findError := o.repo.FindByIdempotencyKey(ctx, order.UserID, order.IdempotencyKey)
			if findError != nil {
				return Order{}, fmt.Errorf("error while looking for order: %w", findError)
			}

			if !bytes.Equal(foundOrder.RequestHash, hash) {
				return Order{}, ErrIdempotencyMismatch
			}

			return foundOrder, nil
		}
		return Order{}, fmt.Errorf("%w: %w", ErrOutboxSaving, outboxErr)
	}
	return outboxOrder, nil
}
