package order

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type OrderRepo struct {
	db        *sql.DB
	dbTimeout time.Duration
}

func NewOrderRepository(db *sql.DB, dbTimeout time.Duration) *OrderRepo {
	if dbTimeout <= 0 {
		dbTimeout = time.Second * 3
	}

	return &OrderRepo{
		dbTimeout: dbTimeout,
		db:        db,
	}
}

func (r *OrderRepo) FindByIdempotencyKey(ctx context.Context, userId, idempotencyKey string) (Order, error) {
	const query = `SELECT id, user_id, idempotency_key, request_hash, items, created_at FROM orders WHERE idempotency_key = $1 AND user_id = $2`

	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()

	var order Order
	var items []byte
	err := r.db.QueryRowContext(ctx, query, idempotencyKey, userId).Scan(&order.ID, &order.UserID, &order.IdempotencyKey, &order.RequestHash, &items, &order.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Order{}, fmt.Errorf("%w: %w", ErrNotFound, err)
		}
		return Order{}, err
	}

	if err = json.Unmarshal(items, &order.Items); err != nil {
		return Order{}, fmt.Errorf("%w: %w", ErrJsonUnmarshal, err)
	}

	return order, nil
}

func (r *OrderRepo) OutboxOrder(ctx context.Context, outboxEvent OutboxRecord, order Order) (Order, error) {
	const order_query = `INSERT INTO orders (id, user_id, idempotency_key, request_hash, items) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (user_id, idempotency_key) DO NOTHING RETURNING id`
	const outbox_query = `INSERT INTO outbox (event_id, payload, attempts, published_at) VALUES ($1, $2, $3, $4)`
	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Order{}, fmt.Errorf("%w: %w", ErrBeginTx, err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, order_query, order.ID, order.UserID, order.IdempotencyKey, order.RequestHash, order.Items)
	if err != nil {
		return Order{}, fmt.Errorf("%w: %w", ErrExecutingQuery, err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return Order{}, fmt.Errorf("%w: %w", ErrWhileRowsAffected, err)
	}

	if rowsAffected == 0 {
		return Order{}, ErrConflict
	}

	res, err = tx.ExecContext(ctx, outbox_query, outboxEvent.EventID, outboxEvent.Payload, outboxEvent.Attempts, outboxEvent.PublishedAt)
	if err != nil {
		return Order{}, fmt.Errorf("error while executing query: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return Order{}, fmt.Errorf("%w: %w", ErrFailedToCommit, err)
	}
	return order, nil
}
