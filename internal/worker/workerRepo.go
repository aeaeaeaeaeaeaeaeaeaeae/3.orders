package worker

import (
	"awesomeProject9/internal/order"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type WorkerRepo struct {
	db        *sql.DB
	dbTimeout time.Duration
}

func NewWorkerRepo(db *sql.DB, dbTimeout time.Duration) *WorkerRepo {
	if dbTimeout <= 0 {
		dbTimeout = time.Second * 3
	}
	return &WorkerRepo{
		dbTimeout: dbTimeout,
		db:        db,
	}
}

func (w *WorkerRepo) GetBatch(ctx context.Context, attempts, limit int) ([]order.OutboxRecord, error, string) {
	const query = `UPDATE outbox SET locked_until = now() + interval '60 seconds', locked_by = $1 WHERE event_id IN (SELECT event_id FROM outbox WHERE published_at IS NULL AND attempts < $2
    			   AND (locked_until IS NULL OR locked_until < now()) ORDER BY event_id LIMIT $3 FOR UPDATE SKIP LOCKED) RETURNING event_id, payload`
	lockedBy := uuid.NewString()

	ctx, cancel := context.WithTimeout(ctx, w.dbTimeout)
	defer cancel()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return []order.OutboxRecord{}, fmt.Errorf("%w: %w", order.ErrBeginTx, err), lockedBy
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, query, lockedBy, attempts, limit)
	if err != nil {
		return []order.OutboxRecord{}, fmt.Errorf("%w: %w", order.ErrExecutingQuery, err), lockedBy
	}
	defer rows.Close()

	var events []order.OutboxRecord
	for rows.Next() {
		var event order.OutboxRecord
		if err = rows.Scan(&event.EventID, &event.Payload); err != nil {
			return []order.OutboxRecord{}, fmt.Errorf("%w: %w", order.ErrScanningTheRows, err), lockedBy
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return []order.OutboxRecord{}, fmt.Errorf("%w: %w", order.ErrScanningTheRows, err), lockedBy
	}

	if err := tx.Commit(); err != nil {
		return []order.OutboxRecord{}, fmt.Errorf("%w: %w", order.ErrFailedToCommit, err), lockedBy
	}

	return events, nil, lockedBy
}

func (w *WorkerRepo) MarkPublished(ctx context.Context, id string) error {
	const query = `UPDATE outbox SET published_at = now(), locked_until = NULL, locked_by = NULL WHERE event_id = $1`

	ctx, cancel := context.WithTimeout(ctx, w.dbTimeout)
	defer cancel()

	_, err := w.db.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}

	return nil
}
func (w *WorkerRepo) MarkFailed(ctx context.Context, id, batchId string) error {
	const query = `UPDATE outbox SET attempts = attempts + 1, locked_until = now() + (2 ^ LEAST(attempts, 6)) * interval '1 second', locked_by = NULL WHERE event_id = $1 AND locked_by = $2`

	ctx, cancel := context.WithTimeout(ctx, w.dbTimeout)
	defer cancel()

	_, err := w.db.ExecContext(ctx, query, id, batchId)
	if err != nil {
		return err
	}

	return nil
}
