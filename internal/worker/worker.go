package worker

import (
	"awesomeProject9/internal/order"
	"context"
	"log/slog"
	"time"
)

type WorkerRepository interface {
	GetBatch(ctx context.Context, attempts, limit int) ([]order.OutboxRecord, error, string)
	MarkPublished(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id, batchId string) error
}

type Publisher interface {
	Publish(ctx context.Context, eventID string, payload []byte) error
}

type Worker struct {
	repo        WorkerRepository
	pub         Publisher
	batchSize   int
	maxAttempts int
	interval    time.Duration
}

func NewWorker(repo WorkerRepository, pub Publisher, batchSize int, maxAttempts int, interval time.Duration) *Worker {
	if batchSize <= 0 {
		batchSize = 10
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	if interval <= 0 {
		interval = time.Second * 3
	}

	return &Worker{
		repo:        repo,
		pub:         pub,
		batchSize:   batchSize,
		maxAttempts: maxAttempts,
		interval:    interval,
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {
				slog.Error("error while processing batch", "err", err)
			}
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) error {
	events, err, lockedBy := w.repo.GetBatch(ctx, w.maxAttempts, w.batchSize)
	if err != nil {
		return err
	}

	for _, v := range events {

		if ctx.Err() != nil {
			slog.Error("error while processing batch", "err", "context canceled")
			return nil
		}

		pubCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		pubErr := w.pub.Publish(pubCtx, v.EventID, v.Payload)
		cancel()
		if pubErr != nil {
			err = w.repo.MarkFailed(ctx, v.EventID, lockedBy)
		} else {
			err = w.repo.MarkPublished(ctx, v.EventID)
		}
		if err != nil {
			return err
		}
	}

	return nil
}
