package worker

import (
	"awesomeProject9/internal/order"
	"context"
	"errors"
	"testing"
)

type fakeRepo struct {
	batch     []order.OutboxRecord
	published []string
	failed    []string
	lastError string
}

func (f *fakeRepo) GetBatch(context.Context, int, int) ([]order.OutboxRecord, error, string) {
	return f.batch, nil, "worker-1"
}

func (f *fakeRepo) MarkPublished(_ context.Context, id string) error {
	f.published = append(f.published, id)
	return nil
}

func (f *fakeRepo) MarkFailed(_ context.Context, id, _ string, lastError string) error {
	f.failed = append(f.failed, id)
	f.lastError = lastError
	return nil
}

type fakePublisher struct {
	failFor map[string]bool
	calls   int
}

func (p *fakePublisher) Publish(_ context.Context, id string, _ []byte) error {
	p.calls++
	if p.failFor[id] {
		return errors.New("analytics unavailable")
	}
	return nil
}

func TestProcessBatch_MarksSuccessAndFailure(t *testing.T) {
	repo := &fakeRepo{batch: []order.OutboxRecord{{EventID: "ok"}, {EventID: "bad"}}}
	pub := &fakePublisher{failFor: map[string]bool{"bad": true}}
	w := NewWorker(repo, pub, 10, 3, 0)

	if err := w.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.published) != 1 || repo.published[0] != "ok" {
		t.Fatalf("published = %v, want [ok]", repo.published)
	}
	if len(repo.failed) != 1 || repo.failed[0] != "bad" {
		t.Fatalf("failed = %v, want [bad]", repo.failed)
	}
	if repo.lastError != "analytics unavailable" {
		t.Fatalf("last error = %q", repo.lastError)
	}
}

func TestProcessBatch_StopsOnCancelWithoutCountingAttempts(t *testing.T) {
	repo := &fakeRepo{batch: []order.OutboxRecord{{EventID: "a"}, {EventID: "b"}}}
	pub := &fakePublisher{}
	w := NewWorker(repo, pub, 10, 3, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.processBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if pub.calls != 0 || len(repo.failed) != 0 || len(repo.published) != 0 {
		t.Fatalf("after cancel nothing must be sent or marked: calls=%d failed=%v published=%v",
			pub.calls, repo.failed, repo.published)
	}
}
