package analytics

import "context"

type LocalPublisher struct {
	Analytics *Analytics
}

func (p LocalPublisher) Publish(ctx context.Context, _ string, payload []byte) error {
	return p.Analytics.Apply(ctx, payload)
}
