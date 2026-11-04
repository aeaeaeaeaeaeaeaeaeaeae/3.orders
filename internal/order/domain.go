package order

import (
	"errors"
	"time"
)

var (
	ErrScanningTheRows     = errors.New("error while scanning the rows")
	ErrNotFound            = errors.New("error not found")
	ErrIdempotencyMismatch = errors.New("idempotency mismatch")
	ErrBeginTx             = errors.New("error while begin tx")
	ErrFailedToCommit      = errors.New("error while commiting")
	ErrConflict            = errors.New("idempotency key already exists")
	ErrWhileRowsAffected   = errors.New("error while calling rows affected method")
	ErrOutboxSaving        = errors.New("error while saving order and outbox event data")
	ErrJsonUnmarshal       = errors.New("error while unmarshalling json")
	ErrExecutingQuery      = errors.New("error while executing query")
)

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Order struct {
	ID             string // uuid.UUID
	UserID         string // uuid.UUID
	IdempotencyKey string
	RequestHash    []byte
	Items          []Item
	CreatedAt      time.Time
}

// Контракт события
type OrderCreated struct {
	EventID       string    `json:"event_id"`       // uuid
	SchemaVersion int       `json:"schema_version"` // 1
	OrderID       string    `json:"order_id"`       // uuid.UUID
	OccurredAt    time.Time `json:"occurred_at"`
	Items         []Item    `json:"items"`
}

type OutboxRecord struct {
	EventID     string // uuid.UUID
	Payload     []byte
	Attempts    int
	PublishedAt *time.Time
}
