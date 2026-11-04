// Package migrations встраивает SQL-схему в бинарник и применяет её при старте.
package migrations

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed 001_init.sql
var initSQL string

// Apply создаёт таблицы, если их ещё нет. Схема идемпотентна (IF NOT EXISTS).
func Apply(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, initSQL); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
