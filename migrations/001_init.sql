CREATE TABLE IF NOT EXISTS orders (
    id               uuid        PRIMARY KEY,
    user_id          uuid        NOT NULL,
    idempotency_key  text        NOT NULL,
    request_hash     bytea       NOT NULL,   -- sha256 канонизированного состава
    items            jsonb       NOT NULL,   -- [{"sku":"A","quantity":2}]
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key)
);

-- Пишется в той же транзакции, что и orders.
CREATE TABLE IF NOT EXISTS outbox (
    event_id      uuid        PRIMARY KEY,   -- постоянный ID события
    payload       jsonb       NOT NULL,      -- готовое событие order.created
    attempts      int         NOT NULL DEFAULT 0,
    published_at  timestamptz,               -- NULL = ещё не доставлено
    locked_until  timestamptz,               -- аренда воркером / время следующей попытки
    locked_by     text,
    last_error    text,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (event_id)
    WHERE published_at IS NULL;
