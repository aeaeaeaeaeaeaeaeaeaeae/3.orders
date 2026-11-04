// Package httpapi — HTTP-вход OrderService.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"awesomeProject9/internal/order"

	"github.com/google/uuid"
)

const maxBodyBytes = 64 << 10

type OrderCreator interface {
	OrderHandling(ctx context.Context, o order.Order) (order.Order, error)
}

type Handler struct {
	orders OrderCreator
	slots  chan struct{}
}

func New(orders OrderCreator, maxInFlight int) *Handler {
	if maxInFlight <= 0 {
		maxInFlight = 100
	}
	return &Handler{
		orders: orders,
		slots:  make(chan struct{}, maxInFlight),
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", h.createOrder)
	return mux
}

type createOrderRequest struct {
	Items []order.Item `json:"items"`
}

type orderResponse struct {
	OrderID string       `json:"order_id"`
	Items   []order.Item `json:"items"`
}

func (h *Handler) createOrder(w http.ResponseWriter, r *http.Request) {
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "too many concurrent requests")
		return
	}

	userID, err := uuid.Parse(r.Header.Get("X-User-ID"))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "missing or invalid X-User-ID")
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	var req createOrderRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := validateItems(req.Items); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	created, err := h.orders.OrderHandling(r.Context(), order.Order{
		UserID:         userID.String(),
		IdempotencyKey: key,
		Items:          req.Items,
	})
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, orderResponse{OrderID: created.ID, Items: created.Items})
	case errors.Is(err, order.ErrIdempotencyMismatch):
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key was already used with different items")
	case errors.Is(r.Context().Err(), context.Canceled):
		return
	default:
		slog.Error("create order", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func validateItems(items []order.Item) error {
	if len(items) == 0 {
		return errors.New("items must not be empty")
	}
	for _, it := range items {
		if it.SKU == "" || it.Quantity <= 0 {
			return errors.New("each item needs a sku and a positive quantity")
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
