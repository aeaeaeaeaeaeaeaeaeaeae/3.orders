package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"awesomeProject9/internal/analytics"
	"awesomeProject9/internal/httpapi"
	"awesomeProject9/internal/order"
	"awesomeProject9/internal/worker"
	"awesomeProject9/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable")
	addr := env("ADDR", ":8080")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetMaxOpenConns(envInt("DB_MAX_CONNS", 20))
	db.SetMaxIdleConns(10)

	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if err := migrations.Apply(ctx, db); err != nil {
		return err
	}

	orderSvc := order.NewOrderService(order.NewOrderRepository(db, 3*time.Second))
	an := analytics.New()

	outboxWorker := worker.NewWorker(
		worker.NewWorkerRepo(db, 3*time.Second),
		analytics.LocalPublisher{Analytics: an},
		envInt("OUTBOX_BATCH_SIZE", 10),
		envInt("OUTBOX_MAX_ATTEMPTS", 10),
		time.Second,
	)

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(orderSvc, envInt("MAX_IN_FLIGHT", 100)).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Go(func() { outboxWorker.Run(ctx) })

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
	case err := <-serverErr:
		stop()
		wg.Wait()
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "err", err)
	}
	wg.Wait()
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}
