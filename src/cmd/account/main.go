package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/account"
)

func main() {
	_ = config.Load()

	port := os.Getenv("ACCOUNT_HTTP_PORT")
	if port == "" {
		port = "8082"
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:password@localhost:5432/ledger?sslmode=disable"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatalf("[account-svc] failed to open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("[account-svc] failed to ping db: %v", err)
	}

	// Ensure required tables exist
	if err := migrateSchema(ctx, db); err != nil {
		log.Fatalf("[account-svc] schema migration failed: %v", err)
	}

	// Connect to Redis
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		redisOpts = &redis.Options{Addr: redisURL}
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("[account-svc] failed to ping redis: %v", err)
	}

	// Initialize Account Service & HTTP Routes
	svc := account.NewService(db, rdb)
	handler := account.NewHandler(svc)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Printf("[account-svc] listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[account-svc] server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[account-svc] shutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	log.Println("[account-svc] stopped")
}

func migrateSchema(ctx context.Context, db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id VARCHAR(128) PRIMARY KEY,
		email VARCHAR(255) UNIQUE NOT NULL,
		password_hash VARCHAR(255) NOT NULL,
		name VARCHAR(255) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS accounts (
		account_id VARCHAR(128) PRIMARY KEY,
		user_id VARCHAR(128),
		account_number VARCHAR(32) UNIQUE,
		balance BIGINT NOT NULL DEFAULT 0,
		currency VARCHAR(8) NOT NULL DEFAULT 'NGN',
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

	ALTER TABLE accounts ADD COLUMN IF NOT EXISTS user_id VARCHAR(128);
	ALTER TABLE accounts ADD COLUMN IF NOT EXISTS account_number VARCHAR(32) UNIQUE;

	CREATE TABLE IF NOT EXISTS checkout_sessions (
		id VARCHAR(128) PRIMARY KEY,
		user_id VARCHAR(128),
		account_number VARCHAR(32) NOT NULL,
		expected_amount BIGINT NOT NULL,
		currency VARCHAR(8) NOT NULL DEFAULT 'NGN',
		status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
		paid_amount BIGINT NOT NULL DEFAULT 0,
		paid_at TIMESTAMP WITH TIME ZONE,
		expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_checkout_account ON checkout_sessions(account_number);
	CREATE INDEX IF NOT EXISTS idx_checkout_status ON checkout_sessions(status);

	CREATE TABLE IF NOT EXISTS outbox (
		id BIGSERIAL PRIMARY KEY,
		aggregate_type VARCHAR(64) NOT NULL,
		aggregate_id VARCHAR(128) NOT NULL,
		event_type VARCHAR(64) NOT NULL,
		payload JSONB NOT NULL,
		status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(status, created_at);
	`
	_, err := db.ExecContext(ctx, schema)
	return err
}
