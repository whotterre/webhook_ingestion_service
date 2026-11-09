package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/whotterre/webhook_ingestion_service/internal/events"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
)

var (
	ErrDuplicateEvent = errors.New("event already processed")
)

type Store interface {
	CommitPaymentEvent(ctx context.Context, evt *events.PaymentEvent) error
	GetAccountBalance(ctx context.Context, accountID string) (int64, string, error)
	Close() error
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(databaseURL string) (*PostgresStore, error) {
	if databaseURL == "" {
		return nil, errors.New("postgres connection url is required")
	}

	_ = ensureDatabaseExists(databaseURL)

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store := &PostgresStore{db: db}
	if err := store.autoMigrate(ctx); err != nil {
		log.Printf("[ledger] warning: auto-migration error: %v", err)
	}

	return store, nil
}

func ensureDatabaseExists(databaseURL string) error {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return nil
	}

	targetDb := strings.TrimPrefix(u.Path, "/")
	if targetDb == "" || targetDb == "postgres" {
		return nil
	}

	adminURL := *u
	adminURL.Path = "/postgres"

	adminDb, err := sql.Open("postgres", adminURL.String())
	if err != nil {
		return nil
	}
	defer adminDb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := adminDb.PingContext(ctx); err != nil {
		return nil
	}

	var exists bool
	_ = adminDb.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", targetDb).Scan(&exists)
	if !exists {
		log.Printf("[ledger] database %q does not exist, creating it automatically...", targetDb)
		_, err = adminDb.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %q", targetDb))
		if err != nil {
			log.Printf("[ledger] warning: failed to create database %q: %v", targetDb, err)
		} else {
			log.Printf("[ledger] database %q created successfully", targetDb)
		}
	}
	return nil
}

func (s *PostgresStore) autoMigrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS accounts (
		account_id VARCHAR(128) PRIMARY KEY,
		user_id VARCHAR(128),
		account_number VARCHAR(32) UNIQUE,
		balance BIGINT NOT NULL DEFAULT 0,
		currency VARCHAR(8) NOT NULL DEFAULT 'NGN',
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

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

	CREATE TABLE IF NOT EXISTS idempotency_keys (
		key VARCHAR(255) PRIMARY KEY,
		event_id VARCHAR(128) NOT NULL,
		scope VARCHAR(64) NOT NULL DEFAULT 'ledger',
		status VARCHAR(32) NOT NULL DEFAULT 'COMMITTED',
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS ledger_entries (
		id BIGSERIAL PRIMARY KEY,
		event_id VARCHAR(128) UNIQUE NOT NULL,
		payment_id VARCHAR(128) NOT NULL,
		account_id VARCHAR(128) NOT NULL REFERENCES accounts(account_id),
		event_type VARCHAR(64) NOT NULL,
		sequence INTEGER NOT NULL DEFAULT 1,
		amount BIGINT NOT NULL,
		currency VARCHAR(8) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_ledger_entries_account ON ledger_entries(account_id);
	CREATE INDEX IF NOT EXISTS idx_ledger_entries_payment ON ledger_entries(payment_id);
	`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *PostgresStore) CommitPaymentEvent(ctx context.Context, evt *events.PaymentEvent) error {
	if evt == nil {
		return errors.New("payment event is nil")
	}
	if evt.AccountID == "" || evt.EventID == "" {
		return errors.New("event_id and account_id are required")
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		metrics.LedgerCommitsTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	idempotencyKey := fmt.Sprintf("idemp:ledger:%s", evt.EventID)

	// Check & insert idempotency key atomically
	var existingKey string
	err = tx.QueryRowContext(ctx,
		"SELECT key FROM idempotency_keys WHERE key = $1 FOR UPDATE", idempotencyKey,
	).Scan(&existingKey)

	if err == nil {
		metrics.LedgerCommitsTotal.WithLabelValues("duplicate").Inc()
		return ErrDuplicateEvent
	} else if !errors.Is(err, sql.ErrNoRows) {
		metrics.LedgerCommitsTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("check idempotency key: %w", err)
	}

	// Ensure account exists or insert with 0 balance
	currency := evt.Currency
	if currency == "" {
		currency = "NGN"
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO accounts (account_id, balance, currency, updated_at)
		VALUES ($1, 0, $2, NOW())
		ON CONFLICT (account_id) DO NOTHING
	`, evt.AccountID, currency)
	if err != nil {
		metrics.LedgerCommitsTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("upsert account: %w", err)
	}

	// Reconcile matching 15-minute dynamic checkout session (if active)
	if evt.EventType == events.PaymentSucceeded || evt.EventType == events.PaymentCreated {
		now := time.Now().UTC()
		_, _ = tx.ExecContext(ctx, `
			UPDATE checkout_sessions
			SET status = 'PAID', paid_amount = $1, paid_at = $2
			WHERE account_number = $3 AND status = 'PENDING'
		`, evt.Amount, now, evt.AccountID)
	}

	// Calculate balance delta based on event type
	var delta int64
	switch evt.EventType {
	case events.PaymentSucceeded, events.PaymentCreated:
		delta = evt.Amount
	case events.PaymentRefunded, events.ChargebackCreated:
		delta = -evt.Amount
	case events.PaymentFailed:
		delta = 0
	default:
		delta = 0
	}

	if delta != 0 {
		_, err = tx.ExecContext(ctx, `
			UPDATE accounts
			SET balance = balance + $1, updated_at = NOW()
			WHERE account_id = $2
		`, delta, evt.AccountID)
		if err != nil {
			metrics.LedgerCommitsTotal.WithLabelValues("failed").Inc()
			return fmt.Errorf("update account balance: %w", err)
		}
	}

	// Record ledger entry
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (
			event_id, payment_id, account_id, event_type, sequence, amount, currency, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, evt.EventID, evt.PaymentID, evt.AccountID, string(evt.EventType), evt.Sequence, evt.Amount, currency, time.Now())
	if err != nil {
		metrics.LedgerCommitsTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("insert ledger entry: %w", err)
	}

	// Insert idempotency key
	_, err = tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys (key, event_id, scope, status, updated_at)
		VALUES ($1, $2, 'ledger', 'COMMITTED', NOW())
	`, idempotencyKey, evt.EventID)
	if err != nil {
		metrics.LedgerCommitsTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("insert idempotency key: %w", err)
	}

	if err := tx.Commit(); err != nil {
		metrics.LedgerCommitsTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("commit tx: %w", err)
	}

	metrics.LedgerCommitsTotal.WithLabelValues("success").Inc()
	return nil
}

func (s *PostgresStore) GetAccountBalance(ctx context.Context, accountID string) (int64, string, error) {
	var balance int64
	var currency string
	err := s.db.QueryRowContext(ctx,
		"SELECT balance, currency FROM accounts WHERE account_id = $1", accountID,
	).Scan(&balance, &currency)
	if err != nil {
		return 0, "", err
	}
	return balance, currency, nil
}

func (s *PostgresStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
