package account

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrSessionNotFound = errors.New("checkout session not found")
	ErrSessionExpired  = errors.New("checkout session has expired")
)

const defaultCheckoutTTL = 15 * time.Minute

// CreateCheckout initiates a 15-minute dynamic checkout session.
// Leases a unique 10-digit virtual account number with prefix (e.g. 6930xxxxxx),
// stores the 15-minute lease in Redis, and creates a pending session in PostgreSQL.
func (s *Service) CreateCheckout(ctx context.Context, req CreateCheckoutRequest) (*CheckoutResponse, error) {
	if req.ExpectedAmount <= 0 {
		return nil, fmt.Errorf("%w: expected_amount must be greater than 0", ErrInvalidInput)
	}

	currency := strings.ToUpper(strings.TrimSpace(req.Currency))
	if currency == "" {
		currency = "NGN"
	}

	accNumber, err := s.generator.GenerateAccountNumber(ctx)
	if err != nil {
		return nil, err
	}

	sessionID := fmt.Sprintf("ses_%s", newRandomToken())
	now := time.Now().UTC()
	expiresAt := now.Add(defaultCheckoutTTL)

	// Save pending checkout session in PostgreSQL
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO checkout_sessions (id, user_id, account_number, expected_amount, currency, status, paid_amount, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, 'PENDING', 0, $6, $7)
	`, sessionID, req.UserID, accNumber, req.ExpectedAmount, currency, expiresAt, now)
	if err != nil {
		return nil, fmt.Errorf("save checkout session: %w", err)
	}

	// Set 15-minute lease in Redis: checkout:account:<account_number>
	lease := RedisCheckoutLease{
		SessionID:      sessionID,
		UserID:         req.UserID,
		AccountNumber:  accNumber,
		ExpectedAmount: req.ExpectedAmount,
		Currency:       currency,
		ExpiresAt:      expiresAt,
	}
	leaseJSON, _ := json.Marshal(lease)

	redisKey := fmt.Sprintf("checkout:account:%s", accNumber)
	if err := s.redis.Set(ctx, redisKey, leaseJSON, defaultCheckoutTTL).Err(); err != nil {
		return nil, fmt.Errorf("set redis lease: %w", err)
	}

	sessionKey := fmt.Sprintf("checkout:session:%s", sessionID)
	_ = s.redis.Set(ctx, sessionKey, accNumber, defaultCheckoutTTL).Err()

	return &CheckoutResponse{
		SessionID:        sessionID,
		BankName:         "Wema Bank",
		AccountNumber:    accNumber,
		ExpectedAmount:   req.ExpectedAmount,
		PaidAmount:       0,
		Currency:         currency,
		ExpiresInSeconds: int64(defaultCheckoutTTL.Seconds()),
		ExpiresAt:        expiresAt,
		Status:           "PENDING",
	}, nil
}

// GetCheckoutStatus checks the live status of a 15-minute checkout session.
func (s *Service) GetCheckoutStatus(ctx context.Context, sessionID string) (*CheckoutResponse, error) {
	var (
		userID         sql.NullString
		accountNumber  string
		expectedAmount int64
		currency       string
		status         string
		paidAmount     int64
		expiresAt      time.Time
	)

	err := s.db.QueryRowContext(ctx, `
		SELECT user_id, account_number, expected_amount, currency, status, paid_amount, expires_at
		FROM checkout_sessions
		WHERE id = $1
	`, sessionID).Scan(&userID, &accountNumber, &expectedAmount, &currency, &status, &paidAmount, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSessionNotFound
		}
		return nil, fmt.Errorf("query checkout session: %w", err)
	}

	now := time.Now().UTC()
	remaining := int64(expiresAt.Sub(now).Seconds())
	if remaining < 0 {
		remaining = 0
	}

	// If pending but expired, update status to EXPIRED
	if status == "PENDING" && now.After(expiresAt) {
		status = "EXPIRED"
		_, _ = s.db.ExecContext(ctx, `UPDATE checkout_sessions SET status = 'EXPIRED' WHERE id = $1 AND status = 'PENDING'`, sessionID)
		_ = s.redis.Del(ctx, fmt.Sprintf("checkout:account:%s", accountNumber)).Err()
		_ = s.redis.Del(ctx, fmt.Sprintf("checkout:session:%s", sessionID)).Err()
	}

	return &CheckoutResponse{
		SessionID:        sessionID,
		BankName:         "Wema Bank",
		AccountNumber:    accountNumber,
		ExpectedAmount:   expectedAmount,
		PaidAmount:       paidAmount,
		Currency:         currency,
		ExpiresInSeconds: remaining,
		ExpiresAt:        expiresAt,
		Status:           status,
	}, nil
}

// ReconcilePayment matches an incoming webhook payment to an active 15-minute checkout session.
func (s *Service) ReconcilePayment(ctx context.Context, accountNumber string, amount int64) (string, error) {
	redisKey := fmt.Sprintf("checkout:account:%s", accountNumber)
	val, err := s.redis.Get(ctx, redisKey).Result()

	var sessionID string
	var expectedAmount int64

	if err == nil && val != "" {
		var lease RedisCheckoutLease
		if err := json.Unmarshal([]byte(val), &lease); err == nil {
			sessionID = lease.SessionID
			expectedAmount = lease.ExpectedAmount
		}
	}

	// Fallback to DB check if Redis lease expired just now
	if sessionID == "" {
		err := s.db.QueryRowContext(ctx, `
			SELECT id, expected_amount
			FROM checkout_sessions
			WHERE account_number = $1 AND status = 'PENDING'
			ORDER BY created_at DESC
			LIMIT 1
		`, accountNumber).Scan(&sessionID, &expectedAmount)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", nil
			}
			return "", err
		}
	}

	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `
		UPDATE checkout_sessions
		SET status = 'PAID', paid_amount = $1, paid_at = $2
		WHERE id = $3
	`, amount, now, sessionID)
	if err != nil {
		return "", fmt.Errorf("update checkout session to paid: %w", err)
	}

	_ = s.redis.Del(ctx, redisKey).Err()
	_ = s.redis.Del(ctx, fmt.Sprintf("checkout:session:%s", sessionID)).Err()

	return sessionID, nil
}
