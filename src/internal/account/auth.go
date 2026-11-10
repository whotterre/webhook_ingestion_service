package account

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrUserExists         = errors.New("user with this email already exists")
	ErrInvalidCredentials = errors.New("invalid email or password")
	ErrInvalidInput       = errors.New("invalid input data")
)

func (s *Service) SignUp(ctx context.Context, req SignUpRequest) (*SignUpResponse, error) {
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	req.Name = strings.TrimSpace(req.Name)

	if req.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if _, err := mail.ParseAddress(req.Email); err != nil {
		return nil, fmt.Errorf("%w: invalid email address", ErrInvalidInput)
	}
	if len(req.Password) < 6 {
		return nil, fmt.Errorf("%w: password must be at least 6 characters", ErrInvalidInput)
	}

	currency := strings.ToUpper(strings.TrimSpace(req.Currency))
	if currency == "" {
		currency = "NGN"
	}

	var existingID string
	err := s.db.QueryRowContext(ctx, "SELECT id FROM users WHERE email = $1", req.Email).Scan(&existingID)
	if err == nil {
		return nil, ErrUserExists
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("check existing user: %w", err)
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	accNumber, err := s.generator.GenerateAccountNumber(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	userID := fmt.Sprintf("usr_%s", newRandomToken())
	accountID := fmt.Sprintf("acc_%s", newRandomToken())
	now := time.Now().UTC()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO users (id, email, password_hash, name, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`, userID, req.Email, string(hashedPassword), req.Name, now)
	if err != nil {
		return nil, fmt.Errorf("insert user: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO accounts (account_id, user_id, account_number, balance, currency, created_at, updated_at)
		VALUES ($1, $2, $3, 0, $4, $5, $5)
	`, accountID, userID, accNumber, currency, now)
	if err != nil {
		return nil, fmt.Errorf("insert account: %w", err)
	}

	outboxPayload, _ := json.Marshal(map[string]interface{}{
		"user_id":        userID,
		"account_id":     accountID,
		"account_number": accNumber,
		"currency":       currency,
		"email":          req.Email,
		"created_at":     now,
	})

	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, status, created_at)
		VALUES ($1, $2, $3, $4, 'PENDING', $5)
	`, "account", accountID, "account.created", outboxPayload, now)
	if err != nil {
		return nil, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit signup: %w", err)
	}

	return &SignUpResponse{
		UserID:        userID,
		Email:         req.Email,
		Name:          req.Name,
		AccountID:     accountID,
		AccountNumber: accNumber,
		Balance:       0,
		Currency:      currency,
		CreatedAt:     now,
	}, nil
}

func (s *Service) Login(ctx context.Context, req LoginRequest) (*LoginResponse, error) {
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	if req.Email == "" || req.Password == "" {
		return nil, ErrInvalidCredentials
	}

	var (
		userID       string
		email        string
		passwordHash string
		name         string
	)

	err := s.db.QueryRowContext(ctx, `
		SELECT id, email, password_hash, name
		FROM users
		WHERE email = $1
	`, req.Email).Scan(&userID, &email, &passwordHash, &name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrInvalidCredentials
		}
		return nil, fmt.Errorf("query user: %w", err)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	var (
		accountID     string
		accountNumber string
		balance       int64
		currency      string
	)

	err = s.db.QueryRowContext(ctx, `
		SELECT account_id, COALESCE(account_number, ''), balance, currency
		FROM accounts
		WHERE user_id = $1
		ORDER BY created_at ASC
		LIMIT 1
	`, userID).Scan(&accountID, &accountNumber, &balance, &currency)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("query account: %w", err)
	}

	token := fmt.Sprintf("tk_%s", newRandomToken())

	return &LoginResponse{
		UserID:        userID,
		Email:         email,
		Name:          name,
		AccountID:     accountID,
		AccountNumber: accountNumber,
		Balance:       balance,
		Currency:      currency,
		Token:         token,
	}, nil
}
