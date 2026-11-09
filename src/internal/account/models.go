package account

import "time"

type SignUpRequest struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Currency string `json:"currency,omitempty"`
}

type SignUpResponse struct {
	UserID        string    `json:"user_id"`
	Email         string    `json:"email"`
	Name          string    `json:"name"`
	AccountID     string    `json:"account_id"`
	AccountNumber string    `json:"account_number"`
	Balance       int64     `json:"balance"`
	Currency      string    `json:"currency"`
	CreatedAt     time.Time `json:"created_at"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginResponse struct {
	UserID        string `json:"user_id"`
	Email         string `json:"email"`
	Name          string `json:"name"`
	AccountID     string `json:"account_id"`
	AccountNumber string `json:"account_number"`
	Balance       int64  `json:"balance"`
	Currency      string `json:"currency"`
	Token         string `json:"token"`
}

type CreateCheckoutRequest struct {
	UserID         string `json:"user_id,omitempty"`
	ExpectedAmount int64  `json:"expected_amount"`
	Currency       string `json:"currency,omitempty"`
	Description    string `json:"description,omitempty"`
}

type CheckoutResponse struct {
	SessionID        string    `json:"session_id"`
	BankName         string    `json:"bank_name"`
	AccountNumber    string    `json:"account_number"`
	ExpectedAmount   int64     `json:"expected_amount"`
	PaidAmount       int64     `json:"paid_amount"`
	Currency         string    `json:"currency"`
	ExpiresInSeconds int64     `json:"expires_in_seconds"`
	ExpiresAt        time.Time `json:"expires_at"`
	Status           string    `json:"status"` // PENDING, PAID, EXPIRED
}

type RedisCheckoutLease struct {
	SessionID      string    `json:"session_id"`
	UserID         string    `json:"user_id"`
	AccountNumber  string    `json:"account_number"`
	ExpectedAmount int64     `json:"expected_amount"`
	Currency       string    `json:"currency"`
	ExpiresAt      time.Time `json:"expires_at"`
}