package account

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidationErrors(t *testing.T) {
	svc := &Service{}
	ctx := context.Background()

	// Missing name
	_, err := svc.SignUp(ctx, SignUpRequest{
		Email:    "test@example.com",
		Password: "password123",
	})
	if err == nil || !strings.Contains(err.Error(), "name is required") {
		t.Fatalf("expected missing name error, got %v", err)
	}

	// Invalid email
	_, err = svc.SignUp(ctx, SignUpRequest{
		Name:     "Alice",
		Email:    "invalid-email",
		Password: "password123",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid email") {
		t.Fatalf("expected invalid email error, got %v", err)
	}

	// Short password
	_, err = svc.SignUp(ctx, SignUpRequest{
		Name:     "Alice",
		Email:    "alice@example.com",
		Password: "123",
	})
	if err == nil || !strings.Contains(err.Error(), "password must be at least 6 characters") {
		t.Fatalf("expected password length error, got %v", err)
	}

	// Invalid checkout amount
	_, err = svc.CreateCheckout(ctx, CreateCheckoutRequest{
		ExpectedAmount: 0,
	})
	if err == nil || !strings.Contains(err.Error(), "expected_amount must be greater than 0") {
		t.Fatalf("expected checkout amount error, got %v", err)
	}
}

func TestHealthCheckHandler(t *testing.T) {
	handler := NewHandler(nil)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	if rr.Body.String() != "ok" {
		t.Fatalf("expected 'ok', got %q", rr.Body.String())
	}
}
