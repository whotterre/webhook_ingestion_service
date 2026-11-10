package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sony/gobreaker"
	"github.com/whotterre/webhook_ingestion_service/internal/dedup"
	"github.com/whotterre/webhook_ingestion_service/internal/events"
	"github.com/whotterre/webhook_ingestion_service/internal/webhook"
)

type PaymentStatusClient interface {
	CheckStatus(ctx context.Context, paymentID string) (*events.PaymentEvent, error)
}

type PaymentVerifierWorker struct {
	dedup    dedup.Deduplicator
	cb       *gobreaker.CircuitBreaker
	client   PaymentStatusClient
	publisher webhook.KeyedPublisher
}

func NewPaymentVerifierWorker(d dedup.Deduplicator, pub webhook.KeyedPublisher) *PaymentVerifierWorker {
	st := gobreaker.Settings{
		Name:        "PSP-Status-API",
		MaxRequests: 5,
		Interval:    60 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 10 && failureRatio >= 0.5
		},
	}
	return &PaymentVerifierWorker{
		dedup:     d,
		cb:        gobreaker.NewCircuitBreaker(st),
		publisher: pub,
	}
}

// VerifyOrPoll verifies payment status via circuit-broken PSP API
func (w *PaymentVerifierWorker) VerifyPayment(ctx context.Context, paymentID, accountID string) error {
	seen, _ := w.dedup.CheckAndMark(ctx, "payment_verified", paymentID, 24*time.Hour)
	if seen {
		return nil
	}

	result, err := w.cb.Execute(func() (interface{}, error) {
		return w.client.CheckStatus(ctx, paymentID)
	})
	if err != nil {
		return fmt.Errorf("circuit breaker / status check failed: %w", err)
	}

	event := result.(*events.PaymentEvent)
	if event != nil && event.EventType == events.PaymentSucceeded {
		payload, _ := json.Marshal(event)
		return w.publisher.PublishWithKey(accountID, payload)
	}

	return nil
}