package events

import "time"

type EventType string

const (
    PaymentCreated    EventType = "payment.created"
    PaymentProcessing EventType = "payment.processing"
    PaymentSucceeded  EventType = "payment.succeeded"
    PaymentFailed     EventType = "payment.failed"
    PaymentRefunded   EventType = "payment.refunded"
    ChargebackCreated EventType = "chargeback.created"
)

type PaymentData struct {
    Method     string `json:"method"`
    Last4      string `json:"last4"`
    CustomerID string `json:"customer_id"`
}

type PaymentEvent struct {
    EventID    string      `json:"event_id"`
    EventType  EventType   `json:"event_type"`
    OccurredAt time.Time   `json:"occurred_at"`
    Provider   string      `json:"provider"`
    PaymentID  string      `json:"payment_id"`
    AccountID  string      `json:"account_id"`
    Sequence   uint32      `json:"sequence"`
    Amount     int64       `json:"amount"`
    Currency   string      `json:"currency"`
    Data       PaymentData `json:"data"`
}