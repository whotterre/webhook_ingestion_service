package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/whotterre/webhook_ingestion_service/internal/dedup"
	"github.com/whotterre/webhook_ingestion_service/internal/events"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
)

type NotifierWorker struct {
	dedup    dedup.Deduplicator
	notifier *EmailNotifier
}

type ReceiptData struct {
	EventType  events.EventType
	CustomerID string
	PaymentID  string
	Currency   string
	Amount     int64
}

func resolveTemplatePath() string {
	if p := os.Getenv("RECEIPT_TEMPLATE_PATH"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	candidates := []string{
		"workers/notifications/templates/reciept.html",
		"src/workers/notifications/templates/reciept.html",
		"../workers/notifications/templates/reciept.html",
		"templates/reciept.html",
		"../templates/reciept.html",
		"../../workers/notifications/templates/reciept.html",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return "workers/notifications/templates/reciept.html"
}

func NewNotifierWorker(d dedup.Deduplicator) (*NotifierWorker, error) {
	templatePath := resolveTemplatePath()
	notifier, err := NewEmailNotifier(templatePath)
	if err != nil {
		log.Printf("[notifier-worker] warning: could not initialize email notifier with template %q: %v. Running in log-only mode.", templatePath, err)
		notifier = nil
	}
	return &NotifierWorker{
		dedup:    d,
		notifier: notifier,
	}, nil
}

func (w *NotifierWorker) Handle(ctx context.Context, body []byte, routingKey string) error {
	start := time.Now()
	defer func() {
		metrics.WorkerProcessingDurationSeconds.WithLabelValues("notifier").Observe(time.Since(start).Seconds())
	}()

	var evt events.PaymentEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		metrics.DLQMessagesTotal.WithLabelValues("notifier", "malformed_json").Inc()
		metrics.WorkerEventsProcessedTotal.WithLabelValues("notifier", "error").Inc()
		return fmt.Errorf("unmarshal payment event: %w", err)
	}

	if evt.EventID == "" {
		metrics.DLQMessagesTotal.WithLabelValues("notifier", "missing_event_id").Inc()
		metrics.WorkerEventsProcessedTotal.WithLabelValues("notifier", "error").Inc()
		return errors.New("event_id is missing")
	}

	if w.dedup != nil {
		isDup, err := w.dedup.CheckAndMark(ctx, "notifier", evt.EventID, 24*time.Hour)
		if err != nil {
			log.Printf("[notifier-worker] dedup check error: %v", err)
		} else if isDup {
			log.Printf("[notifier-worker] event %s already notified, skipping", evt.EventID)
			metrics.WorkerEventsProcessedTotal.WithLabelValues("notifier", "duplicate").Inc()
			return nil
		}
	}

	log.Printf("[notifier-worker] dispatched notification for event %s | type=%s customer=%s payment=%s",
		evt.EventID, evt.EventType, evt.Data.CustomerID, evt.PaymentID)

	if w.notifier != nil {
		if err := w.notifier.Deliver(ctx, evt); err != nil {
			log.Printf("[notifier-worker] email delivery failed for event %s: %v", evt.EventID, err)
		}
	}

	metrics.WorkerEventsProcessedTotal.WithLabelValues("notifier", "success").Inc()
	return nil
}