package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/whotterre/webhook_ingestion_service/internal/dedup"
	"github.com/whotterre/webhook_ingestion_service/internal/events"
	"github.com/whotterre/webhook_ingestion_service/internal/ledger"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
)

type LedgerWorker struct {
	dedup dedup.Deduplicator
	store ledger.Store
}

func NewLedgerWorker(d dedup.Deduplicator, s ledger.Store) *LedgerWorker {
	return &LedgerWorker{
		dedup: d,
		store: s,
	}
}

func (w *LedgerWorker) Handle(ctx context.Context, body []byte, routingKey string) error {
	start := time.Now()
	defer func() {
		metrics.WorkerProcessingDurationSeconds.WithLabelValues("ledger").Observe(time.Since(start).Seconds())
	}()

	var evt events.PaymentEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		metrics.DLQMessagesTotal.WithLabelValues("ledger", "malformed_json").Inc()
		metrics.WorkerEventsProcessedTotal.WithLabelValues("ledger", "error").Inc()
		return fmt.Errorf("unmarshal payment event: %w", err)
	}

	if evt.EventID == "" {
		metrics.DLQMessagesTotal.WithLabelValues("ledger", "missing_event_id").Inc()
		metrics.WorkerEventsProcessedTotal.WithLabelValues("ledger", "error").Inc()
		return errors.New("event_id is missing")
	}

	if w.dedup != nil {
		isDup, err := w.dedup.CheckAndMark(ctx, "ledger", evt.EventID, 24*time.Hour)
		if err != nil {
			log.Printf("[ledger-worker] dedup check error for event %s: %v", evt.EventID, err)
		} else if isDup {
			log.Printf("[ledger-worker] event %s already processed (dedup hit), skipping commit", evt.EventID)
			metrics.WorkerEventsProcessedTotal.WithLabelValues("ledger", "duplicate").Inc()
			return nil
		}
	}

	if w.store != nil {
			if err := w.store.CommitPaymentEvent(ctx, &evt); err != nil {
			if errors.Is(err, ledger.ErrDuplicateEvent) {
				log.Printf("[ledger-worker] event %s already committed in postgres, skipping", evt.EventID)
				metrics.WorkerEventsProcessedTotal.WithLabelValues("ledger", "duplicate").Inc()
				return nil
			}
			metrics.DLQMessagesTotal.WithLabelValues("ledger", "db_error").Inc()
			metrics.WorkerEventsProcessedTotal.WithLabelValues("ledger", "error").Inc()
			return fmt.Errorf("ledger commit failed for event %s: %w", evt.EventID, err)
		}
	}

	log.Printf("[ledger-worker] committed event %s | account=%s amount=%d %s type=%s",
		evt.EventID, evt.AccountID, evt.Amount, evt.Currency, evt.EventType)

	metrics.WorkerEventsProcessedTotal.WithLabelValues("ledger", "success").Inc()
	return nil
}
