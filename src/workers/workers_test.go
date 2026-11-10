package workers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/whotterre/webhook_ingestion_service/internal/archive"
	"github.com/whotterre/webhook_ingestion_service/internal/events"
)

type mockDeduplicator struct {
	seen map[string]bool
}

func (m *mockDeduplicator) CheckAndMark(ctx context.Context, scope, key string, ttl time.Duration) (bool, error) {
	composite := scope + ":" + key
	if m.seen[composite] {
		return true, nil
	}
	m.seen[composite] = true
	return false, nil
}

func (m *mockDeduplicator) Close() error {
	return nil
}

type mockLedgerStore struct {
	committed []*events.PaymentEvent
}

func (m *mockLedgerStore) CommitPaymentEvent(ctx context.Context, evt *events.PaymentEvent) error {
	m.committed = append(m.committed, evt)
	return nil
}

func (m *mockLedgerStore) GetAccountBalance(ctx context.Context, accountID string) (int64, string, error) {
	return 0, "NGN", nil
}

func (m *mockLedgerStore) Close() error {
	return nil
}

func TestWorkersPipeline(t *testing.T) {
	mockDedup := &mockDeduplicator{seen: make(map[string]bool)}
	mockStore := &mockLedgerStore{}

	ledgerW := NewLedgerWorker(mockDedup, mockStore)
	notifW := NewNotifierWorker(mockDedup)
	analyticsW := NewAnalyticsWorker(mockDedup)

	evt := events.PaymentEvent{
		EventID:    "evt_test_001",
		EventType:  events.PaymentSucceeded,
		OccurredAt: time.Now(),
		Provider:   "fake-psp",
		PaymentID:  "pay_test_001",
		AccountID:  "acct_test_001",
		Amount:     10000,
		Currency:   "NGN",
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	ctx := context.Background()

	// Process across workers
	if err := ledgerW.Handle(ctx, payload, evt.AccountID); err != nil {
		t.Fatalf("ledger handler failed: %v", err)
	}
	if err := notifW.Handle(ctx, payload, evt.AccountID); err != nil {
		t.Fatalf("notif handler failed: %v", err)
	}
	if err := analyticsW.Handle(ctx, payload, evt.AccountID); err != nil {
		t.Fatalf("analytics handler failed: %v", err)
	}

	if len(mockStore.committed) != 1 {
		t.Fatalf("expected 1 committed event in ledger store, got %d", len(mockStore.committed))
	}

	// Test Deduplication
	if err := ledgerW.Handle(ctx, payload, evt.AccountID); err != nil {
		t.Fatalf("duplicate handle should succeed (ack), got error: %v", err)
	}
	// Verify count didn't increase
	if len(mockStore.committed) != 1 {
		t.Fatalf("duplicate should not commit to store, committed count: %d", len(mockStore.committed))
	}
}

func TestArchiveAndRead(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "webhook_archive_test")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	archiver, err := archive.NewFileArchiver(tmpDir)
	if err != nil {
		t.Fatalf("create archiver: %v", err)
	}

	testEvent := []byte(`{"event_id":"evt_archive_01","amount":500}`)
	if err := archiver.Archive(testEvent); err != nil {
		t.Fatalf("archive write failed: %v", err)
	}
	_ = archiver.Close()

	files, err := os.ReadDir(tmpDir)
	if err != nil || len(files) == 0 {
		t.Fatalf("no archive files created")
	}

	archivePath := filepath.Join(tmpDir, files[0].Name())
	readEvents := 0
	err = archive.ReadArchiveFile(archivePath, func(data []byte) error {
		readEvents++
		return nil
	})
	if err != nil {
		t.Fatalf("read archive failed: %v", err)
	}
	if readEvents != 1 {
		t.Fatalf("expected 1 read event, got %d", readEvents)
	}
}
