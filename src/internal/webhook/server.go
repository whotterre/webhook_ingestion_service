package webhook

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/whotterre/webhook_ingestion_service/internal/events"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
)

var (
	signatureHeader = "X-PSP-Signature"
)

type KeyedPublisher interface {
	PublishWithKey(key string, body []byte) error
}

type LegacyPublisher interface {
	Publish([]byte) error
}

type server struct {
	publisher interface{}
}

func SetupServer(publisher interface{}) http.Handler {
	mux := http.NewServeMux()
	h := &server{publisher: publisher}
	mux.HandleFunc("/webhook", h.handleWebhookPost)
	mux.HandleFunc("/healthz", h.handleHealthCheck)
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func (s *server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleWebhookPost(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		metrics.WebhookDurationSeconds.WithLabelValues("/webhook").Observe(time.Since(start).Seconds())
	}()

	webhookSecret := os.Getenv("WEBHOOK_SECRET")

	if r.Method != http.MethodPost {
		metrics.WebhookRequestsTotal.WithLabelValues("method_not_allowed", "/webhook").Inc()
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("read body: %v", err)
		metrics.WebhookRequestsTotal.WithLabelValues("bad_request", "/webhook").Inc()
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if webhookSecret != "" && r.Header.Get(signatureHeader) != "" {
		if err := verifySignature(webhookSecret, body, r.Header.Get(signatureHeader)); err != nil {
			log.Printf("signature verification failed: %v", err)
			metrics.WebhookRequestsTotal.WithLabelValues("unauthorized", "/webhook").Inc()
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// Extract routing key (prefer account_id for per-account ordering)
	key := ""
	var evt events.PaymentEvent
	if err := json.Unmarshal(body, &evt); err == nil {
		if evt.AccountID != "" {
			key = evt.AccountID
		} else if evt.PaymentID != "" {
			key = evt.PaymentID
		} else if evt.EventID != "" {
			key = evt.EventID
		}
	}

	if s.publisher != nil {
		var pubErr error
		if kp, ok := s.publisher.(KeyedPublisher); ok {
			pubErr = kp.PublishWithKey(key, body)
		} else if lp, ok := s.publisher.(LegacyPublisher); ok {
			pubErr = lp.Publish(body)
	}

		if pubErr != nil {
			log.Printf("publish to kafka: %v", pubErr)
			metrics.KafkaPublishedTotal.WithLabelValues("failed", "raw.events").Inc()
			metrics.WebhookRequestsTotal.WithLabelValues("bad_gateway", "/webhook").Inc()
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		metrics.KafkaPublishedTotal.WithLabelValues("success", "raw.events").Inc()
	} else {
		log.Printf("kafka publisher not configured; skipping publish")
	}

	metrics.WebhookRequestsTotal.WithLabelValues("success", "/webhook").Inc()
	w.WriteHeader(http.StatusOK)
}
