package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Ingest metrics
	WebhookRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "webhook_requests_total",
			Help: "Total number of webhook HTTP requests received",
		},
		[]string{"status", "path"},
	)

	WebhookDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "webhook_request_duration_seconds",
			Help:    "Webhook HTTP request processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)

	KafkaPublishedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_published_events_total",
			Help: "Total number of events published to Kafka",
		},
		[]string{"status", "topic"},
	)

	// Dispatcher metrics
	DispatcherConsumedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "dispatcher_consumed_events_total",
			Help: "Total number of events consumed from Kafka by dispatcher",
		},
	)

	RabbitMQPublishedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rabbitmq_published_events_total",
			Help: "Total number of events published to RabbitMQ",
		},
		[]string{"status", "exchange"},
	)

	// Worker metrics
	WorkerEventsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_events_processed_total",
			Help: "Total number of events processed by workers",
		},
		[]string{"worker", "status"},
	)

	WorkerProcessingDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_processing_duration_seconds",
			Help:    "Worker processing latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"worker"},
	)

	DedupChecksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dedup_checks_total",
			Help: "Total number of deduplication checks",
		},
		[]string{"result"}, // hit, miss, error
	)

	LedgerCommitsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_commits_total",
			Help: "Total number of ledger transaction commits to Postgres",
		},
		[]string{"status"}, // success, duplicate, failed
	)

	DLQMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_messages_total",
			Help: "Total number of poison/failed messages sent to DLQ",
		},
		[]string{"worker", "reason"},
	)
)

func init() {
	prometheus.MustRegister(
		WebhookRequestsTotal,
		WebhookDurationSeconds,
		KafkaPublishedTotal,
		DispatcherConsumedTotal,
		RabbitMQPublishedTotal,
		WorkerEventsProcessedTotal,
		WorkerProcessingDurationSeconds,
		DedupChecksTotal,
		LedgerCommitsTotal,
		DLQMessagesTotal,
	)
}

// StartMetricsServer starts a standalone HTTP server serving Prometheus /metrics.
func StartMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("[metrics] starting metrics server on %s/metrics", addr)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("[metrics] metrics server error: %v", err)
		}
	}()
}
