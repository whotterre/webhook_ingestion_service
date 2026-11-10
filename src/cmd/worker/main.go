package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/streadway/amqp"
	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/dedup"
	"github.com/whotterre/webhook_ingestion_service/internal/ledger"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
	"github.com/whotterre/webhook_ingestion_service/internal/rabbitmq"
	"github.com/whotterre/webhook_ingestion_service/workers"
	"github.com/whotterre/webhook_ingestion_service/workers/notifications"
)

func main() {
	workerType := flag.String("type", "all", "Worker type to run: all, ledger, notifier, analytics")
	queueRange := flag.String("queues", "0-63", "Queue partition range to consume (e.g., 0-63 or 0,1,2)")
	queueCount := flag.Int("total-queues", 64, "Total number of queues in topology")
	metricsPort := flag.String("metrics-port", "9091", "Port to expose Prometheus /metrics")
	flag.Parse()

	if envType := os.Getenv("WORKER_TYPE"); envType != "" {
		*workerType = envType
	}
	if envQueues := os.Getenv("WORKER_QUEUES"); envQueues != "" {
		*queueRange = envQueues
	}
	if envPort := os.Getenv("METRICS_PORT"); envPort != "" {
		*metricsPort = envPort
	}

	_ = config.Load()

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Printf("[worker]: Need RabbitMQ URL")
	}
	rabbitExchange := os.Getenv("RABBITMQ_EXCHANGE")
	if rabbitExchange == "" {
		rabbitExchange = "webhooks"
	}
	postgresURL := os.Getenv("DATABASE_URL")
	if postgresURL == "" {
		log.Printf("[worker]: Need Postgres URL")
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Printf("[worker]: Need Redis URL")
	}

	log.Printf("[worker-tier] initializing worker_type=%q rabbitmq=%q exchange=%q",
		*workerType, rabbitURL, rabbitExchange)

	metrics.StartMetricsServer(":" + *metricsPort)

	// Initialize Dedup Cache (Redis)
	var deduplicator dedup.Deduplicator
	if rDedup, err := dedup.NewRedisDeduplicator(redisURL); err != nil {
		log.Printf("[worker-tier] warning: redis connection failed (%v), running without dedup cache", err)
	} else {
		deduplicator = rDedup
		defer deduplicator.Close()
		log.Println("[worker-tier] redis deduplication cache connected")
	}

	var ledgerStore ledger.Store
	if *workerType == "all" || *workerType == "ledger" {
		if pStore, err := ledger.NewPostgresStore(postgresURL); err != nil {
			log.Printf("[worker-tier] warning: postgres connection failed (%v)", err)
		} else {
			ledgerStore = pStore
			defer ledgerStore.Close()
			log.Println("[worker-tier] postgres ledger store connected")
		}
	}

	// Ensure topology (Exchange, DLX, DLQ, 64 Queues)
	if conn, err := amqp.Dial(rabbitURL); err == nil {
		err = rabbitmq.DeclareTopology(conn, rabbitmq.TopologyConfig{
			ExchangeName: rabbitExchange,
			ExchangeType: "direct",
			QueuePrefix:  "webhooks",
			QueueCount:   *queueCount,
			DLXName:      "dlx.webhooks",
			DLQName:      "dlq.webhooks",
		})
		if err != nil {
			log.Printf("[worker-tier] declare topology warning: %v", err)
		} else {
			log.Printf("[worker-tier] topology declared with %d queues and DLQ", *queueCount)
		}
		conn.Close()
	}

	// Parse queues to consume
	queues := parseQueueList(*queueRange, "webhooks")
	log.Printf("[worker-tier] assigned to consume from %d queue(s): %v", len(queues), queues)

	// Initialize Workers
	ledgerWorker := workers.NewLedgerWorker(deduplicator, ledgerStore)
	notifierWorker, _ := notifications.NewNotifierWorker(deduplicator)
	analyticsWorker := workers.NewAnalyticsWorker(deduplicator)

	// Combined message handler dispatching according to worker type
	handler := func(ctx context.Context, body []byte, routingKey string) error {
		switch strings.ToLower(*workerType) {
		case "ledger":
			return ledgerWorker.Handle(ctx, body, routingKey)
		case "notifier":
			return notifierWorker.Handle(ctx, body, routingKey)
		case "analytics":
			return analyticsWorker.Handle(ctx, body, routingKey)
		case "all":
			fallthrough
		default:
			if err := ledgerWorker.Handle(ctx, body, routingKey); err != nil {
				return err
			}
			if err := notifierWorker.Handle(ctx, body, routingKey); err != nil {
				return err
			}
			if err := analyticsWorker.Handle(ctx, body, routingKey); err != nil {
				return err
			}
			return nil
		}
	}

	consumer, err := rabbitmq.NewConsumer(rabbitmq.ConsumerConfig{
		URL:           rabbitURL,
		Queues:        queues,
		PrefetchCount: 20,
		WorkerName:    fmt.Sprintf("worker-%s", *workerType),
	})
	if err != nil {
		log.Fatalf("[worker-tier] failed to create rabbitmq consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[worker-tier] %s worker active and listening for messages...", *workerType)
	if err := consumer.StartConsuming(ctx, handler); err != nil {
		log.Fatalf("[worker-tier] consumer exited with error: %v", err)
	}

	log.Println("[worker-tier] worker stopped cleanly")
}

func parseQueueList(spec, prefix string) []string {
	spec = strings.TrimSpace(spec)
	if strings.Contains(spec, "-") {
		parts := strings.Split(spec, "-")
		if len(parts) == 2 {
			start, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err1 == nil && err2 == nil && start <= end {
				var result []string
				for i := start; i <= end; i++ {
					result = append(result, fmt.Sprintf("%s-%d", prefix, i))
				}
				return result
			}
		}
	}

	// Comma separated list
	items := strings.Split(spec, ",")
	var result []string
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, err := strconv.Atoi(item); err == nil {
			result = append(result, fmt.Sprintf("%s-%s", prefix, item))
		} else {
			result = append(result, item)
		}
	}
	if len(result) == 0 {
		result = append(result, fmt.Sprintf("%s-0", prefix))
	}
	return result
}
