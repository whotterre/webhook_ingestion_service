package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/archive"
	"github.com/whotterre/webhook_ingestion_service/internal/kafka"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
	"github.com/whotterre/webhook_ingestion_service/internal/rabbitmq"
)

const (
	consumeBackoff    = time.Second
	emptySessionDelay = 100 * time.Millisecond
	shutdownTimeout   = 30 * time.Second
)

// StartDispatch starts a minimal dispatcher that consumes from Kafka and publishes to RabbitMQ & raw archive.
func StartDispatch(ctx context.Context) error {
	cfg := config.Load()
	log.Println("[dispatch] starting")

	var cfgErrs []error

	if cfg.MetricsPort == "" {
		cfgErrs = append(cfgErrs, errors.New("MetricsPort env variable missing"))
	}
	if len(cfg.Kafka.Brokers) == 0 {
		cfgErrs = append(cfgErrs, errors.New("Kafka Brokers env variable missing or empty"))
	}
	if cfg.Kafka.Topic == "" {
		cfgErrs = append(cfgErrs, errors.New("Kafka Topic env variable missing"))
	}
	if cfg.Kafka.GroupID == "" {
		cfgErrs = append(cfgErrs, errors.New("Kafka GroupID env variable missing"))
	}
	if cfg.RabbitMQURL == "" {
		cfgErrs = append(cfgErrs, errors.New("RabbitMQ URL env variable missing"))
	}
	if cfg.RabbitMQExchange == "" {
		cfgErrs = append(cfgErrs, errors.New("RabbitMQ exchange env variable missing"))
	}
	if cfg.ArchiveDir == "" {
		cfgErrs = append(cfgErrs, errors.New("archive dir env variable missing"))
	}
	if len(cfgErrs) > 0 {
		return fmt.Errorf("invalid config: %w", errors.Join(cfgErrs...))
	}

	metricsPort := cfg.MetricsPort
	brokers := cfg.Kafka.Brokers
	topic := cfg.Kafka.Topic
	group := cfg.Kafka.GroupID
	rabbitURL := cfg.RabbitMQURL
	rabbitExchange := cfg.RabbitMQExchange
	archiveDir := cfg.ArchiveDir

	log.Printf("[dispatch] config: brokers=%v topic=%q group=%q", brokers, topic, group)
	log.Printf("[dispatch] rabbit url=%q exchange=%q", rabbitURL, rabbitExchange)

	metrics.StartMetricsServer(":" + metricsPort)

	archiver, err := archive.NewFileArchiver(archiveDir)
	if err != nil {
		return fmt.Errorf("archiver init: %w", err)
	}
	defer func() {
		log.Println("[dispatch] closing archiver")
		if cerr := archiver.Close(); cerr != nil {
			log.Printf("[dispatch] archiver close error: %v", cerr)
		}
	}()
	log.Printf("[dispatch] raw event archive writing to %q", archiveDir)

	log.Println("[dispatch] connecting to RabbitMQ...")
	rbPub, err := rabbitmq.NewPublisher(rabbitURL, rabbitExchange)
	if err != nil {
		log.Printf("[dispatch] RabbitMQ connect failed: %v", err)
		return fmt.Errorf("rabbitmq connect: %w", err)
	}
	log.Println("[dispatch] RabbitMQ connected")
	defer func() {
		log.Println("[dispatch] closing RabbitMQ publisher")
		if cerr := rbPub.Close(); cerr != nil {
			log.Printf("[dispatch] RabbitMQ publisher close error: %v", cerr)
		}
	}()

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_6_0_0
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = false
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	saramaCfg.Consumer.Group.Session.Timeout = 30 * time.Second
	saramaCfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	kafkaCfg := kafka.Config{
		Brokers:       cfg.Kafka.Brokers,
		Topic:         cfg.Kafka.Topic,
		Username:      cfg.Kafka.Username,
		Password:      cfg.Kafka.Password,
		SASLMechanism: cfg.Kafka.SASLMechanism,
		CACertPath:    cfg.Kafka.CACertPath,
		TLS:           cfg.Kafka.TLS,
	}

	if err := kafka.ConfigureNetwork(saramaCfg, kafkaCfg); err != nil {
		log.Printf("[dispatch] kafka network config failed: %v", err)
		return fmt.Errorf("kafka network config: %w", err)
	}

	log.Printf("[dispatch] sarama config: version=%s offset=%v tls=%v sasl=%v",
		saramaCfg.Version, saramaCfg.Consumer.Offsets.Initial,
		saramaCfg.Net.TLS.Enable, saramaCfg.Net.SASL.Enable)

	log.Printf("[dispatch] creating consumer group %q...", group)
	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, saramaCfg)
	if err != nil {
		log.Printf("[dispatch] consumer group creation failed: %v", err)
		return fmt.Errorf("consumer group creation: %w", err)
	}
	log.Println("[dispatch] consumer group created")

	consumerGroupClosed := false
	closeConsumerGroup := func() {
		if consumerGroupClosed {
			return
		}
		consumerGroupClosed = true
		log.Println("[dispatch] closing consumer group")
		done := make(chan struct{})
		go func() {
			consumerGroup.Close()
			close(done)
		}()
		select {
		case <-done:
			log.Println("[dispatch] consumer group closed")
		case <-time.After(shutdownTimeout):
			log.Printf("[dispatch] consumer group close timed out after %s", shutdownTimeout)
		}
	}
	defer closeConsumerGroup()

	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("[dispatch] sarama error: %v", err)
		}
	}()

	handler := &kafkaHandler{
		publisher:      rbPub,
		archiver:       archiver,
		topic:          topic,
		rabbitExchange: rabbitExchange,
	}

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("[dispatch] entering consume loop, topic=%q", topic)

	for {
		if runCtx.Err() != nil {
			log.Printf("[dispatch] context cancelled, exiting loop: %v", runCtx.Err())
			break
		}

		log.Println("[dispatch] beginning consume session")
		err := consumerGroup.Consume(runCtx, []string{topic}, handler)
		switch {
		case err != nil:
			log.Printf("[dispatch] consume error: %v", err)
			select {
			case <-time.After(consumeBackoff):
			case <-runCtx.Done():
			}
		case runCtx.Err() == nil:
			select {
			case <-time.After(emptySessionDelay):
			case <-runCtx.Done():
			}
		}
		log.Println("[dispatch] consume session ended")
	}

	log.Println("[dispatch] consume loop exited, shutting down")
	closeConsumerGroup()

	log.Println("[dispatch] stopped cleanly")
	return nil
}