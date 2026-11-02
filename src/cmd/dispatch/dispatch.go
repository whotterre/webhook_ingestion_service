package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/kafka"
	"github.com/whotterre/webhook_ingestion_service/internal/rabbitmq"
)

// StartDispatch starts a minimal dispatcher that consumes from Kafka and publishes to RabbitMQ.
func StartDispatch(ctx context.Context) error {
	cfg := config.Load()
	log.Println("[dispatch] starting")

	brokers := cfg.Kafka.Brokers
	topic := cfg.Kafka.Topic
	group := os.Getenv("KAFKA_GROUP_ID")
	if group == "" {
		group = "fishie"
	}

	log.Printf("[dispatch] config: brokers=%v topic=%q group=%q",
		brokers, topic, group)

	if len(brokers) == 0 || topic == "" || group == "" {
		log.Println("[dispatch] missing required config, aborting")
		return nil
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	rabbitExchange := os.Getenv("RABBITMQ_EXCHANGE")
	if rabbitExchange == "" {
		rabbitExchange = "webhooks"
	}
	log.Printf("[dispatch] rabbit url=%q exchange=%q", rabbitURL, rabbitExchange)

	log.Println("[dispatch] connecting to rabbitmq...")
	rbPub, err := rabbitmq.NewPublisher(rabbitURL, rabbitExchange)
	if err != nil {
		log.Printf("[dispatch] rabbitmq connect failed: %v", err)
		return err
	}
	log.Println("[dispatch] rabbitmq connected")
	defer func() {
		log.Println("[dispatch] closing rabbitmq publisher")
		rbPub.Close()
	}()

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_8_0_0
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaCfg.Consumer.Return.Errors = true

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
		return err
	}

	log.Printf("[dispatch] sarama config: version=%s offset=%v tls=%v sasl=%v",
		saramaCfg.Version, saramaCfg.Consumer.Offsets.Initial, saramaCfg.Net.TLS.Enable, saramaCfg.Net.SASL.Enable)

	log.Printf("[dispatch] creating consumer group %q...", group)
	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, saramaCfg)
	if err != nil {
		log.Printf("[dispatch] consumer group creation failed: %v", err)
		return err
	}
	log.Println("[dispatch] consumer group created")
	defer func() {
		log.Println("[dispatch] closing consumer group")
		consumerGroup.Close()
	}()

	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("[dispatch] sarama error: %v", err)
		}
	}()

	handler := &kafkaHandler{publisher: rbPub, topic: topic}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		sig := <-sigs
		log.Printf("[dispatch] received signal %v, shutting down", sig)
		cancel()
	}()

	log.Printf("[dispatch] entering consume loop, topic=%q", topic)
	for {
		if runCtx.Err() != nil {
			log.Printf("[dispatch] context cancelled, exiting loop: %v", runCtx.Err())
			break
		}

		log.Println("[dispatch] beginning consume session")
		if err := consumerGroup.Consume(runCtx, []string{topic}, handler); err != nil {
			log.Printf("[dispatch] consume error: %v", err)
			// small backoff so we don't spin on persistent failure
			select {
			case <-time.After(time.Second):
			case <-runCtx.Done():
			}
		}
		log.Println("[dispatch] consume session ended")
	}

	log.Println("[dispatch] stopped cleanly")
	return nil
}

type kafkaHandler struct {
	publisher *rabbitmq.Publisher
	topic     string
}

func (h *kafkaHandler) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("[kafka] session setup: member=%s generation=%d claims=%v",
		s.MemberID(), s.GenerationID(), s.Claims())
	return nil
}

func (h *kafkaHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("[kafka] session cleanup: member=%s generation=%d",
		s.MemberID(), s.GenerationID())
	return nil
}

func (h *kafkaHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("[kafka] claim started: topic=%s partition=%d initialOffset=%d",
		claim.Topic(), claim.Partition(), claim.InitialOffset())

	msgCount := 0
	for msg := range claim.Messages() {
		msgCount++
		log.Printf("[kafka] msg received: topic=%s partition=%d offset=%d key=%q len=%d",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), len(msg.Value))

		if len(msg.Key) == 0 {
			log.Printf("[kafka] WARN: empty key at offset=%d, skipping", msg.Offset)
			sess.MarkMessage(msg, "")
			continue
		}

		if err := h.publisher.Publish(msg.Value); err != nil {
			log.Printf("[kafka] publish to rabbit failed: key=%q offset=%d err=%v",
				msg.Key, msg.Offset, err)
			// do not mark — sarama will redeliver
			continue
		}

		log.Printf("[kafka] published to rabbit: key=%q offset=%d", msg.Key, msg.Offset)
		sess.MarkMessage(msg, "")
		log.Printf("[kafka] marked offset=%d", msg.Offset)
	}

	log.Printf("[kafka] claim ended: topic=%s partition=%d processed=%d",
		claim.Topic(), claim.Partition(), msgCount)
	return nil
}