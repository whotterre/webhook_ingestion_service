package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/whotterre/webhook_ingestion_service/internal/archive"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
	"github.com/whotterre/webhook_ingestion_service/internal/rabbitmq"
)

// kafkaHandler consumes messages from Kafka, archives them, and publishes them
// to RabbitMQ. It commits an offset only after both the archive write and the
// RabbitMQ publish succeed for that message.
type kafkaHandler struct {
	publisher      *rabbitmq.Publisher
	archiver       archive.Archiver
	topic          string
	rabbitExchange string
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
	for {
		select {
		case <-sess.Context().Done():
			log.Printf("[kafka] claim aborted by shutdown: topic=%s partition=%d processed=%d uncommitted=%d",
				claim.Topic(), claim.Partition(), msgCount, 0)
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				log.Printf("[kafka] claim ended: topic=%s partition=%d processed=%d",
					claim.Topic(), claim.Partition(), msgCount)
				return nil
			}

			msgCount++
			metrics.DispatcherConsumedTotal.Inc()

			log.Printf("[kafka] msg received: topic=%s partition=%d offset=%d key=%q len=%d",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), len(msg.Value))

			if err := h.process(msg); err != nil {
				// Do NOT commit: the message will be redelivered after rebalance/restart.
				// Return the error so sarama tears down this claim session; the outer
				// consume loop in StartDispatch will log and retry with backoff.
				log.Printf("[kafka] process failed: topic=%s partition=%d offset=%d key=%q err=%v",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
				return fmt.Errorf("consume claim topic=%s partition=%d offset=%d: %w",
					msg.Topic, msg.Partition, msg.Offset, err)
			}

			sess.MarkMessage(msg, "")
		}
	}
}

// process runs the per-message pipeline. It returns a non-nil error if either
// step fails; the caller must not commit the offset in that case.
//
// On failure of the archive step we do NOT attempt the RabbitMQ publish,
// because downstream consumers of RabbitMQ are presumed to expect every message
// that reaches RabbitMQ to also be present in the archive. Failing fast (and
// letting the message be redelivered) preserves that invariant.
func (h *kafkaHandler) process(msg *sarama.ConsumerMessage) error {
	if h.archiver == nil {
		return errors.New("archiver not configured")
	}
	if h.publisher == nil {
		return errors.New("publisher not configured")
	}

	if err := h.archiver.Archive(msg.Value); err != nil {
		return fmt.Errorf("archive: %w", err)
	}

	routingKey := string(msg.Key)
	if routingKey == "" {
		routingKey = "0"
	}

	if err := h.publisher.Publish(msg.Value, routingKey); err != nil {
		metrics.RabbitMQPublishedTotal.WithLabelValues("failed", h.rabbitExchange).Inc()
		return fmt.Errorf("rabbitmq publish: %w", err)
	}

	metrics.RabbitMQPublishedTotal.WithLabelValues("success", h.rabbitExchange).Inc()
	return nil
}


var _ = context.Background