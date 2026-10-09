package rabbitmq

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type TopologyConfig struct {
	ExchangeName  string
	ExchangeType  string // "direct"
	QueuePrefix   string
	QueueCount    int
	DLXName       string
	DLQName       string
	DeliveryLimit int32
}

// DeclareTopology declares all exchanges, DLX/DLQ, worker queues, and binds them.
// Idempotent: safe to call on startup.
func DeclareTopology(conn *amqp.Connection, cfg TopologyConfig) error {
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}
	if cfg.DLXName == "" {
		cfg.DLXName = "dlx.webhooks"
	}
	if cfg.DLQName == "" {
		cfg.DLQName = "dlq.webhooks"
	}
	if cfg.QueuePrefix == "" {
		cfg.QueuePrefix = "webhooks"
	}

	// Declare Main Exchange safely
	if cfg.ExchangeName != "" {
		ch, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("open channel for exchange: %w", err)
		}

		// Check if exchange already exists
		if err := ch.ExchangeDeclarePassive(cfg.ExchangeName, cfg.ExchangeType, true, false, false, false, nil); err != nil {
			// Channel was closed by passive failure; reopen channel to declare
			ch.Close()
			ch, err = conn.Channel()
			if err == nil {
				if err := ch.ExchangeDeclare(
					cfg.ExchangeName,
					cfg.ExchangeType,
					true,  // durable
					false, // auto-delete
					false, // internal
					false, // no-wait
					nil,
				); err != nil {
					log.Printf("[topology] warning: declare exchange %s: %v", cfg.ExchangeName, err)
				}
				ch.Close()
			}
		} else {
			ch.Close()
		}
	}

	// Declare DLX (dead-letter exchange)
	{
		ch, err := conn.Channel()
		if err == nil {
			_ = ch.ExchangeDeclare(
				cfg.DLXName,
				"direct",
				true,  // durable
				false, // auto-delete
				false, // internal
				false, // no-wait
				nil,
			)

			// Declare and bind DLQ
			_, _ = ch.QueueDeclare(
				cfg.DLQName,
				true,  // durable
				false, // auto-delete
				false, // exclusive
				false, // no-wait
				nil,
			)

			_ = ch.QueueBind(
				cfg.DLQName,
				"", // routing key
				cfg.DLXName,
				false,
				nil,
			)
			ch.Close()
		}
	}

	// 4. Declare and bind the worker queues
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel for queues: %w", err)
	}
	defer ch.Close()

	for i := 0; i < cfg.QueueCount; i++ {
		queueName := fmt.Sprintf("%s-%d", cfg.QueuePrefix, i)

		// Classic queue arguments with dead-letter routing
		queueArgs := amqp.Table{
			"x-dead-letter-exchange": cfg.DLXName,
		}

		if _, err := ch.QueueDeclare(
			queueName,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			queueArgs,
		); err != nil {
			// If arguments mismatch (e.g. existing queue without DLX), try passive or declare without args
			log.Printf("[topology] queue %s declare with args failed (%v), trying standard declare", queueName, err)
			ch.Close()
			ch, _ = conn.Channel()
			if ch != nil {
				_, _ = ch.QueueDeclare(queueName, true, false, false, false, nil)
			}
		}

		if ch != nil && cfg.ExchangeName != "" {
			bindingKey := fmt.Sprintf("%d", i)
			_ = ch.QueueBind(
				queueName,
				bindingKey,
				cfg.ExchangeName,
				false,
				nil,
			)
		}
	}

	return nil
}