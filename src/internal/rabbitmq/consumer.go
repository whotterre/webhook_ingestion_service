package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type MessageHandler func(ctx context.Context, body []byte, routingKey string) error

type ConsumerConfig struct {
	URL           string
	Queues        []string
	PrefetchCount int
	WorkerName    string
}

type Consumer struct {
	conn *amqp.Connection
	cfg  ConsumerConfig
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("rabbitmq url required")
	}
	if len(cfg.Queues) == 0 {
		return nil, fmt.Errorf("at least one queue is required to consume")
	}
	if cfg.PrefetchCount <= 0 {
		cfg.PrefetchCount = 10
	}

	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	return &Consumer{
		conn: conn,
		cfg:  cfg,
	}, nil
}

// StartConsuming spawns consumer goroutines with dedicated channels for each configured queue.
func (c *Consumer) StartConsuming(ctx context.Context, handler MessageHandler) error {
	var wg sync.WaitGroup

	for _, qName := range c.cfg.Queues {
		ch, err := c.conn.Channel()
		if err != nil {
			return fmt.Errorf("open channel for queue %s: %w", qName, err)
		}

		if err := ch.Qos(c.cfg.PrefetchCount, 0, false); err != nil {
			ch.Close()
			return fmt.Errorf("set qos for queue %s: %w", qName, err)
		}

		deliveries, err := ch.Consume(
			qName,
			fmt.Sprintf("%s-%s", c.cfg.WorkerName, qName), // consumer tag
			false, // auto-ack = false
			false, // exclusive = false
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			ch.Close()
			return fmt.Errorf("consume queue %s: %w", qName, err)
		}

		wg.Add(1)
		go func(queue string, channel *amqp.Channel, msgs <-chan amqp.Delivery) {
			defer wg.Done()
			defer channel.Close()
			log.Printf("[%s] consuming from queue %q", c.cfg.WorkerName, queue)

			for {
				select {
				case <-ctx.Done():
					log.Printf("[%s] shutting down consumer for queue %q", c.cfg.WorkerName, queue)
					return
				case d, ok := <-msgs:
					if !ok {
						log.Printf("[%s] delivery channel closed for queue %q", c.cfg.WorkerName, queue)
						return
					}

					msgCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					err := handler(msgCtx, d.Body, d.RoutingKey)
					cancel()

					if err != nil {
						log.Printf("[%s] error processing message from %s (routing_key=%s): %v. Nacking to DLQ.",
							c.cfg.WorkerName, queue, d.RoutingKey, err)
						_ = d.Nack(false, false)
					} else {
						_ = d.Ack(false)
					}
				}
			}
		}(qName, ch, deliveries)
	}

	wg.Wait()
	return nil
}

func (c *Consumer) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}