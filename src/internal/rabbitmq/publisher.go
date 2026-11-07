package rabbitmq

import (
	"errors"
	"hash/fnv"
	"log"
	"strconv"

	"github.com/streadway/amqp"
)

type Publisher struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	exchange string
}

func NewPublisher(url string, exchange string) (*Publisher, error) {
	if url == "" {
		return nil, errors.New("RabbitMQ URL required")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	if exchange != "" {
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return nil, err
		}

		// Check if exchange already exists on broker
		if err := ch.ExchangeDeclarePassive(exchange, "direct", true, false, false, false, nil); err != nil {
			// Passive check closed the channel; open fresh channel to declare
			ch.Close()
			if secCh, secErr := conn.Channel(); secErr == nil {
				if err := secCh.ExchangeDeclare(
					exchange,
					"direct",
					true,  // durable
					false, // auto-deleted
					false, // internal
					false, // no-wait
					nil,
				); err != nil {
					log.Printf("[rabbitmq-publisher] notice: exchange %q already exists with custom configuration", exchange)
				}
				secCh.Close()
			}
		} else {
			ch.Close()
		}
	}

	// Open active publishing channel
	pubCh, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Publisher{conn: conn, channel: pubCh, exchange: exchange}, nil
}

// HashRoutingKey computes modulus hash for direct queue partitioning (0..partitionCount-1).
func HashRoutingKey(key string, partitionCount int) string {
	if partitionCount <= 0 {
		partitionCount = 64
	}
	if key == "" {
		return "0"
	}
	// If it's already an integer, keep it within modulus
	if val, err := strconv.Atoi(key); err == nil {
		return strconv.Itoa(val % partitionCount)
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	partition := h.Sum32() % uint32(partitionCount)
	return strconv.Itoa(int(partition))
}

func (p *Publisher) Publish(body []byte, routingKey string) error {
	if p == nil || p.channel == nil {
		return errors.New("publisher not initialized")
	}

	// Map key to partition index (0-63)
	targetKey := HashRoutingKey(routingKey, 64)

	err := p.channel.Publish(
		p.exchange, // exchange
		targetKey,  // routing key mapped to queue partition 0..63
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
	if err != nil {
		log.Printf("rabbit publish error: %v", err)
	}
	return err
}

func (p *Publisher) Close() error {
	if p == nil {
		return nil
	}
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
