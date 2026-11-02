package rabbitmq

import (
	"errors"
	"log"

	"github.com/streadway/amqp"
)

type Publisher struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	exchange string
}

func NewPublisher(url string, exchange string) (*Publisher, error) {
	if url == "" {
		return nil, errors.New("rabbitmq url required")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	// ensure exchange exists
	if err := ch.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // args
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &Publisher{conn: conn, channel: ch, exchange: exchange}, nil
}

func (p *Publisher) Publish(body []byte) error {
	if p == nil || p.channel == nil {
		return errors.New("publisher not initialized")
	}
	err := p.channel.Publish(
		p.exchange, // exchange
		"",         // routing key
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
		p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
