package main

import (
	"log"
	"os"

	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/kafka"
)

func main() {
	cfg := config.Load()

	if !cfg.Kafka.Enabled() {
		log.Println("kafka not configured; set KAFKA_BROKERS and KAFKA_TOPIC")
		os.Exit(1)
	}

	publisher, err := kafka.NewPublisher(kafka.Config{
		Brokers:       cfg.Kafka.Brokers,
		Topic:         cfg.Kafka.Topic,
		Username:      cfg.Kafka.Username,
		Password:      cfg.Kafka.Password,
		SASLMechanism: cfg.Kafka.SASLMechanism,
		CACertPath:    cfg.Kafka.CACertPath,
		TLS:           cfg.Kafka.TLS,
	})
	if err != nil {
		log.Printf("kafka connection failed: %v", err)
		os.Exit(1)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("close kafka publisher: %v", err)
		}
	}()

	log.Println("kafka connection ok")
}
