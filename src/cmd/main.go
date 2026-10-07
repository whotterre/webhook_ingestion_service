package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/kafka"
	"github.com/whotterre/webhook_ingestion_service/internal/webhook"
)

func main() {
	cfg := config.Load()

	var publisher *kafka.Publisher
	if cfg.Kafka.Enabled() {
		kafkaPublisher, err := kafka.NewPublisher(kafka.Config{
			Brokers:       cfg.Kafka.Brokers,
			Topic:         cfg.Kafka.Topic,
			Username:      cfg.Kafka.Username,
			Password:      cfg.Kafka.Password,
			SASLMechanism: cfg.Kafka.SASLMechanism,
			CACertPath:    cfg.Kafka.CACertPath,
			TLS:           cfg.Kafka.TLS,
		})
		if err != nil {
			log.Printf("kafka publisher disabled: %v", err)
		} else {
			publisher = kafkaPublisher
			defer func() {
				if err := publisher.Close(); err != nil {
					log.Printf("close kafka publisher: %v", err)
				}
			}()
			log.Printf("Kafka publisher connected to %d broker(s), topic %q", len(cfg.Kafka.Brokers), cfg.Kafka.Topic)
		}
	} else {
		log.Println("Kafka not configured; running without publish")
	}

	handler := webhook.SetupServer(publisher)

	addr := ":" + cfg.HTTPPort
	log.Printf("Starting webhook server on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Fprintf(log.Writer(), "Server error: %v", err)
	}
}
