package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/webhook"
)

func main() {
	cfg := config.Load()
	handler := webhook.SetupServer()

	addr := ":" + cfg.HTTPPort
	log.Printf("Starting webhook server on %s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Fprintf(log.Writer(), "Server error: %v", err)
	}
}
