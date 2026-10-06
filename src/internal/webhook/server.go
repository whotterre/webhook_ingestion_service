package webhook

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/whotterre/webhook_ingestion_service/internal/events"
)

var (
	signatureHeader = "X-PSP-Signature"
	webhookSecret = os.Getenv("WEBHOOK_SECRET")
)

func SetupServer() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", handleWebhookPost)
	return mux
}

func handleWebhookPost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("read body: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := verifySignature(webhookSecret, body, r.Header.Get(signatureHeader)); err != nil {
		log.Printf("signature verification failed: %v", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var pe events.PaymentEvent
	if err := json.Unmarshal(body, &pe); err != nil {
		log.Printf("parse event: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	log.Printf("received webhook: %s", pe.EventID)

	w.WriteHeader(http.StatusOK)
}