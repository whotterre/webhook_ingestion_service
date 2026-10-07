package webhook

import (
	"io"
	"log"
	"net/http"
	"os"
)

var (
	signatureHeader = "X-PSP-Signature"
)

type MessagePublisher interface {
	Publish([]byte) error
}

type server struct {
	publisher MessagePublisher
}

func SetupServer(publisher MessagePublisher) http.Handler {
	mux := http.NewServeMux()
	h := &server{publisher: publisher}
	mux.HandleFunc("/webhook", h.handleWebhookPost)
	return mux
}

func (s *server) handleWebhookPost(w http.ResponseWriter, r *http.Request) {
	webhookSecret := os.Getenv("WEBHOOK_SECRET")

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

	if webhookSecret != "" && r.Header.Get(signatureHeader) != "" {
		if err := verifySignature(webhookSecret, body, r.Header.Get(signatureHeader)); err != nil {
			log.Printf("signature verification failed: %v", err)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	log.Printf("received webhook: %s", string(body))

	if s.publisher != nil {
		if err := s.publisher.Publish(body); err != nil {
			log.Printf("publish to kafka: %v", err)
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		log.Printf("published webhook to kafka")
	} else {
		log.Printf("kafka publisher not configured; skipping publish")
	}

	w.WriteHeader(http.StatusOK)
}
