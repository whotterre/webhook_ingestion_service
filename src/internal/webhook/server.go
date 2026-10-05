package webhook

import (
	"io"
	"log"
	"net/http"
)

func SetupServer() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", handleWebhookPost)
	return mux
}

func handleWebhookPost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	log.Printf("Received webhook: %s", string(body))

	w.WriteHeader(http.StatusOK)
}
