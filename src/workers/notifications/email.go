package notifications

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"time"

	"github.com/whotterre/webhook_ingestion_service/config"
	"github.com/whotterre/webhook_ingestion_service/internal/events"
)

type Deliverer interface {
	Deliver(ctx context.Context, evt events.PaymentEvent) error
}

type EmailNotifier struct {
	tmpl *template.Template
	DB *sql.DB
}

func NewEmailNotifier(templatePath string) (*EmailNotifier, error) {
	tmpl, err := template.ParseFiles(templatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse email template: %w", err)
	}

	return &EmailNotifier{tmpl: tmpl}, nil
}

func (n *EmailNotifier) BuildReceipt(data *ReceiptData) (string, error) {
	if data == nil {
		return "", fmt.Errorf("receipt data cannot be nil")
	}

	var buf bytes.Buffer
	if err := n.tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute receipt template: %w", err)
	}

	return buf.String(), nil
}

func (n *EmailNotifier) Deliver(ctx context.Context, evt events.PaymentEvent) error {
	data := &ReceiptData{
		EventType:  events.EventType(evt.EventType.String()),
		CustomerID: evt.Data.CustomerID,
		PaymentID:  evt.PaymentID,
		Currency:   evt.Currency,
		Amount:     evt.Amount,
	}

	htmlBody, err := n.BuildReceipt(data)
	if err != nil {
		return fmt.Errorf("build receipt: %w", err)
	}

	err = n.sendViaProvider(ctx, data.CustomerID, "Your Payment Receipt", htmlBody)
	if err != nil {
		return fmt.Errorf("send email via provider: %w", err)
	}

	return nil
}


func (n *EmailNotifier) sendViaProvider(ctx context.Context, customerID, subject, htmlBody string) error {
	cfg := config.Load()
	apiKey := cfg.BrevoConfig.BrevoAPIKey
	fromEmail := cfg.BrevoConfig.BrevoFromEmail

	if apiKey == "" || fromEmail == "" {
		return fmt.Errorf("brevo config missing: api_key=%v from_email=%v", apiKey != "", fromEmail != "")
	}

	toEmail := cfg.BrevoConfig.TestDestEmail
	if toEmail == "" {
		return fmt.Errorf("TEST_DEST_EMAIL is empty; cannot determine recipient")
	}

	// Build the Brevo API payload
	payload := map[string]interface{}{
		"sender": map[string]string{
			"email": fromEmail,
			"name":  "Payments",
		},
		"to": []map[string]string{
			{"email": toEmail},
		},
		"subject":     subject,
		"htmlContent": htmlBody,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal brevo payload: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://api.brevo.com/v3/smtp/email",
		bytes.NewBuffer(bodyBytes),
	)
	if err != nil {
		return fmt.Errorf("create brevo request: %w", err)
	}
	req.Header.Set("api-key", apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Brevo API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Brevo API returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}