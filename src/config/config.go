package config

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type KafkaConfig struct {
	Brokers       []string
	GroupID 	  string
	Topic         string
	Username      string
	Password      string
	SASLMechanism string
	CACertPath    string
	TLS           bool
}

type BrevoConfig struct {
	BrevoAPIKey string
	BrevoSMTPServer string
	BrevoSMTPLogin string // host
	BrevoFromEmail string
	TestDestEmail string 
}


func (k KafkaConfig) Enabled() bool {
	return len(k.Brokers) > 0 && strings.TrimSpace(k.Topic) != ""
}

type Config struct {
	ArchiveDir string
	HTTPPort string
	Kafka    KafkaConfig
	BrevoConfig BrevoConfig
	MetricsPort string
	RabbitMQURL string 
	RabbitMQExchange string
}

func Load() *Config {
	_ = godotenv.Load("../.env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load()
	_ = godotenv.Load(".env")

	caCertPath := getEnv("KAFKA_CA_CERT_PATH", "")
	if resolved := resolveKafkaCACertPath(caCertPath); resolved != "" {
		caCertPath = resolved
	}

	return &Config{
		ArchiveDir: getEnv("ARCHIVE_DIR", "./archives"),
		HTTPPort: getEnv("HTTP_PORT", "8080"),
		Kafka: KafkaConfig{
			Brokers:       splitCSV(getEnv("KAFKA_BROKERS", "")),
			GroupID:       getEnv("KAFKA_GROUP_ID", ""),
			Topic:         getEnv("KAFKA_TOPIC", "webhooks"),
			Username:      getEnv("KAFKA_USERNAME", ""),
			Password:      getEnv("KAFKA_PASSWORD", ""),
			SASLMechanism: getEnv("KAFKA_SASL_MECHANISM", "scram-sha-512"),
			CACertPath:    caCertPath,
			TLS:           getBoolEnv("KAFKA_TLS", true),
		},
		BrevoConfig: BrevoConfig{
			BrevoAPIKey: getEnv("BREVO_API_KEY", ""),
			BrevoSMTPServer: getEnv("BREVO_SMTP_SERVER", ""),
			BrevoSMTPLogin: getEnv("BREVO_SMTP_LOGIN", ""),
			BrevoFromEmail: getEnv("BREVO_FROM_EMAIL", ""),
			TestDestEmail: getEnv("TEST_DEST_EMAIL", ""),
		},
		MetricsPort: getEnv("METRICS_PORT", ""),
		RabbitMQURL: getEnv("RABBITMQ_URL", ""),
		RabbitMQExchange: getEnv("RABBITMQ_EXCHANGE", "webhooks"),
	}
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getBoolEnv(key string, defaultVal bool) bool {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultVal
	}

	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultVal
	}
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		broker = strings.TrimPrefix(broker, "http://")
		broker = strings.TrimPrefix(broker, "https://")
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}

func defaultKafkaCACertPath() string {
	candidates := []string{
		"../../cert.pem",
		"../cert.pem",
		"cert.pem",
		"src/cert.pem",
		"../src/cert.pem",
	}

	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}

	return ""
}

func resolveKafkaCACertPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return defaultKafkaCACertPath()
	}

	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, "path/to/ca.pem") || strings.Contains(lower, "replace/me") {
		return defaultKafkaCACertPath()
	}

	if info, err := os.Stat(trimmed); err == nil && !info.IsDir() {
		return trimmed
	}

	return defaultKafkaCACertPath()
}
