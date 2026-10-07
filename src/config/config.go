package config

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type KafkaConfig struct {
	Brokers       []string
	Topic         string
	Username      string
	Password      string
	SASLMechanism string
	CACertPath    string
	TLS           bool
}

func (k KafkaConfig) Enabled() bool {
	return len(k.Brokers) > 0 && strings.TrimSpace(k.Topic) != ""
}

type Config struct {
	HTTPPort string
	Kafka    KafkaConfig
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
		HTTPPort: getEnv("HTTP_PORT", "8080"),
		Kafka: KafkaConfig{
			Brokers:       splitCSV(getEnv("KAFKA_BROKERS", "")),
			Topic:         getEnv("KAFKA_TOPIC", "webhooks"),
			Username:      getEnv("KAFKA_USERNAME", ""),
			Password:      getEnv("KAFKA_PASSWORD", ""),
			SASLMechanism: getEnv("KAFKA_SASL_MECHANISM", "scram-sha-512"),
			CACertPath:    caCertPath,
			TLS:           getBoolEnv("KAFKA_TLS", true),
		},
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
