package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPPort string
}

func Load() *Config {
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load()
	_ = godotenv.Load(".env")

	return &Config{
		HTTPPort: getEnv("HTTP_PORT", "8080"),
	}
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}
