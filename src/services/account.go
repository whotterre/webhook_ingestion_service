package services

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

const defaultPrefix = "8099"

type AccountService struct {
	redis  *redis.Client
	prefix string
}

func NewAccountService(rdb *redis.Client) *AccountService {
	prefix := os.Getenv("ACCOUNT_NUMBER_PREFIX")
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &AccountService{
		redis:  rdb,
		prefix: prefix,
	}
}

// GenerateAccountNumber atomically increments a counter in Redis and formats a 10-digit account number with prefix (e.g. 6930xxxxxx).
func (s *AccountService) GenerateAccountNumber(ctx context.Context) (string, error) {
	seq, err := s.redis.Incr(ctx, "account:number:seq").Result()
	if err != nil {
		return "", err
	}

	remaining := 10 - len(s.prefix)
	if remaining <= 0 {
		return s.prefix[:10], nil
	}
	mod := int64(1)
	for range remaining {
		mod *= 10
	}
	return fmt.Sprintf("%s%0*d", s.prefix, remaining, seq%mod), nil
}
