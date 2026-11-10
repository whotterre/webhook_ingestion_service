package account

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockFailed = errors.New("failed to acquire account generation lock")
)

const (
	unlockScript  = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	lockKey       = "lock:account:generation"
	seqKey        = "account:number:seq"
	defaultPrefix = "6930" // Standard 4-digit bank prefix (e.g. Wema/ALAT virtual accounts)
)

type Generator struct {
	redis  *redis.Client
	prefix string
}

func NewGenerator(rdb *redis.Client) *Generator {
	prefix := os.Getenv("ACCOUNT_NUMBER_PREFIX")
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Generator{
		redis:  rdb,
		prefix: prefix,
	}
}

func (g *Generator) SetPrefix(prefix string) {
	g.prefix = prefix
}

// GenerateAccountNumber atomically increments a counter in Redis under a distributed lock
// and formats a strictly 10-digit account number with the bank prefix (e.g. 6930000001).
func (g *Generator) GenerateAccountNumber(ctx context.Context) (string, error) {
	token, err := g.AcquireLock(ctx, 5*time.Second, 5)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrLockFailed, err)
	}
	defer g.ReleaseLock(ctx, token)

	seq, err := g.redis.Incr(ctx, seqKey).Result()
	if err != nil {
		return "", fmt.Errorf("redis incr sequence: %w", err)
	}

	return FormatAccountNumber(g.prefix, seq), nil
}

// FormatAccountNumber formats an account number given a bank prefix and sequence.
// Guaranteed to produce an exact 10-digit NUBAN string (e.g. "6930000001").
func FormatAccountNumber(prefix string, seq int64) string {
	if prefix == "" {
		prefix = defaultPrefix
	}
	remaining := 10 - len(prefix)
	if remaining <= 0 {
		return prefix[:10]
	}
	mod := int64(1)
	for i := 0; i < remaining; i++ {
		mod *= 10
	}
	return fmt.Sprintf("%s%0*d", prefix, remaining, seq%mod)
}

func (g *Generator) AcquireLock(ctx context.Context, ttl time.Duration, retries int) (string, error) {
	token := newRandomToken()

	for i := 0; i <= retries; i++ {
		ok, err := g.redis.SetNX(ctx, lockKey, token, ttl).Result()
		if err == nil && ok {
			return token, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}

	return "", errors.New("timeout acquiring distributed lock")
}

func (g *Generator) ReleaseLock(ctx context.Context, token string) {
	if token == "" || g.redis == nil {
		return
	}
	_ = g.redis.Eval(ctx, unlockScript, []string{lockKey}, token).Err()
}

func newRandomToken() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
