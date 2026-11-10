package dedup

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/whotterre/webhook_ingestion_service/internal/metrics"
)

type Deduplicator interface {
	// CheckAndMark returns true if the key was already seen (duplicate), or false if it is new and successfully marked.
	CheckAndMark(ctx context.Context, scope, key string, ttl time.Duration) (bool, error)
	// Close cleans up connections.
	Close() error
}

type RedisDeduplicator struct {
	client       *redis.Client
	hasBloomMod  bool
	bloomFilter  string
}

func NewRedisDeduplicator(redisURL string) (*RedisDeduplicator, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		opts = &redis.Options{
			Addr: redisURL,
		}
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	d := &RedisDeduplicator{
		client:      client,
		bloomFilter: "webhooks_bloom_filter",
	}

	bloomTest := client.Do(ctx, "BF.EXISTS", d.bloomFilter, "probe_key")
	if bloomTest.Err() == nil || strings.Contains(strings.ToLower(bloomTest.Err().Error()), "not found") {
		d.hasBloomMod = true
		log.Println("[dedup] RedisBloom module detected, using Bloom filters")
		_ = client.Do(ctx, "BF.RESERVE", d.bloomFilter, 0.001, 10000000).Err()
	} else {
		log.Printf("[dedup] Standard Redis detected (Bloom module not present: %v), using atomic SETNX keys with TTL", bloomTest.Err())
	}

	return d, nil
}

// CheckAndMark checks if scope:key exists. If duplicate, returns (true, nil).
// If new, records it and returns (false, nil).
func (d *RedisDeduplicator) CheckAndMark(ctx context.Context, scope, key string, ttl time.Duration) (bool, error) {
	if d == nil || d.client == nil {
		return false, nil
	}

	compositeKey := fmt.Sprintf("dedup:%s:%s", scope, key)

	if d.hasBloomMod {
		bloomKey := fmt.Sprintf("%s:%s", scope, key)
		existsCmd := d.client.Do(ctx, "BF.EXISTS", d.bloomFilter, bloomKey)
		if exists, err := existsCmd.Int(); err == nil && exists == 1 {
			metrics.DedupChecksTotal.WithLabelValues("hit").Inc()
			return true, nil
		}

		addCmd := d.client.Do(ctx, "BF.ADD", d.bloomFilter, bloomKey)
		if err := addCmd.Err(); err != nil {
			metrics.DedupChecksTotal.WithLabelValues("error").Inc()
			return false, err
		}
		metrics.DedupChecksTotal.WithLabelValues("miss").Inc()
		return false, nil
	}

	// Standard Redis atomic SETNX with expiration
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	set, err := d.client.SetNX(ctx, compositeKey, "1", ttl).Result()
	if err != nil {
		metrics.DedupChecksTotal.WithLabelValues("error").Inc()
		return false, fmt.Errorf("redis setnx failed: %w", err)
	}

	if !set {
		// Key already existed -> Duplicate
		metrics.DedupChecksTotal.WithLabelValues("hit").Inc()
		return true, nil
	}

	metrics.DedupChecksTotal.WithLabelValues("miss").Inc()
	return false, nil
}

func (d *RedisDeduplicator) Close() error {
	if d != nil && d.client != nil {
		return d.client.Close()
	}
	return nil
}
