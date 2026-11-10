package account

import (
	"database/sql"

	"github.com/redis/go-redis/v9"
)

type Service struct {
	db        *sql.DB
	redis     *redis.Client
	generator *Generator
}

func NewService(db *sql.DB, rdb *redis.Client) *Service {
	gen := NewGenerator(rdb)
	return &Service{
		db:        db,
		redis:     rdb,
		generator: gen,
	}
}

func (s *Service) Generator() *Generator {
	return s.generator
}
