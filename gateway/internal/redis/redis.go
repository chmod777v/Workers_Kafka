package my_redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisAdapter struct {
	client *redis.Client
}

func NewAdapter(addr, password string) (*RedisAdapter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0, //бд по умолчанию
	})
	adapter := &RedisAdapter{client: client}

	if err := adapter.Ping(); err != nil {
		client.Close()
		return nil, err
	}
	return adapter, nil
}

func (r *RedisAdapter) AddTask(ctx context.Context, token, message string) error {
	return r.client.Set(ctx, token, message, 24*time.Hour).Err()
}

func (r *RedisAdapter) GetTask(ctx context.Context, token string) (string, error) {
	message, err := r.client.Get(ctx, token).Result()
	return message, err
}

func (r *RedisAdapter) Ping() error {
	retries := 3
	for attempt := 1; attempt <= retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := r.client.Ping(ctx).Result()
		cancel()

		if err == nil {
			return nil
		}
		if attempt == retries {
			return fmt.Errorf("Failed ping redis: %s", err.Error())
		}
		time.Sleep(3 * time.Second)
	}
	return nil
}

func (r *RedisAdapter) Close() {
	r.client.Close()
}
