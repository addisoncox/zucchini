package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

var ctx = context.Background()

func NewClient(addr string, password string, db int) *RedisClient {
	return &RedisClient{client: redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})}
}

func (r *RedisClient) Set(key string, value interface{}) {
	r.client.Set(ctx, key, value, 0)
}

func (r *RedisClient) Get(key string) interface{} {
	res, err := r.client.Do(ctx, "get", key).Result()
	if err != nil {
		panic(err.Error())
	}
	return res
}

func (r *RedisClient) LPush(key string, value interface{}) {
	r.client.LPush(ctx, key, value)
}

func (r *RedisClient) BRPop(key string) (string, error) {
	values, err := r.client.BRPop(ctx, time.Second, key).Result()
	if err != nil {
		return "", err
	}
	return values[1], err
}

func (r *RedisClient) LLen(key string) uint64 {
	if res, err := r.client.LLen(ctx, key).Uint64(); err != nil {
		panic(err.Error())
	} else {
		return res
	}
}

func (r *RedisClient) LRem(key string, count int64, value interface{}) {
	r.client.LRem(ctx, key, count, value)
}
