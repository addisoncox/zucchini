package zucchini

import (
	"context"
	"time"

	"github.com/go-redis/redis/v9"
)

type RedisClient struct {
	Client *redis.Client
}

func NewRedisClient(addr string, password string, db int) *RedisClient {
	return &RedisClient{Client: redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})}
}

var ctx = context.Background()

func (r *RedisClient) Set(key string, value interface{}) {
	r.Client.Set(ctx, key, value, 0)
}

func (r *RedisClient) Get(key string) interface{} {
	res, err := r.Client.Do(ctx, "get", key).Result()
	if err != nil {
		panic(err.Error())
	}
	return res
}

func (r *RedisClient) LPush(key string, value interface{}) {
	r.Client.LPush(ctx, key, value)
}

func (r *RedisClient) BRPop(key string) (string, error) {
	values, err := r.Client.BRPop(ctx, time.Second, key).Result()
	if err != nil {
		return "", err
	}
	return values[1], err
}

func (r *RedisClient) RPop(key string) (string, error) {
	value, err := r.Client.RPop(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return value, err
}

func (r *RedisClient) LLen(key string) uint64 {
	if res, err := r.Client.LLen(ctx, key).Uint64(); err != nil {
		panic(err.Error())
	} else {
		return res
	}
}

func (r *RedisClient) LRem(key string, count int64, value interface{}) uint64 {
	removed, _ := r.Client.LRem(ctx, key, count, value).Uint64()
	return removed
}
