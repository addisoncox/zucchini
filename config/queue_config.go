package config

import "github.com/addisoncox/zucchini/redis"

type QueueConfig struct {
	Name           string
	Capacity       uint64
	Redis          redis.RedisClient
	GoroutineLimit uint64
}
