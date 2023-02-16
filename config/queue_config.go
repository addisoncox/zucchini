package config

import (
	"time"

	"github.com/addisoncox/zucchini/redis"
)

type RetryStrategy uint

const (
	ExponentialBackoff RetryStrategy = iota
	ExponentialBackoffWithJitter
	Custom
)

type QueueConfig struct {
	Name           string
	Capacity       uint64
	Redis          redis.RedisClient
	GoroutineLimit uint64
	RetryStrategy  RetryStrategy
	RetryLimit     uint
	// given nth retry, return how much time to wait
	CustomRetryFunction func(uint) time.Duration
}
