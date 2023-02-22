package zucchini

import (
	"time"

	"github.com/google/uuid"
)

type TaskStatus int
type TaskID uuid.UUID

const (
	Failed TaskStatus = iota
	Succeeded
	Timeout
)

type TaskResult struct {
	Status TaskStatus
	Value  []byte
}

type RetryStrategy uint

const (
	ExponentialBackoff RetryStrategy = iota
	SetDelay
	Custom
)

type TaskDefinition[TaskArgType, TaskResultType any] struct {
	TaskHandler         func(TaskArgType) TaskResultType
	TaskCallback        func(TaskStatus, TaskResultType) error
	Timeout             time.Duration
	TaskName            string
	MaxRetries          uint
	RetryStrategy       RetryStrategy
	RetryJitter         time.Duration
	RetryDelay          time.Duration
	CustomRetryFunction func(uint) time.Duration
}
