package zucchini

import (
	"time"

	"github.com/addisoncox/zucchini/internal"
	"github.com/google/uuid"
)

type TaskStatus struct {
	Status internal.TaskStatus
}

func (t TaskStatus) Failed() bool {
	return t.Status == internal.Failed
}

func (t TaskStatus) Succeeded() bool {
	return t.Status == internal.Succeeded
}

func (t TaskStatus) Queued() bool {
	return t.Status == internal.Queued
}

func (t TaskStatus) Processing() bool {
	return t.Status == internal.Processing
}

func (t TaskStatus) Cancelled() bool {
	return t.Status == internal.Cancelled
}

func (t TaskStatus) String() string {
	switch taskStatus := t.Status; taskStatus {
	case internal.Failed:
		return "failed"
	case internal.Succeeded:
		return "succeeded"
	case internal.Queued:
		return "queued"
	case internal.Processing:
		return "processing"
	case internal.Cancelled:
		return "cancelled"
	}

	return "unknown task status"
}

type TaskID uuid.UUID

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
	TaskHandler  func(TaskArgType) TaskResultType
	TaskCallback func(TaskStatus, TaskResultType) error
	Timeout      time.Duration
	TaskName     string
	Options      TaskDefinitionOptions
}

type CustomSerializer interface {
	Serialize(v any) ([]byte, error)
	Deserialize(data []byte, v any) error
}

type TaskDefinitionOptions struct {
	MaxRetries          uint
	RetryStrategy       RetryStrategy
	RetryJitter         time.Duration
	RetryDelay          time.Duration
	CustomRetryFunction func(uint) time.Duration
	CustomSerializer    CustomSerializer
}
