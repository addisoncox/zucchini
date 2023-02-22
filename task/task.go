package task

import (
	"encoding/json"
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

const ZUCCHINI_TASK_PREFIX = "zhc:task:"
const ZUCCHINI_RES_PREFIX = "zhc:res:"
const ZUCCHINI_CMD_PREFIX = "zhc:cmd:"

type TaskCommand struct {
	TaskId  TaskID
	Command string
}

type TaskPayload struct {
	ID       TaskID
	Timeout  time.Duration
	Argument interface{}
}

// Prod[T, B] (mytask)
type Task struct {
	Name    string
	Data    interface{}
	Timeout time.Duration
}

func (t TaskResult) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t TaskResult) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, &t.Value)
}

func (t TaskResult) Succeeded() bool {
	return t.Status == Succeeded
}
