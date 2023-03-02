package internal

import (
	"time"

	"github.com/google/uuid"
)

const ZUCCHINI_TASK_PREFIX = "zhc:task:"
const ZUCCHINI_RES_PREFIX = "zhc:res:"
const ZUCCHINI_CMD_PREFIX = "zhc:cmd:"

type TaskCommand struct {
	TaskId  uuid.UUID
	Command string
}

type TaskPayload[T any] struct {
	ID       uuid.UUID
	Timeout  time.Duration
	Argument T
}

type TaskStatus uint8

const (
	Queued TaskStatus = iota
	Processing
	Failed
	Succeeded
	Cancelled
)

type Task[T any] struct {
	Payload TaskPayload[T]
	Status  TaskStatus
	Retries uint
}
