package zucchini

import (
	"encoding/json"
	"time"

	"github.com/addisoncox/zucchini/internal"
	"github.com/google/uuid"
)

type Producer[TaskArgType, TaskResultType any] struct {
	redis        *RedisClient
	taskCallback func(TaskStatus, TaskResultType) error
	taskName     string
	taskTimeout  time.Duration
	maxCapacity  uint64
	taskCount    uint64
}

func NewProducer[TaskArgType, TaskResultType any](
	taskDefinition TaskDefinition[TaskArgType, TaskResultType],
	redis *RedisClient,
	maxCapacity uint64,
) Producer[TaskArgType, TaskResultType] {
	return Producer[TaskArgType, TaskResultType]{
		redis:        redis,
		taskCallback: taskDefinition.TaskCallback,
		taskName:     taskDefinition.TaskName,
		taskTimeout:  taskDefinition.Timeout,
		maxCapacity:  maxCapacity,
		taskCount:    0,
	}
}

func (p *Producer[TaskArgType, TaskResultType]) QueueTask(args TaskArgType) TaskID {
	if p.taskCount < p.maxCapacity {
		taskID := TaskID(uuid.New())
		taskBytes, err := json.Marshal(
			internal.TaskPayload{
				ID:       uuid.UUID(taskID),
				Timeout:  p.taskTimeout,
				Argument: args,
			})
		if err != nil {
			panic("Could not marshall task data.")
		}
		p.redis.LPush(internal.ZUCCHINI_TASK_PREFIX+p.taskName, taskBytes)
		internal.AtomicInc(&p.taskCount)
		return taskID
	} else {
		panic("Tried to enqueue more tasks than queue capacity")
	}
}

func (p *Producer[TaskArgType, TaskResultType]) AwaitCallback() {
	for {
		if p.redis.LLen(internal.ZUCCHINI_RES_PREFIX+p.taskName) == 0 {
			time.Sleep(time.Second)
			continue
		}
		resultData, _ := p.redis.BRPop(internal.ZUCCHINI_RES_PREFIX + p.taskName)
		internal.AtomicDec(&p.taskCount)
		if p.taskCallback == nil {
			return
		}
		var result TaskResult
		var resultValue TaskResultType
		json.Unmarshal([]byte(resultData), &result)
		json.Unmarshal(result.Value, &resultValue)
		p.taskCallback(result.Status, resultValue)
	}
}

func (p *Producer[TaskArgType, TaskResultType]) CancelTask(taskID TaskID) {
	cmdPayload, _ := json.Marshal(
		internal.TaskCommand{
			TaskId:  uuid.UUID(taskID),
			Command: "cancel",
		},
	)
	p.redis.LPush(internal.ZUCCHINI_CMD_PREFIX+p.taskName, cmdPayload)
}
