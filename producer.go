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
	serializer   serializer
}

func NewProducer[TaskArgType, TaskResultType any](
	taskDefinition TaskDefinition[TaskArgType, TaskResultType],
	redis *RedisClient,
) Producer[TaskArgType, TaskResultType] {
	serializer := serializer{
		json.Marshal,
		json.Unmarshal,
	}
	if taskDefinition.Options.CustomSerializer != nil {
		serializer.serialize = taskDefinition.Options.CustomSerializer.Serialize
		serializer.deserialize = taskDefinition.Options.CustomSerializer.Deserialize
	}
	return Producer[TaskArgType, TaskResultType]{
		redis:        redis,
		taskCallback: taskDefinition.TaskCallback,
		taskName:     taskDefinition.TaskName,
		taskTimeout:  taskDefinition.Timeout,
		serializer:   serializer,
	}
}

func (p *Producer[TaskArgType, TaskResultType]) commandQueueName() string {
	return internal.ZUCCHINI_CMD_PREFIX + p.taskName
}

func (p *Producer[TaskArgType, TaskResultType]) taskQueueName() string {
	return internal.ZUCCHINI_TASK_PREFIX + p.taskName
}

func (p *Producer[TaskArgType, TaskResultType]) resultQueueName() string {
	return internal.ZUCCHINI_RES_PREFIX + p.taskName
}

func (p *Producer[TaskArgType, TaskResultType]) QueueTask(args TaskArgType) TaskID {
	taskID := TaskID(uuid.New())
	taskPayloadBytes, err := p.serializer.serialize(
		internal.TaskPayload[TaskArgType]{
			ID:       uuid.UUID(taskID),
			Timeout:  p.taskTimeout,
			Argument: args,
		})
	if err != nil {
		panic("Could not marshall task data.")
	}
	p.redis.LPush(p.taskQueueName(), taskPayloadBytes)
	return taskID
}

func (p *Producer[TaskArgType, TaskResultType]) AwaitCallback() {
	for {
		if p.redis.LLen(p.resultQueueName()) == 0 {
			time.Sleep(time.Second)
			continue
		}
		resultData, _ := p.redis.BRPop(p.resultQueueName())
		if p.taskCallback == nil {
			return
		}
		var result TaskResult
		var resultValue TaskResultType
		p.serializer.deserialize([]byte(resultData), &result)
		p.serializer.deserialize(result.Value, &resultValue)
		p.taskCallback(result.Status, resultValue)
	}
}

func (p *Producer[TaskArgType, TaskResultType]) CancelTask(taskID TaskID) {
	cmdPayload, _ := p.serializer.serialize(
		internal.TaskCommand{
			TaskId:  uuid.UUID(taskID),
			Command: "cancel",
		},
	)
	p.redis.LPush(p.commandQueueName(), cmdPayload)
}
