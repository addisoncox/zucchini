package producer

import (
	"encoding/json"
	"time"

	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
	"github.com/addisoncox/zucchini/util"
	"github.com/google/uuid"
)

type Producer[TaskArgType, TaskResultType any] struct {
	redis        *redis.RedisClient
	taskCallback func(task.TaskStatus, TaskResultType) error
	taskName     string
	taskTimeout  time.Duration
	maxCapacity  uint64
	taskCount    uint64
}

func NewProducer[TaskArgType, TaskResultType any](
	taskDefinition task.TaskDefinition[TaskArgType, TaskResultType],
	redis *redis.RedisClient,
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

func (p *Producer[TaskArgType, TaskResultType]) QueueTask(args TaskArgType) task.TaskID {
	if p.taskCount < p.maxCapacity {
		taskID := task.TaskID(uuid.New())
		taskBytes, err := json.Marshal(
			task.TaskPayload{
				ID:       taskID,
				Timeout:  p.taskTimeout,
				Argument: args,
			})
		if err != nil {
			panic("Could not marshall task data.")
		}
		p.redis.LPush(task.ZUCCHINI_TASK_PREFIX+p.taskName, taskBytes)
		util.AtomicInc(&p.taskCount)
		return taskID
	} else {
		panic("Tried to enqueue more tasks than queue capacity")
	}
}

func (p *Producer[TaskArgType, TaskResultType]) AwaitCallback() {
	for {
		if p.redis.LLen(task.ZUCCHINI_RES_PREFIX+p.taskName) == 0 {
			time.Sleep(time.Second)
			continue
		}
		resultData, _ := p.redis.BRPop(task.ZUCCHINI_RES_PREFIX + p.taskName)
		util.AtomicDec(&p.taskCount)
		if p.taskCallback == nil {
			return
		}
		var result task.TaskResult
		var resultValue TaskResultType
		json.Unmarshal([]byte(resultData), &result)
		json.Unmarshal(result.Value, &resultValue)
		p.taskCallback(result.Status, resultValue)
	}
}

func (p *Producer[TaskArgType, TaskResultType]) CancelTask(taskID task.TaskID) {
	cmdPayload, _ := json.Marshal(
		task.TaskCommand{
			TaskId:  taskID,
			Command: "cancel",
		},
	)
	p.redis.LPush(task.ZUCCHINI_CMD_PREFIX+p.taskName, cmdPayload)
}
