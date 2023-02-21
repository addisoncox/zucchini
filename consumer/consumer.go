package consumer

import (
	"encoding/json"
	"math"
	"math/rand"
	"time"

	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
	"github.com/addisoncox/zucchini/util"
)

type Consumer[TaskArgType, TaskResultType any] struct {
	redis               *redis.RedisClient
	taskHandler         func(TaskArgType) TaskResultType
	taskName            string
	retryStrategy       task.RetryStrategy
	maxRetries          uint
	retryJitter         time.Duration
	retryDelay          time.Duration
	customRetryFunction func(uint) time.Duration
	maxConcurrency      uint64
	currentConcurrency  uint64
	taskRetryCounter    map[task.TaskID]uint
}

func NewConsumer[TaskArgType, TaskResultType any](
	taskDefinition task.TaskDefinition[TaskArgType, TaskResultType],
	redis *redis.RedisClient,
	maxConcurrency uint64,
) Consumer[TaskArgType, TaskResultType] {
	return Consumer[TaskArgType, TaskResultType]{
		redis:               redis,
		taskHandler:         taskDefinition.TaskHandler,
		taskName:            taskDefinition.TaskName,
		retryStrategy:       taskDefinition.RetryStrategy,
		maxRetries:          taskDefinition.MaxRetries,
		retryJitter:         taskDefinition.RetryJitter,
		retryDelay:          taskDefinition.RetryDelay,
		customRetryFunction: taskDefinition.CustomRetryFunction,
		maxConcurrency:      maxConcurrency,
		currentConcurrency:  0,
		taskRetryCounter:    make(map[task.TaskID]uint),
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) handleTimeout(retryCount uint) {
	if c.retryStrategy == task.ExponentialBackoff {
		backoffTime := c.retryDelay * time.Duration(math.Pow(2, float64(retryCount)))
		rand.Seed(time.Now().UnixNano())
		jitter := c.retryJitter * time.Duration(rand.Float64()*2)
		time.Sleep(backoffTime + jitter)
	} else if c.retryStrategy == task.SetDelay {
		rand.Seed(time.Now().UnixNano())
		jitter := c.retryDelay * time.Duration(rand.Float64()*2)
		time.Sleep(c.retryJitter + jitter)
	} else if c.retryStrategy == task.Custom {
		time.Sleep(c.customRetryFunction(retryCount))
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) processTask(
	handlerFunc func(TaskArgType) TaskResultType,
	taskID task.TaskID,
	arg TaskArgType,
	timeout time.Duration,
) {
	result := make(chan interface{}, 1)
	go func() {
		result <- handlerFunc(arg)
	}()

	select {
	case resultValue := <-result:
		serializedResultValue, _ := json.Marshal(resultValue)
		serializedTaskResult, _ := json.Marshal(task.TaskResult{
			Status: task.Succeeded,
			Value:  serializedResultValue,
		})
		c.redis.LPush(task.ZUCCHINI_RES_PREFIX+c.taskName, serializedTaskResult)
		util.AtomicDec(&c.currentConcurrency)
	case <-time.After(timeout):
		var taskResult task.TaskResult
		if c.taskRetryCounter[taskID] > c.maxRetries {
			taskResult = task.TaskResult{
				Status: task.Failed,
				Value:  []byte{},
			}
			delete(c.taskRetryCounter, taskID)
			util.AtomicDec(&c.currentConcurrency)
		} else {
			taskResult = task.TaskResult{
				Status: task.Timeout,
				Value:  []byte{},
			}
			c.taskRetryCounter[taskID]++
			c.handleTimeout(c.taskRetryCounter[taskID])
			c.processTask(handlerFunc, taskID, arg, timeout)
		}
		c.redis.LPush(task.ZUCCHINI_RES_PREFIX+c.taskName, taskResult)
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) ProcessTasks() {
	for {
		if c.currentConcurrency >= c.maxConcurrency {
			time.Sleep(time.Second)
			continue
		}
		if c.redis.LLen(task.ZUCCHINI_TASK_PREFIX+c.taskName) == 0 {
			time.Sleep(time.Second)
			continue
		}
		taskData, err := c.redis.BRPop(task.ZUCCHINI_TASK_PREFIX + c.taskName)
		if err != nil {
			panic(err.Error())
		}
		var taskPayload task.TaskPayload
		var taskArg TaskArgType
		json.Unmarshal([]byte(taskData), &taskPayload)
		serializedArg, _ := json.Marshal(taskPayload.Argument)
		json.Unmarshal(serializedArg, &taskArg)
		util.AtomicInc(&c.currentConcurrency)
		go c.processTask(
			c.taskHandler,
			taskPayload.ID,
			taskArg,
			taskPayload.Timeout,
		)
	}
}
