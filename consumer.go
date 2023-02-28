package zucchini

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/addisoncox/zucchini/internal"
	"github.com/google/uuid"
)

type Consumer[TaskArgType, TaskResultType any] struct {
	redis               *RedisClient
	taskHandler         func(TaskArgType) TaskResultType
	taskName            string
	retryStrategy       RetryStrategy
	maxRetries          uint
	retryJitter         time.Duration
	retryDelay          time.Duration
	customRetryFunction func(uint) time.Duration
	maxConcurrency      uint64
	currentConcurrency  uint64
	taskRetryCounter    map[TaskID]uint
	taskTimeouts        map[TaskID]time.Duration
	taskArgs            map[TaskID]TaskArgType
	taskStatuses        map[TaskID]internal.TaskStatus
	cancelQueue         map[TaskID]bool
	processedQueued     []TaskID
}

type taskData[T any] struct {
	ID      string
	Arg     T
	Status  string
	Retries uint
}

func NewConsumer[TaskArgType, TaskResultType any](
	taskDefinition TaskDefinition[TaskArgType, TaskResultType],
	redis *RedisClient,
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
		taskRetryCounter:    make(map[TaskID]uint),
		taskTimeouts:        make(map[TaskID]time.Duration),
		taskArgs:            make(map[TaskID]TaskArgType),
		taskStatuses:        make(map[TaskID]internal.TaskStatus),
		cancelQueue:         make(map[TaskID]bool),
		processedQueued:     make([]TaskID, 0),
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) commandQueueName() string {
	return internal.ZUCCHINI_CMD_PREFIX + c.taskName
}

func (c *Consumer[TaskArgType, TaskResultType]) taskQueueName() string {
	return internal.ZUCCHINI_TASK_PREFIX + c.taskName
}

func (c *Consumer[TaskArgType, TaskResultType]) resultQueueName() string {
	return internal.ZUCCHINI_RES_PREFIX + c.taskName
}

func monitor(w http.ResponseWriter, req *http.Request) {
	http.ServeFile(w, req, "../static/monitor.html")
}

func (c *Consumer[TaskArgType, TaskResultType]) taskInfo(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tasksData := make([]taskData[TaskArgType], 0)
	for id, arg := range c.taskArgs {
		tasksData = append(tasksData, taskData[TaskArgType]{
			ID:      uuid.UUID(id).String(),
			Arg:     arg,
			Status:  TaskStatus{Status: c.taskStatuses[id]}.String(),
			Retries: c.taskRetryCounter[id],
		})
	}
	json.NewEncoder(w).Encode(tasksData)
}

func (c *Consumer[TaskArgType, TaskResultType]) StartMonitorServer(addr string) {
	http.HandleFunc("/", monitor)
	http.HandleFunc("/info", c.taskInfo)
	http.ListenAndServe(addr, nil)
}

func (c *Consumer[TaskArgType, TaskResultType]) handleTimeout(retryCount uint) {
	if c.retryStrategy == ExponentialBackoff {
		backoffTime := c.retryDelay * time.Duration(math.Pow(2, float64(retryCount)))
		rand.Seed(time.Now().UnixNano())
		jitter := c.retryJitter * time.Duration(rand.Float64()*2)
		time.Sleep(backoffTime + jitter)
	} else if c.retryStrategy == SetDelay {
		rand.Seed(time.Now().UnixNano())
		jitter := c.retryDelay * time.Duration(rand.Float64()*2)
		time.Sleep(c.retryJitter + jitter)
	} else if c.retryStrategy == Custom {
		time.Sleep(c.customRetryFunction(retryCount))
	}
}
func (c *Consumer[TaskArgType, TaskResultType]) clearTaskData(taskID TaskID) {
	delete(c.taskRetryCounter, taskID)
	delete(c.taskArgs, taskID)
	delete(c.taskTimeouts, taskID)
	delete(c.taskStatuses, taskID)
}

func (c *Consumer[TaskArgType, TaskResultType]) processTask(
	handlerFunc func(TaskArgType) TaskResultType,
	taskID TaskID,
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
		serializedTaskResult, _ := json.Marshal(TaskResult{
			Status: TaskStatus{Status: internal.Succeeded},
			Value:  serializedResultValue,
		})
		c.redis.LPush(internal.ZUCCHINI_RES_PREFIX+c.taskName, serializedTaskResult)
		c.clearTaskData(taskID)
		internal.AtomicDec(&c.currentConcurrency)
	case <-time.After(timeout):
		if c.taskRetryCounter[taskID] > c.maxRetries {
			taskResult := TaskResult{
				Status: TaskStatus{Status: internal.Failed},
				Value:  []byte{},
			}
			c.clearTaskData(taskID)
			c.redis.LPush(internal.ZUCCHINI_RES_PREFIX+c.taskName, taskResult)
			internal.AtomicDec(&c.currentConcurrency)
		} else {
			c.taskRetryCounter[taskID]++
			c.handleTimeout(c.taskRetryCounter[taskID])
			c.processTask(handlerFunc, taskID, arg, timeout)
		}
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) cancelTask(taskID TaskID) error {
	taskPayloadData, _ := json.Marshal(
		internal.TaskPayload{
			ID:       uuid.UUID(taskID),
			Timeout:  c.taskTimeouts[taskID],
			Argument: c.taskArgs[taskID],
		},
	)
	if c.redis.LRem(internal.ZUCCHINI_TASK_PREFIX+c.taskName, 1, taskPayloadData) == 0 {
		c.cancelQueue[taskID] = true
	}
	return nil
}

func (c *Consumer[TaskArgType, TaskResultType]) handleCommand(taskID TaskID, command string) {
	if command == "cancel" {
		c.cancelTask(taskID)
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) ProcessTasks() {
	for {
		for c.redis.LLen(internal.ZUCCHINI_CMD_PREFIX+c.taskName) != 0 {
			cmdData, err := c.redis.BRPop(internal.ZUCCHINI_CMD_PREFIX + c.taskName)
			var cmd internal.TaskCommand
			json.Unmarshal([]byte(cmdData), &cmd)
			if err != nil {
				panic(err.Error())
			}
			c.handleCommand(TaskID(cmd.TaskId), cmd.Command)
		}
		if c.redis.LLen(internal.ZUCCHINI_TASK_PREFIX+c.taskName) == 0 {
			time.Sleep(time.Second)
			continue
		}
		taskData, err := c.redis.BRPop(internal.ZUCCHINI_TASK_PREFIX + c.taskName)
		if err != nil {
			panic(err.Error())
		}
		var taskPayload internal.TaskPayload
		var taskArg TaskArgType
		json.Unmarshal([]byte(taskData), &taskPayload)
		_, taskCancelled := c.cancelQueue[TaskID(taskPayload.ID)]
		if taskCancelled {
			delete(c.cancelQueue, TaskID(taskPayload.ID))
			continue
		}
		serializedArg, _ := json.Marshal(taskPayload.Argument)
		json.Unmarshal(serializedArg, &taskArg)
		internal.AtomicInc(&c.currentConcurrency)
		c.taskArgs[TaskID(taskPayload.ID)] = taskArg
		c.taskTimeouts[TaskID(taskPayload.ID)] = taskPayload.Timeout
		if c.currentConcurrency >= c.maxConcurrency {
			c.taskStatuses[TaskID(taskPayload.ID)] = internal.Queued
			time.Sleep(time.Second)
			continue
		}
		c.taskStatuses[TaskID(taskPayload.ID)] = internal.Processing
		go c.processTask(
			c.taskHandler,
			TaskID(taskPayload.ID),
			taskArg,
			taskPayload.Timeout,
		)
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) processCommands() {
	for {
		if c.redis.LLen(c.commandQueueName()) == 0 {
			time.Sleep(time.Second)
			continue
		}
		commandData, err := c.redis.BRPop(c.commandQueueName())
		if err != nil {
			panic(err.Error())
		}
		var command internal.TaskCommand
		json.Unmarshal([]byte(commandData), &command)

		c.handleCommand(TaskID(command.TaskId), command.Command)
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) PTasks() {
	go c.processCommands()
	for {
		if len(c.processedQueued) > 0 {
			if c.currentConcurrency <= c.currentConcurrency {
				taskID := c.processedQueued[0]
				c.processedQueued = c.processedQueued[1:]
				internal.AtomicInc(&c.currentConcurrency)
				c.taskStatuses[taskID] = internal.Processing
				go c.processTask(
					c.taskHandler,
					taskID,
					c.taskArgs[taskID],
					c.taskTimeouts[taskID],
				)
			} else {
				time.Sleep(1)
				continue
			}
		}
		if c.redis.LLen(c.taskQueueName()) == 0 {
			time.Sleep(time.Second)
			continue
		}
		taskData, err := c.redis.BRPop(c.taskQueueName())
		if err != nil {
			panic(err.Error())
		}
		var taskPayload internal.TaskPayload
		var taskArg TaskArgType
		json.Unmarshal([]byte(taskData), &taskPayload)
		_, taskCancelled := c.cancelQueue[TaskID(taskPayload.ID)]
		if taskCancelled {
			delete(c.cancelQueue, TaskID(taskPayload.ID))
			continue
		}
		serializedArg, _ := json.Marshal(taskPayload.Argument)
		json.Unmarshal(serializedArg, &taskArg)
		c.taskArgs[TaskID(taskPayload.ID)] = taskArg
		c.taskTimeouts[TaskID(taskPayload.ID)] = taskPayload.Timeout
		fmt.Println(c.currentConcurrency, c.maxConcurrency)
		if c.currentConcurrency >= c.maxConcurrency {
			c.taskStatuses[TaskID(taskPayload.ID)] = internal.Queued
			c.processedQueued = append(c.processedQueued, TaskID(taskPayload.ID))
			time.Sleep(time.Second)
			continue
		}
		internal.AtomicInc(&c.currentConcurrency)
		c.taskStatuses[TaskID(taskPayload.ID)] = internal.Processing
		go c.processTask(
			c.taskHandler,
			TaskID(taskPayload.ID),
			taskArg,
			taskPayload.Timeout,
		)
	}
}
