package zucchini

import (
	"encoding/json"
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
	pausePollTime       time.Duration
	queuedTaskIDs       chan TaskID
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
		pausePollTime:       time.Second,
		// TODO: Wrapper for unbounded?
		queuedTaskIDs: make(chan TaskID, 1000),
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

func (c *Consumer[TaskArgType, TaskResultType]) pausePoll() {
	time.Sleep(c.pausePollTime)
}

func (c *Consumer[TaskArgType, TaskResultType]) saveTask(taskID TaskID, payload string) {
	c.redis.Set(uuid.UUID(taskID).String(), payload)
	c.queuedTaskIDs <- taskID
}

func (c *Consumer[TaskArgType, TaskResultType]) getTask(taskID TaskID) internal.TaskPayload {
	res := c.redis.Get(uuid.UUID(taskID).String())
	return internal.UnmarshalOrPanic[internal.TaskPayload]([]byte(res))
}

func (c *Consumer[TaskArgType, TaskResultType]) taskCancelled(taskID TaskID) bool {
	_, taskCancelled := c.cancelQueue[taskID]
	return taskCancelled
}

func (c *Consumer[TaskArgType, TaskResultType]) getTaskArg(taskPayload internal.TaskPayload) TaskArgType {
	var taskArg TaskArgType
	serializedArg, _ := json.Marshal(taskPayload.Argument)
	json.Unmarshal(serializedArg, &taskArg)
	return taskArg
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

func (c *Consumer[TaskArgType, TaskResultType]) queueTasks() {
	for {
		for c.redis.LLen(c.taskQueueName()) > 0 {
			taskData, err := c.redis.RPop(c.taskQueueName())
			if err != nil {
				panic(err.Error())
			}
			var taskPayload internal.TaskPayload
			json.Unmarshal([]byte(taskData), &taskPayload)
			c.saveTask(TaskID(taskPayload.ID), taskData)
			c.taskStatuses[TaskID(taskPayload.ID)] = internal.Queued
		}
		c.pausePoll()
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) ProcessTasks() {
	go c.processCommands()
	go c.queueTasks()
	for {
		if c.currentConcurrency >= c.maxConcurrency {
			c.pausePoll()
			continue
		}
		nextTaskID := <-c.queuedTaskIDs
		if c.taskCancelled(nextTaskID) {
			continue
		}
		taskPayload := c.getTask(nextTaskID)
		internal.AtomicInc(&c.currentConcurrency)
		c.taskStatuses[nextTaskID] = internal.Processing
		go c.processTask(
			c.taskHandler,
			nextTaskID,
			c.getTaskArg(taskPayload),
			taskPayload.Timeout,
		)
	}
}
