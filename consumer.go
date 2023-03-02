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
	cancelQueue         map[TaskID]bool
	processedQueued     []TaskID
	pausePollTime       time.Duration
	queuedTaskIDs       chan TaskID
	taskIDs             []TaskID
	monitorAddr         string
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
		cancelQueue:         make(map[TaskID]bool),
		processedQueued:     make([]TaskID, 0),
		pausePollTime:       time.Second,
		// TODO: Wrapper for unbounded?
		queuedTaskIDs: make(chan TaskID, 1000),
		taskIDs:       make([]TaskID, 0),
		monitorAddr:   "",
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

func (c *Consumer[TaskArgType, TaskResultType]) monitor(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, internal.HTMLForZucchiniMonitor(c.taskName, c.monitorAddr))
	return
}

func (c *Consumer[TaskArgType, TaskResultType]) pausePoll() {
	time.Sleep(c.pausePollTime)
}

func (c *Consumer[TaskArgType, TaskResultType]) saveTask(taskID TaskID, task internal.Task[TaskArgType]) {
	taskBytes, _ := json.Marshal(task)
	c.redis.Set(uuid.UUID(taskID).String(), taskBytes)
}

func (c *Consumer[TaskArgType, TaskResultType]) queueTask(taskID TaskID) {
	c.queuedTaskIDs <- taskID
}

func (c *Consumer[TaskArgType, TaskResultType]) getTask(taskID TaskID) internal.Task[TaskArgType] {
	res := c.redis.Get(uuid.UUID(taskID).String())
	return internal.UnmarshalOrPanic[internal.Task[TaskArgType]]([]byte(res))
}

func (c *Consumer[TaskArgType, TaskResultType]) taskCancelled(taskID TaskID) bool {
	_, taskCancelled := c.cancelQueue[taskID]
	return taskCancelled
}

func (c *Consumer[TaskArgType, TaskResultType]) taskInfo(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tasksData := make([]taskData[TaskArgType], 0)
	for _, id := range c.taskIDs {
		task := c.getTask(id)
		tasksData = append(tasksData, taskData[TaskArgType]{
			ID:      uuid.UUID(id).String(),
			Arg:     task.Payload.Argument,
			Status:  TaskStatus{Status: task.Status}.String(),
			Retries: task.Retries,
		})
	}
	json.NewEncoder(w).Encode(tasksData)
}

func (c *Consumer[TaskArgType, TaskResultType]) StartMonitorServer(addr string) {
	c.monitorAddr = addr
	http.HandleFunc("/", c.monitor)
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

	task := c.getTask(taskID)
	select {
	case resultValue := <-result:
		serializedResultValue, _ := json.Marshal(resultValue)
		serializedTaskResult, _ := json.Marshal(TaskResult{
			Status: TaskStatus{Status: internal.Succeeded},
			Value:  serializedResultValue,
		})
		c.redis.LPush(c.resultQueueName(), serializedTaskResult)
		task.Status = internal.Succeeded
		c.saveTask(taskID, task)
		internal.AtomicDec(&c.currentConcurrency)
	case <-time.After(timeout):
		if task.Retries > c.maxRetries {
			taskResult := TaskResult{
				Status: TaskStatus{Status: internal.Failed},
				Value:  []byte{},
			}
			task.Status = internal.Failed
			c.saveTask(taskID, task)
			c.redis.LPush(c.resultQueueName(), taskResult)
			internal.AtomicDec(&c.currentConcurrency)
		} else {
			task.Retries++
			c.saveTask(taskID, task)
			c.handleTimeout(task.Retries)
			c.processTask(handlerFunc, taskID, arg, timeout)
		}
	}
}

func (c *Consumer[TaskArgType, TaskResultType]) cancelTask(taskID TaskID) {
	c.cancelQueue[taskID] = true
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
			var taskPayload internal.TaskPayload[TaskArgType]
			json.Unmarshal([]byte(taskData), &taskPayload)
			task := internal.Task[TaskArgType]{
				Payload: taskPayload,
				Status:  internal.Queued,
				Retries: 0,
			}
			c.taskIDs = append(c.taskIDs, TaskID(taskPayload.ID))
			if c.taskCancelled(TaskID(taskPayload.ID)) {
				task.Status = internal.Cancelled
				c.saveTask(TaskID(taskPayload.ID), task)
				continue
			}
			c.saveTask(TaskID(taskPayload.ID), task)
			c.queueTask(TaskID(taskPayload.ID))
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
		task := c.getTask(nextTaskID)
		if c.taskCancelled(nextTaskID) {
			task.Status = internal.Cancelled
			c.saveTask(nextTaskID, task)
			continue
		}
		internal.AtomicInc(&c.currentConcurrency)
		task.Status = internal.Processing
		c.saveTask(nextTaskID, task)
		go c.processTask(
			c.taskHandler,
			nextTaskID,
			task.Payload.Argument,
			task.Payload.Timeout,
		)
	}
}
