package queue

import (
	"encoding/json"
	"math"
	"math/rand"
	"time"

	"github.com/addisoncox/zucchini/config"
	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
	"github.com/addisoncox/zucchini/util"
	"github.com/google/uuid"
)

const ZUCCHINI_TASK_PREFIX = "zhc:task:"
const ZUCCHINI_RES_PREFIX = "zhc:res:"

type TaskID uuid.UUID

type Task struct {
	TaskID  TaskID
	Data    interface{}
	Timeout time.Duration
}

type Queue struct {
	name                string
	redis               *redis.RedisClient
	capacity            uint64
	taskCount           uint64
	goroutinesRunning   uint64
	goroutineLimit      uint64
	callback            func(task.TaskResult)
	retryStrategy       config.RetryStrategy
	delay               time.Duration
	baseJitter          time.Duration
	taskRetryCounter    map[TaskID]uint
	retryLimit          uint
	customRetryFunction func(uint) time.Duration
	handlerFunc         func([]byte) interface{}
}

func (q *Queue) EnqueueTask(task task.Task) TaskID {
	if q.taskCount < q.capacity {
		taskID := TaskID(uuid.New())
		taskBytes, err := json.Marshal(
			Task{
				TaskID:  taskID,
				Data:    task.Data,
				Timeout: task.Timeout,
			})
		if err != nil {
			panic("Could not marshall task data.")
		}
		q.taskRetryCounter[taskID] = 0
		q.redis.LPush(ZUCCHINI_TASK_PREFIX+q.name, taskBytes)
		if taskBytes == nil {
		}
		util.AtomicInc(&q.taskCount)
		return taskID
	} else {
		panic("Tried to enqueue more tasks than queue capacity")
	}
}

func (q *Queue) handleTimeout(retryCount uint) {
	if q.retryStrategy == config.ExponentialBackoff {
		backoffTime := q.delay * time.Duration(math.Pow(2, float64(retryCount)))
		rand.Seed(time.Now().UnixNano())
		jitter := q.baseJitter * time.Duration(rand.Float64()*2)
		time.Sleep(backoffTime + jitter)
	} else if q.retryStrategy == config.SetDelay {
		rand.Seed(time.Now().UnixNano())
		jitter := q.baseJitter * time.Duration(rand.Float64()*2)
		time.Sleep(q.delay + jitter)
	} else if q.retryStrategy == config.Custom {
		time.Sleep(q.customRetryFunction(retryCount))
	}
}

func (q *Queue) processTask(
	handlerFunc func([]byte) interface{},
	taskID TaskID,
	taskData []byte,
	timeout time.Duration,
) {
	result := make(chan interface{}, 1)
	go func() {
		result <- handlerFunc(taskData)
	}()

	select {
	case resultValue := <-result:
		serializedResult, _ := json.Marshal(resultValue)
		serializedResultString := string(serializedResult)
		taskResult := task.TaskResult{
			Status: task.Succeeded,
			Value:  serializedResultString,
		}
		q.redis.LPush(ZUCCHINI_RES_PREFIX+q.name, taskResult)
		util.AtomicDec(&q.goroutinesRunning)
	case <-time.After(timeout):
		var taskResult task.TaskResult
		if q.taskRetryCounter[taskID] > q.retryLimit {
			taskResult = task.TaskResult{
				Status: task.Failed,
				Value:  "",
			}
			q.taskRetryCounter[taskID] = 0
			util.AtomicDec(&q.goroutinesRunning)
		} else {
			taskResult = task.TaskResult{
				Status: task.Timeout,
				Value:  "",
			}
			q.taskRetryCounter[taskID]++
			q.handleTimeout(q.taskRetryCounter[taskID])
			q.processTask(handlerFunc, taskID, taskData, timeout)
		}
		q.redis.LPush(ZUCCHINI_RES_PREFIX+q.name, taskResult)
	}
}

/*
	func (q *Queue) processTask(function interface{}, arguments ...interface{}) {
		taskResult := util.Call(function, arguments...)
		q.redis.LPush(q.name, taskResult)
		util.AtomicDec(&q.goroutinesRunning)
	}
*/
func (q *Queue) ProcessTasks(handlerFunc func([]byte) interface{}) {
	q.handlerFunc = handlerFunc
	for {
		if q.goroutinesRunning > q.goroutineLimit {
			time.Sleep(time.Second)
			continue
		}
		if q.redis.LLen(ZUCCHINI_TASK_PREFIX+q.name) == 0 {
			time.Sleep(time.Second)
			continue
		}
		taskData, err := q.redis.BRPop(ZUCCHINI_TASK_PREFIX + q.name)
		if err != nil {
			panic(err.Error())
		}
		var task Task
		json.Unmarshal([]byte(taskData), &task)
		serializedTaskData, _ := json.Marshal(task.Data)
		util.AtomicInc(&q.goroutinesRunning)
		go q.processTask(q.handlerFunc, task.TaskID, serializedTaskData, task.Timeout)
	}
}

func (q *Queue) RegisterCallback(callback func(task.TaskResult)) {
	q.callback = callback
}

func (q *Queue) Listen() {
	for {
		if q.redis.LLen(ZUCCHINI_RES_PREFIX+q.name) == 0 {
			time.Sleep(time.Second)
			continue
		}
		value, err := q.redis.BRPop(ZUCCHINI_RES_PREFIX + q.name)
		if q.callback == nil {
			return
		}
		if err != nil {
			q.callback(task.TaskResult{
				Status: task.Failed,
				Value:  "",
			})
		} else {
			q.callback(task.TaskResult{
				Status: task.Succeeded,
				Value:  value,
			})
		}
	}
}

func NewQueue(cfg config.QueueConfig) Queue {
	return Queue{
		name:                cfg.Name,
		redis:               &cfg.Redis,
		capacity:            cfg.Capacity,
		taskCount:           0,
		goroutinesRunning:   0,
		goroutineLimit:      cfg.GoroutineLimit,
		callback:            nil,
		retryStrategy:       cfg.RetryStrategy,
		delay:               cfg.Delay,
		baseJitter:          cfg.BaseJitter,
		taskRetryCounter:    make(map[TaskID]uint),
		retryLimit:          cfg.RetryLimit,
		customRetryFunction: cfg.CustomRetryFunction,
		handlerFunc:         nil,
	}
}
