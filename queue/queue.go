package queue

import (
	"time"

	"github.com/addisoncox/zucchini/config"
	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
	"github.com/addisoncox/zucchini/util"
)

type Task struct {
	task task.Task
	id   uint64
}

type Queue struct {
	name              string
	tasks             chan Task
	redis             *redis.RedisClient
	capacity          uint64
	taskCount         uint64
	goroutinesRunning uint64
	goroutineLimit    uint64
	callback          func(task.TaskResult)
	taskIDCounter     uint64
	taskRetryCounter  map[uint64]uint
	retryLimit        uint
}

func (q *Queue) EnqueueTask(task task.Task) {
	if q.taskCount < q.capacity {
		q.taskIDCounter++
		q.tasks <- Task{
			task: task,
			id:   q.taskIDCounter,
		}
		util.AtomicInc(&q.taskCount)
	} else {
		panic("Tried to enqueue more tasks than queue capacity")
	}
}

func (q *Queue) RunNextTask() {
	if q.taskCount > 0 {
		nextTask := <-q.tasks
		q.processTask(
			nextTask.task.Function,
			nextTask.task.Timeout,
			nextTask.id,
			nextTask.task.Arguments...,
		)
	}
}

func (q *Queue) handleTimeout() {
	// TODO
}

func (q *Queue) processTask(
	function interface{},
	timeout time.Duration,
	taskID uint64,
	arguments ...interface{},
) {
	result := make(chan task.TaskResult, 1)
	go func() {
		result <- util.Call(function, arguments...)
	}()

	select {
	case taskResult := <-result:
		q.redis.LPush(q.name, taskResult)
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
			q.handleTimeout()
			q.processTask(function, timeout, taskID, arguments...)
		}
		q.redis.LPush(q.name, taskResult)
	}
}

/*
	func (q *Queue) processTask(function interface{}, arguments ...interface{}) {
		taskResult := util.Call(function, arguments...)
		q.redis.LPush(q.name, taskResult)
		util.AtomicDec(&q.goroutinesRunning)
	}
*/
func (q *Queue) ProcessTasks() {
	for {
		time.Sleep(time.Second)
		for q.taskCount > 0 {
			if q.goroutinesRunning < q.goroutineLimit {
				nextTask := <-q.tasks
				q.taskCount--
				q.goroutinesRunning++
				go q.processTask(
					nextTask.task.Function,
					nextTask.task.Timeout,
					nextTask.id,
					nextTask.task.Arguments...,
				)
			} else {
				break
			}
		}
	}
}

func (q *Queue) RegisterCallback(callback func(task.TaskResult)) {
	q.callback = callback
}

func (q *Queue) Listen() {
	for {

		value, err := q.redis.BRPop(q.name)
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
		name:              cfg.Name,
		tasks:             make(chan Task, cfg.Capacity),
		redis:             &cfg.Redis,
		capacity:          cfg.Capacity,
		taskCount:         0,
		goroutinesRunning: 0,
		goroutineLimit:    cfg.GoroutineLimit,
		callback:          nil,
		taskIDCounter:     0,
		taskRetryCounter:  make(map[uint64]uint),
		retryLimit:        cfg.RetryLimit,
	}
}
