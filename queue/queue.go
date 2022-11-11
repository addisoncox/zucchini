package queue

import (
	"time"

	"github.com/addisoncox/zucchini/config"
	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
	"github.com/addisoncox/zucchini/util"
)

type Queue struct {
	name       string
	tasks      chan task.Task
	redis      *redis.RedisClient
	capacity   uint64
	taskCount  uint64
	goroutines uint32
	workers    uint32
	callback   func(task.TaskResult)
}

func (q *Queue) EnqueueTask(task task.Task) {
	if q.taskCount < q.capacity {
		q.tasks <- task
		q.taskCount++
	} else {
		panic("Tried to enqueue more tasks than queue capacity")
	}
}

func (q *Queue) RunNextTask() {
	if q.taskCount > 0 {
		nextTask := <-q.tasks
		taskResult := util.Call(nextTask.Function, nextTask.Arguments...)
		q.taskCount--
		q.redis.LPush(q.name, taskResult)
	}
}
func (q *Queue) processTask(function interface{}, arguments ...interface{}) {
	taskResult := util.Call(function, arguments...)
	q.redis.LPush(q.name, taskResult)
	q.goroutines--
}

func (q *Queue) ProcessTasks() {
	for {
		time.Sleep(time.Second)
		for q.taskCount > 0 {
			if q.goroutines < q.workers {
				nextTask := <-q.tasks
				q.taskCount--
				q.goroutines++
				go q.processTask(nextTask.Function, nextTask.Arguments...)
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
		name:       cfg.Name,
		tasks:      make(chan task.Task, cfg.Capacity),
		redis:      &cfg.Redis,
		capacity:   cfg.Capacity,
		taskCount:  0,
		goroutines: 0,
		workers:    cfg.Workers,
		callback:   nil,
	}
}
