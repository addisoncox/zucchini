package queue

import (
	"time"

	"github.com/addisoncox/zucchini/config"
	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
	"github.com/addisoncox/zucchini/util"
)

type Queue struct {
	name      string
	tasks     chan task.Task
	redis     *redis.RedisClient
	capacity  uint64
	taskCount uint64
	callback  func(task.TaskResult)
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

func (q *Queue) ProcessTasks() {
	for q.taskCount > 0 {
		nextTask := <-q.tasks
		taskResult := util.Call(nextTask.Function, nextTask.Arguments...)
		q.taskCount--
		q.redis.LPush(q.name, taskResult)
	}
}

func (q *Queue) RegisterCallback(callback func(task.TaskResult)) {
	q.callback = callback
}

func (q *Queue) Listen() {
	for {
		time.Sleep(time.Second)
		//fmt.Println(q.redis.LLen(q.name))
		for q.redis.LLen(q.name) > 0 {
			value, err := q.redis.RPop(q.name)
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
}

func NewQueue(cfg config.QueueConfig) Queue {
	return Queue{
		cfg.Name,
		make(chan task.Task, cfg.Capacity),
		&cfg.Redis,
		cfg.Capacity,
		0,
		nil,
	}
}
