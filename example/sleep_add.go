package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/addisoncox/zucchini/config"
	"github.com/addisoncox/zucchini/queue"
	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
)

type Numbers struct {
	X int
	Y int
}

func sleepAdd(data []byte) interface{} {
	var numbers Numbers
	json.Unmarshal(data, &numbers)
	time.Sleep(time.Second * 3)
	return numbers.X + numbers.Y
}

func main() {
	testTask := task.Task{
		Name:    "test",
		Data:    Numbers{X: 3, Y: 4},
		Timeout: time.Second * 5,
	}
	queueConfig := config.QueueConfig{
		Name:           "TestQueue",
		Capacity:       10,
		Redis:          *redis.NewClient("localhost:6379", "", 0),
		GoroutineLimit: 32,
	}
	testQueue := queue.NewQueue(queueConfig)

	processQueueConfig := config.QueueConfig{
		Name:           "TestQueue",
		Capacity:       10,
		Redis:          *redis.NewClient("localhost:6379", "", 0),
		GoroutineLimit: 32,
	}

	processQueue := queue.NewQueue(processQueueConfig)
	testQueue.RegisterCallback(func(res task.TaskResult) {
		fmt.Println("CALLBACK RUNNING")
		fmt.Println(res.Value)
	})
	testQueue.EnqueueTask(testTask)
	go processQueue.ProcessTasks(sleepAdd)
	testQueue.Listen()
}
