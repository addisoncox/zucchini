package main

import (
	"fmt"
	"time"

	"github.com/addisoncox/zucchini/consumer"
	"github.com/addisoncox/zucchini/producer"
	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
)

type Numbers struct {
	X int
	Y int
}

func sleepAdd(numbers Numbers) int {
	time.Sleep(time.Second * 3)
	return numbers.X + numbers.Y
}

func sleepAddCallback(status task.TaskStatus, res int) error {
	fmt.Println("CALLBACK RUNNING")
	fmt.Println(res)
	return nil
}

func main() {
	sleepAddTaskDefinition := task.TaskDefinition[Numbers, int]{
		TaskHandler:   sleepAdd,
		TaskCallback:  sleepAddCallback,
		Timeout:       time.Second * 5,
		TaskName:      "sleepAdd",
		MaxRetries:    2,
		RetryStrategy: task.ExponentialBackoff,
	}

	taskProducer := producer.NewProducer(
		sleepAddTaskDefinition,
		redis.NewClient("localhost:6379", "", 0),
		10,
	)
	taskConsumer := consumer.NewConsumer(
		sleepAddTaskDefinition,
		redis.NewClient("localhost:6379", "", 0),
		10,
	)

	taskProducer.QueueTask(Numbers{X: 3, Y: 4})
	go taskConsumer.ProcessTasks()
	taskProducer.AwaitCallback()
}
