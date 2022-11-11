package main

import (
	"fmt"
	"time"

	"github.com/addisoncox/zucchini/config"
	"github.com/addisoncox/zucchini/queue"
	"github.com/addisoncox/zucchini/redis"
	"github.com/addisoncox/zucchini/task"
)

func printHello() {
	fmt.Println("Hello")
}

func sleepAdd(x int, y int) int {
	time.Sleep(time.Second * 10)
	return x + y
}

func main() {

	queueConfig := config.QueueConfig{
		Name:     "test",
		Capacity: 100,
		Redis:    *redis.NewClient("localhost:6379", "", 0),
		Workers:  32,
	}
	queue := queue.NewQueue(queueConfig)

	sayHello := task.Task{Function: printHello}

	queue.EnqueueTask(sayHello)
	queue.RunNextTask()

	for i := 0; i < 3; i++ {
		addTask := task.Task{Function: sleepAdd, Arguments: []interface{}{i, 3}}
		queue.EnqueueTask(addTask)
	}

	queue.RegisterCallback(func(result task.TaskResult) {
		if result.Status == task.Succeeded {
			fmt.Println("Task worked!")
			fmt.Println(result.Value)
		}
	})

	go queue.ProcessTasks()
	queue.Listen()
}
