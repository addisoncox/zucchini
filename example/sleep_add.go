package main

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/addisoncox/zucchini"
)

type Numbers struct {
	X int
	Y int
}

func sleepAdd(numbers Numbers) int {
	time.Sleep(time.Second * 3)
	return numbers.X + numbers.Y
}

func sleepAddCallback(status zucchini.TaskStatus, res int) error {
	if status.Succeeded() {
		return nil
	} else {
		return errors.New("...")
	}

}

type MySerializer struct{}

func (m *MySerializer) Serialize(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (m *MySerializer) Deserialize(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func main() {

	mySerializer := &MySerializer{}
	sleepAddTaskDefinition := zucchini.TaskDefinition[Numbers, int]{
		TaskHandler:  sleepAdd,
		TaskCallback: sleepAddCallback,
		Timeout:      time.Second * 5,
		TaskName:     "sleepAdd",
		Options: zucchini.TaskDefinitionOptions{
			MaxRetries:       2,
			RetryStrategy:    zucchini.ExponentialBackoff,
			CustomSerializer: mySerializer,
		},
	}

	taskProducer := zucchini.NewProducer(
		sleepAddTaskDefinition,
		zucchini.NewRedisClient("localhost:6379", "", 0),
	)
	taskConsumer := zucchini.NewConsumer(
		sleepAddTaskDefinition,
		zucchini.NewRedisClient("localhost:6379", "", 0),
		10,
	)
	taskIDs := make([]zucchini.TaskID, 0)
	for i := 0; i < 20; i++ {
		taskIDs = append(taskIDs, taskProducer.QueueTask(Numbers{X: 3, Y: 4 + i}))
	}

	taskProducer.CancelTask(taskIDs[1])
	time.Sleep(time.Second)
	go taskConsumer.ProcessTasks()
	go taskConsumer.StartMonitorServer("localhost:8089")
	taskProducer.AwaitCallback()
}
