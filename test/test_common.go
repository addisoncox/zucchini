package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/addisoncox/zucchini"
	"github.com/google/uuid"
)

func alwaysSucceedsTask(numbers Numbers) int {
	return 42
}

func alwaysSucceedsCallback(status zucchini.TaskStatus, res int) error {
	return nil
}

func alwaysFailsTask(numbers Numbers) int {
	time.Sleep(time.Second * 10)
	return 42
}

func alwaysFailsCallback(status zucchini.TaskStatus, res int) error {
	return nil
}

type Numbers struct {
	X int
	Y int
}

func sleepAdd(numbers Numbers) int {
	time.Sleep(time.Second * 3)
	return numbers.X + numbers.Y
}

func sleepAddCallback(status zucchini.TaskStatus, res int) error {
	fmt.Println(res)
	return nil
}

func GetStatusFromMonitor(monitorAddr string, taskID zucchini.TaskID) string {
	res, err := http.Get("http://" + monitorAddr + "/info")
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	var response []map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		panic(err)
	}
	for _, taskInfo := range response {
		if taskInfo["ID"] == uuid.UUID(taskID).String() {
			return taskInfo["Status"].(string)
		}

	}
	return "unknown"
}

func GetRetriesFromMonitor(monitorAddr string, taskID zucchini.TaskID) float64 {
	res, err := http.Get("http://" + monitorAddr + "/info")
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	var response []map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		panic(err)
	}
	for _, taskInfo := range response {
		if taskInfo["ID"] == uuid.UUID(taskID).String() {
			return taskInfo["Retries"].(float64)
		}

	}
	return -1.0
}

func GetConsumerConcurrency[T, U any](consumer zucchini.Consumer[T, U]) uint64 {
	return reflect.ValueOf(&consumer).Elem().FieldByName("currentConcurrency").Uint()
}

var SleepAddTaskDefinition = zucchini.TaskDefinition[Numbers, int]{
	TaskHandler:  sleepAdd,
	TaskCallback: sleepAddCallback,
	Timeout:      time.Second * 5,
	TaskName:     "sleepAdd",
	Options: zucchini.TaskDefinitionOptions{
		MaxRetries:    2,
		RetryStrategy: zucchini.ExponentialBackoff,
	},
}

var AlwaysSucceedsTaskDefinition = zucchini.TaskDefinition[Numbers, int]{
	TaskHandler:  alwaysSucceedsTask,
	TaskCallback: alwaysSucceedsCallback,
	Timeout:      time.Second * 5,
	TaskName:     "alwaysSucceed",
	Options: zucchini.TaskDefinitionOptions{
		MaxRetries:    2,
		RetryStrategy: zucchini.ExponentialBackoff,
	},
}

var AlwaysFailsTaskDefinition = zucchini.TaskDefinition[Numbers, int]{
	TaskHandler:  alwaysFailsTask,
	TaskCallback: alwaysFailsCallback,
	Timeout:      time.Microsecond,
	TaskName:     "alwaysFail",
	Options: zucchini.TaskDefinitionOptions{
		MaxRetries:    0,
		RetryStrategy: zucchini.ExponentialBackoff,
	},
}

var AlwaysFailsWithRetiresTaskDefinition = zucchini.TaskDefinition[Numbers, int]{
	TaskHandler:  alwaysFailsTask,
	TaskCallback: alwaysFailsCallback,
	Timeout:      time.Microsecond,
	TaskName:     "alwaysFailsRetries",
	Options: zucchini.TaskDefinitionOptions{
		MaxRetries:    2,
		RetryStrategy: zucchini.ExponentialBackoff,
		RetryDelay:    time.Second,
		RetryJitter:   time.Microsecond,
	},
}

func GenerateConsumerProducerPairForTaskDefinition[T, U any](taskDef zucchini.TaskDefinition[T, U], redis *zucchini.RedisClient) (zucchini.Consumer[T, U], zucchini.Producer[T, U]) {
	producer := zucchini.NewProducer(taskDef, redis)
	consumer := zucchini.NewConsumer(taskDef, redis, 10)
	addr := "localhost:9080"
	go consumer.StartMonitorServer(addr)
	go consumer.ProcessTasks()
	return consumer, producer
}
