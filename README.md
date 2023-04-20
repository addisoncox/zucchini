# Zucchini

Zucchini is a lightweight task queue for Go, built on top of Redis.

# Basic Usage

To get started using Zucchini, you need to create a TaskDefinition, a Consumer, and a Producer. 

The TaskDefinition works by wrapping a function to execute your task and a callback function to handle the result over your task. 

The TaskDefinition is generic over the input and output types to your task. A basic example is as follows:

```go
type MyTaskArguments struct {
	X int
	Y int
}

func myTaskHandler(arguments MyTaskArguments) int {
	return arguments.X + arguments.Y
}

func myTaskCallback(status zucchini.TaskStatus, res int) error {
	if status.Succeeded() {
		handle(res)
		return nil
	} else {
	    errors.New("...")
	}
}

taskDefinition := zucchini.TaskDefinition[MyTaskArguments, int]{
		TaskHandler:  myTaskHandler,
		TaskCallback: myTaskCallback,
		Timeout:      time.Second * 5,
		TaskName:     "myTaskName",
		Options: zucchini.TaskDefinitionOptions{
			MaxRetries:       2,
			RetryStrategy:    zucchini.ExponentialBackoff,
		},
	}
```

A producer is responsible for queueing the tasks. To create a producer, you pass it a Redis client and a task defintion. (For pass and db options see [redis-go](https://pkg.go.dev/github.com/redis/go-redis/v9#readme-quickstart) documentation).

<b>Note: The Redis client passed to the consumer and producer must be the same!</b>

```go
taskProducer := zucchini.NewProducer(
		taskDefinition,
		zucchini.NewRedisClient("localhost:6379", pass, db),
	)
```

To queue a task:
```go
var arg MyArgType
arg = ...
taskProducer.QueueTask(arg)
```

To listen for callbacks run:
```go
taskProducer.AwaitCallback()
```
<b>Note: You must call this functions if you want callbacks to run!</b>


A consumer should run where you want to process those tasks using the same Redis client. The last argument passed to the consumer is the max number of goroutines to run concurrently. 
```go
taskConsumer := zucchini.NewConsumer(
		sleepAddTaskDefinition,
		zucchini.NewRedisClient("localhost:6379", pass, db),
		10,
	)
```
To start processing tasks with the consumer run.
```go
taskConsumer.ProcessTasks()
```

# Other Options
## Task Defintion Options
Task defintions accepts a variety of optional arguments
<table>
<th>
Option
</th>
<th>
Type
</th>
<th>
Explanation
</th>
<tbody>
<tr>
<td>MaxRetries</td>
<td>uint</td>
<td>Maximum retries before task fails</td>
</tr>
<tr>
<td>RetryStrategy</td>
<td>zucchini.RetryStrategy</td>
<td>Determines how to caluclate the wait before retrying after failure. One of ExponentialBackoff, SetDelay, or Custom (default is ExponentialBackoff).</td>
</tr>
<tr>
<td>RetryJitter</td>
<td>time.Duration</td>
<td>Base jitter delay between retries.</td>
</tr>
<tr>
<td>RetryDelay</td>
<td>time.Duration</td>
<td>Used to calculate the base time to wait for retry.</td>
</tr>
<tr>
<td>CustomRetryFunction</td>
<td>func(uint) time.Duration</td>
<td>Function of the retry number which returns a time.Duration to wait before next retry. Only if RetryStrategy is set as Custom.</td>
</tr>
<tr>
<td>CustomSerializer</td>
<td>zucchini.CustomSerializer</td>
<td>Serializer to use for task data. See below for an example.</td>
</tr>
</tbody>
</table>

## Retry Delays
Exponential Backoff is calculated as follows
```
(retryDelay * (2 ** retryCount)) + (baseJitter * (rand_between(0, 1) * 2))

```
An example CustomRetryFunction is as follows
```go
func customRetry(retryNumber uint) time.Duration {
	if retryNumber < 5 {
		return time.Second
	} else {
		return time.Second * time.Duration(math.Pow(2, float64(retryNumber)))
	}
}
```
## Task Cancellation 
If you need to cancel tasks, keep track of the `Zucchini.TaskID` that `queueTask` returns, and pass it to `cancelTask`.
```go
taskIDs := make([]zucchini.TaskID, 0)
for i := 0; i < 20; i++ {
	taskIDs = append(taskIDs, taskProducer.QueueTask(Numbers{X: 3, Y: 4 + i}))
}
taskProducer.CancelTask(taskIDs[1])
```

## Checking Status in Callback
Each callback function must have the signature `(zucchini.TaskStatus, res ResultType) -> error`.
To check if a task status matches a certain state, Zucchini provides the helper functions `Failed()`, `Succeeded()`,and `Cancelled()`. You can also get the status as a string (lowercase) by calling `String()` on a `Zucchini.TaskStatus`.

## Using a Custom Serializer
To use a custom serializer for task data you must implement the `zucchini.CustomSerializer` interface
```go
type CustomSerializer interface {
	Serialize(v any) ([]byte, error)
	Deserialize(data []byte, v any) error
}
```
Here's an example:
```go
type MySerializer struct{}

func (m *MySerializer) Serialize(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (m *MySerializer) Deserialize(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

mySerializer := &MySerializer{}
	taskDefintion := zucchini.TaskDefinition[..., ...]{
		...
		Options: zucchini.TaskDefinitionOptions{
			CustomSerializer: mySerializer,
		},
	}
```

## Running a Task Consumer Monitor
To set up a web UI to monitor the task consumer run:
```go
addr := "localhost:9801"
go taskConsumer.StartMonitorServer(addr)
```
## Interpreting the Task State
<table>
<th> Status </th>
<th> Status Meaning </th>
<tbody>
<tr>
<td>Queued</td>
<td>The consumer has registered the task.</td>
</tr>
<tr>
<td>Processing</td>
<td>The consumer is executing the task function, which has not yet completed.</td>
</tr>
<tr>
<td>Succeeded</td>
<td>The task finished successfully.</td>
</tr>
<tr>
<td>Failed</td>
<td>The task failed after all retries.</td>
</tr>
<tr>
<td>Cancelled.</td>
<td>The task was cancelled.</td>
</tr>
</tbody>
</table>