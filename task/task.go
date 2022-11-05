package task

import "encoding/json"

type TaskStatus int

const (
	Failed TaskStatus = iota
	Succeeded
)

type TaskResult struct {
	Status TaskStatus
	Value  string
}

type Task struct {
	Function  interface{}
	Arguments []interface{}
}

func (t TaskResult) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}
