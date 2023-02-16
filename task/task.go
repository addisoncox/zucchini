package task

import (
	"encoding/json"
	"time"
)

type TaskStatus int

const (
	Failed TaskStatus = iota
	Succeeded
	Timeout
)

type TaskResult struct {
	Status TaskStatus `json:"-"`
	Value  string     `json:"value"`
}

type Task struct {
	Function  interface{}
	Arguments []interface{}
	Timeout   time.Duration
}

func (t TaskResult) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t TaskResult) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, &t.Value)
}

func (t TaskResult) Succeeded() bool {
	return t.Status == Succeeded
}
