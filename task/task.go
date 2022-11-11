package task

import "encoding/json"

type TaskStatus int

const (
	Failed TaskStatus = iota
	Succeeded
)

type TaskResult struct {
	Status TaskStatus `json:"-"`
	Value  string     `json:"value"`
}

type Task struct {
	Function  interface{}
	Arguments []interface{}
}

func (t TaskResult) MarshalBinary() ([]byte, error) {
	return json.Marshal(t.Value)
}

func (t TaskResult) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, &t.Value)
}
