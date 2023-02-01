package task

import "testing"

func TestCreateTask(t *testing.T) {
	task := Task{}
	if len(task.Arguments) > 0 {
		t.Fatal("Expected tasks arguments length should be 0s")
	}
}
