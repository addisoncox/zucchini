package queue

import "testing"

func TestCreateQueue(t *testing.T) {
	queue := Queue{
		name:     "test queue",
		capacity: 10,
	}
	if queue.taskCount != 0 {
		t.Fatal("Expected Queue task count to be 0")
	} else if queue.goroutinesRunning != 0 {
		t.Fatal("Expected goroutine count to be 0")
	}
}
