package internal

import (
	"testing"
)

func TestAtomicInc(t *testing.T) {
	var x uint64
	x = 1
	AtomicInc(&x)
	if x != 2 {
		t.Fatal("Atomic Inc failed")
	}
}

func TestAtomicDec(t *testing.T) {
	var x uint64
	x = 1
	AtomicDec(&x)
	if x != 0 {
		t.Fatal("Atomic Dec failed")
	}
}
