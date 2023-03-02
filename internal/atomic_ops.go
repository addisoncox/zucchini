package internal

import (
	"sync"
	"sync/atomic"
)

func AtomicInc(addr *uint64) {
	atomic.AddUint64(addr, 1)
}

func AtomicDec(addr *uint64) {
	atomic.AddUint64(addr, ^uint64(0))
}

func AtomicMapEdit[T comparable, U any](key T, value U, map_ map[T]U) {
	m := sync.Mutex{}
	m.Lock()
	map_[key] = value
	m.Unlock()
}
