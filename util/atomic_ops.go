package util

import "sync/atomic"

func AtomicInc(addr *uint64) {
	atomic.AddUint64(addr, 1)
}

func AtomicDec(addr *uint64) {
	atomic.AddUint64(addr, ^uint64(0))
}
