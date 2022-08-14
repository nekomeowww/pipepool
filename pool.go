package pipepool

import "sync/atomic"

type Pool struct {
	TotalCount     int64
	availableCount int64
}

func NewPool(total int64) *Pool {
	return &Pool{
		TotalCount:     total,
		availableCount: total,
	}
}

func (p *Pool) Acquire() bool {
	if atomic.LoadInt64(&p.availableCount) <= 0 {
		return false
	}

	atomic.AddInt64(&p.availableCount, -1)
	return true
}

func (p *Pool) AvailableCount() int64 {
	return atomic.LoadInt64(&p.availableCount)
}

func (p *Pool) Release() {
	atomic.AddInt64(&p.availableCount, 1)
}
