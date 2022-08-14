package pipepool

import (
	"sync"
	"time"

	"github.com/samber/lo"
)

type PipeElement[E any] struct {
	element E
}

type Pipe[E any] struct {
	elemMutex sync.Mutex
	elems     []PipeElement[E]

	blockingMutex sync.Mutex
	blocking      bool
}

func NewPipe[E any]() *Pipe[E] {
	return &Pipe[E]{}
}

func (p *Pipe[E]) push(e PipeElement[E]) {
	p.elemMutex.Lock()
	defer p.elemMutex.Unlock()
	p.elems = append(p.elems, e)
}

func (p *Pipe[E]) Len() int {
	p.elemMutex.Lock()
	defer p.elemMutex.Unlock()
	return len(p.elems)
}

type PipeTransaction[E any] struct {
	pipe    *Pipe[E]
	element *PipeElement[E]
}

func (p *Pipe[E]) Begin() *PipeTransaction[E] {
	if p.IsBlocking() {
		for {
			time.Sleep(time.Millisecond * 250)
			if !p.IsBlocking() {
				break
			}
		}
	}

	p.blockingMutex.Lock()
	defer p.blockingMutex.Unlock()
	p.blocking = true

	return &PipeTransaction[E]{
		pipe:    p,
		element: nil,
	}
}

func (tx *PipeTransaction[E]) Pull() E {
	tx.pipe.elemMutex.Lock()
	defer tx.pipe.elemMutex.Unlock()
	e := tx.pipe.elems[0]
	tx.pipe.elems = lo.Drop(tx.pipe.elems, 1)
	tx.element = &e
	return e.element
}

func (tx *PipeTransaction[E]) Rollback() {
	tx.pipe.elemMutex.Lock()
	defer tx.pipe.elemMutex.Unlock()

	tx.pipe.blockingMutex.Lock()
	defer tx.pipe.blockingMutex.Unlock()

	tx.pipe.elems = append([]PipeElement[E]{*tx.element}, tx.pipe.elems...)
	tx.pipe.blocking = false
}

func (tx *PipeTransaction[E]) Commit() {
	tx.pipe.blockingMutex.Lock()
	defer tx.pipe.blockingMutex.Unlock()

	tx.pipe.blocking = false
}

func (p *Pipe[E]) IsBlocking() bool {
	p.blockingMutex.Lock()
	defer p.blockingMutex.Unlock()
	return p.blocking
}
