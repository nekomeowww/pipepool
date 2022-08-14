package pipepool

import (
	"sync"
)

type PipePool[E any, C comparable] struct {
	pool *Pool

	mPipesMutex sync.Mutex
	mPipes      map[C]*Pipe[E]

	categorizeByFunc func(E) C
}

func New[E any, C comparable](bufferSize int64, categorizeBy func(E) C) *PipePool[E, C] {
	return &PipePool[E, C]{
		pool:             NewPool(bufferSize),
		mPipes:           make(map[C]*Pipe[E]),
		categorizeByFunc: categorizeBy,
	}
}

func (pp *PipePool[E, C]) categorize(e E) {
	pp.mPipesMutex.Lock()
	defer pp.mPipesMutex.Unlock()

	category := pp.categorizeByFunc(e)
	pipe, ok := pp.mPipes[category]
	if !ok {
		pipe = NewPipe[E]()
		pp.mPipes[category] = pipe
	}

	pipe.push(PipeElement[E]{element: e})
}

func (pp *PipePool[E, C]) Push(e E) {
	pp.pool.Acquire()
	pp.categorize(e)
}

func (pp *PipePool[E, C]) AvailableCount() int64 {
	return pp.pool.AvailableCount()
}

type PipePoolTransaction[E any, C comparable] struct {
	Category C

	pipePool *PipePool[E, C]
	pipeTx   *PipeTransaction[E]
}

func (pp *PipePool[E, C]) Begin() *PipePoolTransaction[E, C] {
	var foundCategory C
	var foundPipe *Pipe[E]
	var foundPipeTx *PipeTransaction[E]

	for {
		pp.mPipesMutex.Lock()
		var found bool
		for category, v := range pp.mPipes {
			if v.IsBlocking() {
				continue
			}
			if v.Len() > 0 {

				foundCategory = category
				foundPipe = v
				found = true
				foundPipeTx = foundPipe.Begin()
				break
			}
		}

		pp.mPipesMutex.Unlock()
		if found {
			break
		}
	}

	return &PipePoolTransaction[E, C]{
		pipePool: pp,
		Category: foundCategory,
		pipeTx:   foundPipeTx,
	}
}

func (tx *PipePoolTransaction[E, C]) Pull() E {
	element := tx.pipeTx.Pull()
	return element
}

func (tx *PipePoolTransaction[E, C]) Rollback() {
	tx.pipePool.mPipesMutex.Lock()
	defer tx.pipePool.mPipesMutex.Unlock()

	tx.pipeTx.Rollback()
}

func (tx *PipePoolTransaction[E, C]) Commit() {
	tx.pipeTx.Commit()
	tx.pipePool.pool.Release()

	if tx.pipeTx.pipe.Len() == 0 {
		tx.pipePool.mPipesMutex.Lock()
		defer tx.pipePool.mPipesMutex.Unlock()
		delete(tx.pipePool.mPipes, tx.Category)
	}
}
