package pipepool

import (
	"sync"
)

// PipePool 管道池
type PipePool[E any, C comparable] struct {
	pool *Pool // stacking pool 堆积池

	mPipesMutex sync.Mutex     // mutex lock for map of pipes管道池并发锁
	mPipes      map[C]*Pipe[E] // map of pipes 管道池

	categorizeByFunc func(E) C // categorize by function 分类函数
}

// New create new pipe pool 创建管道池
//
// Generic type E represents the type of the elements in the pipe.
// Generic type C represents the type of the categories of the pipe.
//
// Pass in bufferSize to control the size of the pool, and pass in categorizeByFunc to control the categorization of the pipe.
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

// Push push element to the pipe pool 向管道池推入元素
func (pp *PipePool[E, C]) Push(e E) {
	pp.pool.Acquire()
	pp.categorize(e)
}

// Categories returns the categories of the pipe pool 管道池的分类
func (pp *PipePool[E, C]) Categories() []C {
	pp.mPipesMutex.Lock()
	defer pp.mPipesMutex.Unlock()

	categories := make([]C, 0, len(pp.mPipes))
	for category := range pp.mPipes {
		categories = append(categories, category)
	}

	return categories
}

// PipeQueuedCount returns the number of elements in the pipe pool 管道池中的元素数量
func (pp *PipePool[E, C]) PipeQueuedCount(category C) int {
	pp.mPipesMutex.Lock()
	defer pp.mPipesMutex.Unlock()

	pipe, ok := pp.mPipes[category]
	if !ok {
		return 0
	}

	return pipe.Len()
}

// Total count of slots of pool 池完整大小
func (pp *PipePool[E, C]) TotalCount() int64 {
	return pp.pool.TotalCount
}

// Available slots of pool 剩余池大小
func (pp *PipePool[E, C]) AvailableCount() int64 {
	return pp.pool.AvailableCount()
}

// PipePoolTransaction transaction of pipe pool 管道池操作事务
type PipePoolTransaction[E any, C comparable] struct {
	Category C // category of pipe 管道分类

	pipePool *PipePool[E, C]     // pipe pool 管道池实例
	pipeTx   *PipeTransaction[E] // pipe transaction 管道操作事务
}

// Begin begin transaction 开始事务
// returns a transaction object that can be used to pull, commit or rollback the transaction
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

// Pull pull element from transaction 取出事务中的元素
func (tx *PipePoolTransaction[E, C]) Pull() E {
	element := tx.pipeTx.Pull()
	return element
}

// Rollback rollback transaction 回滚事务
func (tx *PipePoolTransaction[E, C]) Rollback() {
	tx.pipePool.mPipesMutex.Lock()
	defer tx.pipePool.mPipesMutex.Unlock()

	tx.pipeTx.Rollback()
}

// Commit commit transaction 提交事务
func (tx *PipePoolTransaction[E, C]) Commit() {
	tx.pipeTx.Commit()
	tx.pipePool.pool.Release()

	if tx.pipeTx.pipe.Len() == 0 {
		tx.pipePool.mPipesMutex.Lock()
		defer tx.pipePool.mPipesMutex.Unlock()
		delete(tx.pipePool.mPipes, tx.Category)
	}
}
