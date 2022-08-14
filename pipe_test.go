package pipepool

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewPipe(t *testing.T) {
	assert := assert.New(t)

	pipe := NewPipe[int64]()
	pe := PipeElement[int64]{element: 1}
	pipe.push(pe)

	assert.Equal(int64(1), pipe.elems[0].element)
}

func TestPipeTransaction(t *testing.T) {
	assert := assert.New(t)

	pipe := NewPipe[int64]()

	pipe.push(PipeElement[int64]{element: 1})
	pipe.push(PipeElement[int64]{element: 2})
	pipe.push(PipeElement[int64]{element: 3})
	pipe.push(PipeElement[int64]{element: 4})
	pipe.push(PipeElement[int64]{element: 5})

	var wg sync.WaitGroup

	var processMutex sync.Mutex
	processed := make([]int64, 0, 5)

	wg.Add(1)
	go func() {
		tx := pipe.Begin()
		val := tx.Pull()
		assert.NotZero(val)
		time.Sleep(time.Second * 1)
		tx.Commit()

		processMutex.Lock()
		processed = append(processed, val)
		processMutex.Unlock()

		wg.Done()
	}()

	time.Sleep(time.Millisecond * 500)

	wg.Add(1)
	go func() {
		tx := pipe.Begin()
		val := tx.Pull()
		assert.NotZero(val)
		time.Sleep(time.Second * 1)
		tx.Commit()

		processMutex.Lock()
		processed = append(processed, val)
		processMutex.Unlock()

		wg.Done()
	}()

	wg.Add(1)
	go func() {
		tx := pipe.Begin()
		val := tx.Pull()
		assert.NotZero(val)
		time.Sleep(time.Second * 1)
		tx.Rollback()

		wg.Done()
	}()

	wg.Wait()

	assert.Equal(3, pipe.Len())
	assert.ElementsMatch([]int64{1, 2}, processed)
}

func TestPipeTransactionWithStruct(t *testing.T) {
	t.Run("Commit", func(t *testing.T) {
		assert := assert.New(t)

		type element struct {
			category string
			value    int64
		}

		pipe := NewPipe[element]()

		pipe.push(PipeElement[element]{element: element{"a", 1}})
		pipe.push(PipeElement[element]{element: element{"a", 2}})
		pipe.push(PipeElement[element]{element: element{"a", 3}})
		pipe.push(PipeElement[element]{element: element{"a", 4}})
		pipe.push(PipeElement[element]{element: element{"a", 5}})

		assert.Equal(5, pipe.Len())
		assert.Equal(int64(1), pipe.elems[0].element.value)
		assert.Equal(int64(2), pipe.elems[1].element.value)
		assert.Equal(int64(3), pipe.elems[2].element.value)
		assert.Equal(int64(4), pipe.elems[3].element.value)
		assert.Equal(int64(5), pipe.elems[4].element.value)

		var wg sync.WaitGroup

		var processMutex sync.Mutex
		processed := make([]element, 0, 5)

		wg.Add(1)
		go func() {
			tx := pipe.Begin()
			val := tx.Pull()
			assert.NotZero(val.value)
			time.Sleep(time.Second * 1)
			tx.Commit()

			processMutex.Lock()
			processed = append(processed, val)
			processMutex.Unlock()

			wg.Done()
		}()

		wg.Add(1)
		go func() {
			tx := pipe.Begin()
			val := tx.Pull()
			assert.NotZero(val.value)
			time.Sleep(time.Second * 1)
			tx.Commit()

			processMutex.Lock()
			processed = append(processed, val)
			processMutex.Unlock()

			wg.Done()
		}()

		wg.Add(1)
		go func() {
			tx := pipe.Begin()
			val := tx.Pull()
			assert.NotZero(val.value)
			time.Sleep(time.Second * 1)
			tx.Rollback()

			wg.Done()
		}()

		wg.Wait()

		assert.Equal(3, pipe.Len())
		assert.ElementsMatch([]element{
			{category: "a", value: 1},
			{category: "a", value: 2},
		}, processed)
	})
}
