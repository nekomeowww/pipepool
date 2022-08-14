package pipepool

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		assert := assert.New(t)

		pool := NewPool(10)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				assert.True(pool.Acquire())
				wg.Done()
			}()
		}

		wg.Wait()
		assert.Equal(int64(0), pool.AvailableCount())

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				pool.Release()
				wg.Done()
			}()
		}

		wg.Wait()
		assert.Equal(int64(10), pool.AvailableCount())
	})

	t.Run("OverExecuted", func(t *testing.T) {
		assert := assert.New(t)

		pool := NewPool(10)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				assert.True(pool.Acquire())
				wg.Done()
			}()
		}

		wg.Wait()
		assert.Equal(int64(0), pool.AvailableCount())

		assert.False(pool.Acquire())
		pool.Release()
		assert.True(pool.Acquire())

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				pool.Release()
				wg.Done()
			}()
		}

		wg.Wait()
		assert.Equal(int64(10), pool.AvailableCount())
	})
}
