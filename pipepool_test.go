package pipepool

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	assert := assert.New(t)

	pipePool := New(10, func(i int64) int64 {
		return i
	})

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(index int) {
			pipePool.Push(int64(index + 1))
			wg.Done()
		}(i)
	}

	wg.Wait()

	assert.Equal(6, len(pipePool.mPipes))
	assert.Equal(1, pipePool.mPipes[1].Len())
	assert.Equal(1, pipePool.mPipes[2].Len())
	assert.Equal(1, pipePool.mPipes[3].Len())
	assert.Equal(1, pipePool.mPipes[4].Len())
	assert.Equal(1, pipePool.mPipes[5].Len())
	assert.Equal(1, pipePool.mPipes[6].Len())

	var processMutex sync.Mutex
	processed := make([]int64, 0, 5)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			tx := pipePool.Begin()
			val := tx.Pull()
			assert.NotZero(val)
			time.Sleep(time.Second * 5)

			switch val {
			case 1, 2, 3, 4, 5:
				tx.Commit()
				processMutex.Lock()
				processed = append(processed, val)
				processMutex.Unlock()
			case 6:
				tx.Rollback()
			}

			wg.Done()
		}(i)
	}

	wg.Wait()

	assert.Equal(int64(9), pipePool.AvailableCount())
	assert.Equal(1, len(pipePool.mPipes))
	assert.ElementsMatch([]int64{1, 2, 3, 4, 5}, processed)
}

func TestNewWithStruct(t *testing.T) {
	assert := assert.New(t)

	type element struct {
		category string
		value    int64
	}

	pipePool := New(10, func(i element) string {
		return i.category
	})

	pipePool.Push(element{"a", 1})
	pipePool.Push(element{"a", 2})
	pipePool.Push(element{"a", 3})
	pipePool.Push(element{"b", 1})
	pipePool.Push(element{"b", 2})
	pipePool.Push(element{"c", 1})

	assert.Equal(3, len(pipePool.mPipes))
	assert.Equal(3, pipePool.mPipes["a"].Len())
	assert.Equal(2, pipePool.mPipes["b"].Len())
	assert.Equal(1, pipePool.mPipes["c"].Len())

	var processMutex sync.Mutex
	processed := make([]element, 0, 5)
	var processedTimeCost = make(map[string][]float64)

	totalStartTime := time.Now()

	var wg2 sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg2.Add(1)
		go func(i int) {
			startTime := time.Now()
			tx := pipePool.Begin()
			val := tx.Pull()
			assert.NotZero(val)

			switch val.category {
			case "a", "b":
				time.Sleep(time.Second * 2)
				tx.Commit()

				processMutex.Lock()
				processed = append(processed, val)
				processMutex.Unlock()
			case "c":
				time.Sleep(time.Second * 8)
				tx.Rollback()
			}

			endTime := time.Now()
			processMutex.Lock()
			processedTimeCost[val.category] = append(processedTimeCost[val.category], math.Round(endTime.Sub(startTime).Seconds()))
			processMutex.Unlock()

			wg2.Done()
		}(i)
	}

	wg2.Wait()

	totalEndTime := time.Now()

	assert.Equal(int64(9), pipePool.AvailableCount())
	assert.Equal(1, len(pipePool.mPipes))
	assert.ElementsMatch([]element{
		{category: "a", value: 1},
		{category: "a", value: 2},
		{category: "a", value: 3},
		{category: "b", value: 1},
		{category: "b", value: 2},
	}, processed)

	assert.Equal(3, len(processedTimeCost))
	assert.Equal(map[string][]float64{"a": {2, 4, 6}, "b": {2, 4}, "c": {8}}, processedTimeCost)
	assert.Equal(float64(8), math.Round(totalEndTime.Sub(totalStartTime).Seconds()))
}
