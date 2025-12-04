package flow

import (
	"container/list"
	"sync"
	"time"
)

const COUNTER_DURATION = 1 * time.Second

type Msg struct {
	Size      int64
	Timestamp time.Time
}

type FlowCounter struct {
	currentCounter int64
	que            *list.List
	lock           sync.RWMutex
}

func NewFlowCounter() *FlowCounter {
	return &FlowCounter{
		currentCounter: 0,
		que:  list.New(),
		lock: sync.RWMutex{},
	}
}

func (f *FlowCounter) maintain(currentTime time.Time) {
	for f.que.Len() > 0 && currentTime.Sub(f.que.Front().Value.(Msg).Timestamp) > COUNTER_DURATION {
		f.currentCounter -= f.que.Front().Value.(Msg).Size
		f.que.Remove(f.que.Front())
	}
}

func (f *FlowCounter) Add(size int64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.currentCounter += size
	currentTime := time.Now()
	f.que.PushBack(Msg{
		Size:      size,
		Timestamp: currentTime,
	})
	f.maintain(currentTime)
}

func (f *FlowCounter) Get() float64 {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.maintain(time.Now())
	return float64(f.currentCounter) / COUNTER_DURATION.Seconds()
}
