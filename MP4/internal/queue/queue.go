package queue

import (
	"container/list"
	"fmt"
	"sync"
)


type Queue struct {
	data  *list.List
	lock  sync.RWMutex
	cond  *sync.Cond
}

func NewQueue() *Queue {
	q := &Queue{
		data: list.New(),
		lock: sync.RWMutex{},
	}
	q.cond = sync.NewCond(&q.lock)
	return q
}

func (q *Queue) Push(val any) {
	q.lock.Lock()
	q.data.PushBack(val)
	q.lock.Unlock()

	// Wake processes up
	q.cond.Broadcast()
}

func (q *Queue) Pop() any {
	q.lock.Lock()
	defer q.lock.Unlock()

	// Wait until a new push
	// cond.Wait would release the lock and re-acquire it
	for q.data.Len() == 0 {
		q.cond.Wait()
	}
	front := q.data.Front()
	val := front.Value
	q.data.Remove(front)

	return val
}

func (q *Queue) Front() (any, error) {
	q.lock.RLock()
	defer q.lock.RUnlock()
	if q.data.Len() == 0 {
		return nil, fmt.Errorf("queue is empty")
	}
 	return q.data.Front().Value, nil
}
 
func (q *Queue) Empty() bool {
	q.lock.RLock() 
	defer q.lock.RUnlock()
	return q.data.Len() == 0
}

func (q *Queue) Size() int {
	q.lock.RLock() 
	defer q.lock.RUnlock()
	return q.data.Len()
}