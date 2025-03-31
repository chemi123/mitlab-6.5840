package mr

import "sync"

type SafeTaskMap struct {
	mu sync.RWMutex
	m  map[TaskIdent]Task
}

func NewSafeTaskMap() *SafeTaskMap {
	return &SafeTaskMap{
		m: make(map[TaskIdent]Task),
	}
}

func (s *SafeTaskMap) Get(id TaskIdent) (Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.m[id]
	return task, ok
}

func (s *SafeTaskMap) Set(id TaskIdent, task Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id] = task
}

func (s *SafeTaskMap) Delete(id TaskIdent, task Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id)
}

func (s *SafeTaskMap) Range(fn func(TaskIdent, Task)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.m {
		fn(k, v)
	}
}
