package mr

import (
	"sync"
	"time"
)

type TaskMetadata struct {
	mu sync.Mutex
	TaskIdent
	TaskStatus
	StartTime time.Time
}

func (t *TaskMetadata) SetStatus(status TaskStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.TaskStatus = status
}

func (t *TaskMetadata) SetStartTime(tm time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.StartTime = tm
}

func (t *TaskMetadata) GetStatus() TaskStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.TaskStatus
}

func (t *TaskMetadata) GetStartTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.StartTime
}
