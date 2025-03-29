package mr

import (
	"time"
)

type (
	TaskType   uint32
	TaskStatus uint32
)

const (
	MapType TaskType = iota
	ReduceType
)

const (
	Inqueue TaskStatus = iota
	Started
	Complete
)

type Task interface {
	GetMetadata() *TaskMetadata
}

type TaskIdent struct {
	Type TaskType
	ID   uint32
}
type TaskMetadata struct {
	TaskIdent
	TaskStatus
	StartTime time.Time
}

func (t *TaskMetadata) Start() {
	t.TaskStatus = Started
	t.StartTime = time.Now()
}
