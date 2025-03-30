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
	Running
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
