package mr

import (
	"os"
	"strconv"
)

type TaskFetchStatus uint32

const (
	TaskAvailable TaskFetchStatus = iota
	TaskNotReady
	NomoreTasks
)

type (
	DispatchTaskRequest  struct{}
	DispatchTaskResponse struct {
		TaskFetchStatus
		Task
	}
)

type (
	MarkTaskCompleteRequest struct {
		Task
	}
	MarkTaskCompleteResponse struct{}
)

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
