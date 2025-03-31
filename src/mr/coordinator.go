package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type CoordinatorPhase uint

type Coordinator struct {
	normalTaskQueue TaskEntryQueue
	retryTaskQueue  TaskEntryQueue
	taskMap         *SafeTaskMap
	mapTaskNum      int
	reduceTaskNum   int
	done            chan struct{}
	CoordinatorPhase
}

const TaskTimedout = 10 * time.Second

const (
	MapPhase CoordinatorPhase = iota
	ReducePhase
	CompletePhase
)

func init() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
}

func (c *Coordinator) DispatchTask(request *DispatchTaskRequest, response *DispatchTaskResponse) error {
	if response == nil {
		return fmt.Errorf("response field is nil")
	}

	if c.CoordinatorPhase == CompletePhase {
		response.TaskFetchStatus = NomoreTasks
		return nil
	}

	response.TaskFetchStatus = TaskAvailable
	retryTask, ok := c.retryTaskQueue.Dequeue()
	if ok {
		response.Task = retryTask
		return nil
	}

	normalTask, ok := c.normalTaskQueue.Dequeue()
	if ok {
		response.Task = normalTask
		return nil
	}

	response.TaskFetchStatus = TaskNotReady
	return nil
}

func (c *Coordinator) MarkTaskComplete(
	request *MarkTaskCompleteRequest,
	response *MarkTaskCompleteResponse,
) error {
	if request == nil || request.Task == nil || response == nil {
		return fmt.Errorf("request or response fieid is nil")
	}

	task, ok := c.taskMap.Get(request.Task.GetMetadata().TaskIdent)
	if !ok {
		return fmt.Errorf(
			"taskId: %d is not registered in coordinator. something is wrong\n",
			request.Task.GetMetadata().ID,
		)
	}
	metadata := task.GetMetadata()
	metadata.SetStatus(Complete)

	slog.Debug(fmt.Sprintf("%d %d is done", metadata.Type, metadata.ID))

	c.updateCoordinatorPhase()

	return nil
}

func (c *Coordinator) isTaskTypeComplete(taskType TaskType) bool {
	isComplete := true
	c.taskMap.Range(func(_ TaskIdent, task Task) {
		taskMetadata := task.GetMetadata()
		if taskMetadata.Type != taskType {
			return
		}

		if taskMetadata.GetStatus() != Complete {
			isComplete = false
		}
	})

	return isComplete
}

func (c *Coordinator) updateCoordinatorPhase() {
	switch c.CoordinatorPhase {
	case MapPhase:
		if !c.isTaskTypeComplete(MapType) {
			return
		}
		c.CoordinatorPhase = ReducePhase
		for i := 0; i < c.reduceTaskNum; i++ {
			taskIdent := TaskIdent{
				ReduceType,
				uint32(i),
			}
			taskMetadata := &TaskMetadata{
				TaskIdent:  taskIdent,
				TaskStatus: Inqueue,
				StartTime:  time.Now().Add(100 * time.Hour),
			}
			reduceTask := NewReduceTask(taskMetadata, c.mapTaskNum)
			c.normalTaskQueue.Enqueue(reduceTask)
			c.taskMap.Set(taskIdent, reduceTask)
		}
	case ReducePhase:
		if !c.isTaskTypeComplete(ReduceType) {
			return
		}
		c.CoordinatorPhase = CompletePhase
		close(c.done)
	case CompletePhase:
		slog.Info("Complete phase. closing...")
	default:
		slog.Error(fmt.Sprintf("Unknown phase %d", c.CoordinatorPhase))
	}
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	select {
	case <-c.done:
		slog.Info("All tasks completed! finish coordinator process")
		return true
	default:
		return false
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		mapTaskNum:       len(files),
		reduceTaskNum:    nReduce,
		done:             make(chan struct{}),
		CoordinatorPhase: MapPhase,
		taskMap:          NewSafeTaskMap(),
	}

	for i, file := range files {
		taskIdent := TaskIdent{
			MapType,
			uint32(i),
		}
		taskMetadata := TaskMetadata{
			TaskIdent:  taskIdent,
			StartTime:  time.Now().Add(100 * time.Hour),
			TaskStatus: Inqueue,
		}

		mapTask := NewMapTask(&taskMetadata, file, nReduce)
		c.normalTaskQueue.Enqueue(mapTask)
		c.taskMap.Set(taskIdent, mapTask)
	}

	go c.reassignTimedOutTasks()

	c.server()
	return c
}

func (c *Coordinator) reassignTimedOutTasks() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.taskMap.Range(func(_ TaskIdent, task Task) {
			taskMetadata := task.GetMetadata()
			if taskMetadata.GetStatus() != Running {
				return
			}

			if time.Since(taskMetadata.GetStartTime()) >= TaskTimedout {
				slog.Error(fmt.Sprintf("timedout. enqueue %d %d", taskMetadata.Type, taskMetadata.ID))
				c.retryTaskQueue.Enqueue(task)
			}
		})
	}
}
