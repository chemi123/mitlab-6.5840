package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const TaskTimedout = 10 * time.Second

type Coordinator struct {
	normalTaskQueue   TaskEntryQueue
	retryTaskQueue    TaskEntryQueue
	taskMap           map[TaskIdent]Task
	mapTaskNum        int
	reduceTaskNum     int
	isReduceTaskPhase bool
	// TODO: need chan for exiting
}

func init() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
}

func (c *Coordinator) DispatchTask(request *DispatchTaskRequest, response *DispatchTaskResponse) error {
	if response == nil {
		return fmt.Errorf("response field is nil")
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

	task, ok := c.taskMap[request.Task.GetMetadata().TaskIdent]
	if !ok {
		return fmt.Errorf(
			"taskId: %d is not registered in coordinator. something is wrong\n",
			request.Task.GetMetadata().ID,
		)
	}
	metadata := task.GetMetadata()
	metadata.TaskStatus = Complete

	fmt.Println(metadata.Type, metadata.ID, "is done")

	c.updatePhaseIfNeeded()

	return nil
}

func (c *Coordinator) updatePhaseIfNeeded() {
	if !c.isReduceTaskPhase && c.allMapTasksCompleted() {
		c.isReduceTaskPhase = true
		for i := 0; i < c.reduceTaskNum; i++ {
			taskIdent := TaskIdent{
				ReduceType,
				uint32(i),
			}
			taskMetadata := &TaskMetadata{
				taskIdent,
				Inqueue,
				time.Now().Add(100 * time.Hour),
			}
			reduceTask := NewReduceTask(taskMetadata, c.mapTaskNum)
			c.normalTaskQueue.Enqueue(reduceTask)
			c.taskMap[taskIdent] = reduceTask
		}
	}
}

func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.taskMap {
		// この時点でreducetaskはないが一応行っておく
		mapTask, ok := task.(*MapTask)
		if !ok {
			continue
		}
		taskMetadata := mapTask.GetMetadata()
		if taskMetadata.TaskStatus != Complete {
			return false
		}
	}
	return true
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		mapTaskNum:    len(files),
		reduceTaskNum: nReduce,
	}

	c.taskMap = make(map[TaskIdent]Task)
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

		task := NewMapTask(&taskMetadata, file, nReduce)
		c.normalTaskQueue.Enqueue(task)
		c.taskMap[taskIdent] = task
	}

	go c.reassignTimedOutTasks()

	c.server()
	return c
}

func (c *Coordinator) reassignTimedOutTasks() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, task := range c.taskMap {
			taskMetadata := task.GetMetadata()
			if taskMetadata.TaskStatus != Started {
				continue
			}

			if time.Since(taskMetadata.StartTime) >= TaskTimedout {
				fmt.Println("timedout. enqueue", task.GetMetadata().ID)
				c.retryTaskQueue.Enqueue(task)
			}
		}
	}
}
