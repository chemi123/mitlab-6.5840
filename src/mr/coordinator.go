package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	normalTaskQueue TaskQueue
	retryTaskQueue  TaskQueue
	// TODO: need chan for exiting
}

func init() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
}

// func (c *Coordinator) TriggerReducerTasks(request *Request, response *Response) error {
// }

func (c *Coordinator) DispatchTask(request *DispatchTaskRequest, response *DispatchTaskResponse) error {
	retryTask, ok := c.retryTaskQueue.Dequeue()
	response.TaskFetchStatus = TaskAvailable
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
	c := &Coordinator{}

	for i, file := range files {
		c.normalTaskQueue.Enqueue(
			NewMapTask(
				uint32(i),
				file,
				nReduce,
			),
		)
	}

	// TODO:: 以下を実施するスレッドを生やす
	// - Dispatchしたtaskの完了確認
	// - Map taskが全部完了したことを確認し、reduce

	c.server()
	return c
}
