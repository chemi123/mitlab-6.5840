package mr

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Fprintln(os.Stderr, err.Error())
	return false
}

func doMap(
	mapf func(string, string) []KeyValue,
	task *MapTask,
) error {
	file, err := os.Open(task.FileName)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	keyValueList := mapf(task.FileName, string(content))
	fileMap := make(map[string]*os.File)
	defer func() {
		for _, file := range fileMap {
			file.Close()
		}
	}()
	for _, keyValue := range keyValueList {
		reduceId := ihash(keyValue.Key) % task.NumReduce
		intermediateFileName := fmt.Sprintf("mr-%d-%d", task.GetMetadata().ID, reduceId)
		intermediateFile, ok := fileMap[intermediateFileName]
		if !ok {
			file, err := os.Create(intermediateFileName)
			if err != nil {
				return err
			}
			intermediateFile = file
			fileMap[intermediateFileName] = intermediateFile
		}

		if _, err := fmt.Fprintf(
			intermediateFile,
			"%s %s\n",
			keyValue.Key,
			keyValue.Value,
		); err != nil {
			return err
		}
	}

	return nil
}

func doReduce(
	reducef func(string, []string) string,
	task *ReduceTask,
) error {
	reduceID := task.Metadata.ID
	keyValues := make(map[string][]string)
	for i := 0; i < task.MapTaskNum; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, reduceID)
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			return err
		}
		defer intermediateFile.Close()

		content, err := io.ReadAll(intermediateFile)
		if err != nil {
			return err
		}

		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}

			parts := strings.Fields(line)
			if len(parts) != 2 {
				fmt.Fprintf(os.Stderr, "invalid line format: %q\n", line)
				continue
			}
			key, value := parts[0], parts[1]
			keyValues[key] = append(keyValues[key], value)
		}
	}

	reduceOutputFileName := fmt.Sprintf("mr-out-%", reduceID)
	reduceOutputFile, err := os.Create(reduceOutputFileName)
	if err != nil {
		return err
	}

	for key, values := range keyValues {
		if _, err := fmt.Fprintf(reduceOutputFile, "%s %s", key, reducef(key, values)); err != nil {
			return err
		}
	}
	return nil
}

func callDipatchTask() (*DispatchTaskResponse, error) {
	request := &DispatchTaskRequest{}
	response := &DispatchTaskResponse{}

	ok := call("Coordinator.DispatchTask", request, response)
	if !ok {
		return nil, fmt.Errorf("rpc call for Coordinator.DispatchTask failed")
	}
	return response, nil
}

func callMarkTaskComplete(task Task) error {
	request := &MarkTaskCompleteRequest{task}
	response := &MarkTaskCompleteResponse{}

	ok := call("Coordinator.MarkTaskComplete", request, response)
	if !ok {
		return fmt.Errorf("rpc call for Coordinator.MarkTaskComplete failed")
	}

	return nil
}

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		response, err := callDipatchTask()
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error()+". Retry in 3 seconds")
			time.Sleep(3 * time.Second)
			continue
		}

		if response.TaskFetchStatus == NoMoreTasks || response.TaskFetchStatus == CoordinatorExit {
			fmt.Println("No more tasks to proceed")
			break
		}

		if response.TaskFetchStatus == TaskNotReady {
			fmt.Println("Next task is not ready yet.")
			time.Sleep(3 * time.Second)
			continue
		}

		if err := worker(mapf, reducef, response.Task); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}

	}
}

func worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	task Task,
) error {
	taskMetadata := task.GetMetadata()

	ctx, cancel := context.WithTimeout(context.Background(), TaskTimedout-time.Since(taskMetadata.StartTime))
	defer cancel()
	done := make(chan error, 1)

	switch t := task.(type) {
	case *MapTask:
		go func() {
			done <- doMap(mapf, task.(*MapTask))
		}()
	case *ReduceTask:
		go func() {
			done <- doReduce(reducef, task.(*ReduceTask))
		}()
	default:
		return fmt.Errorf("Unknown or nil Task type: %T\n", t)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("task %d timedout", taskMetadata.ID)
	case err := <-done:
		if err != nil {
			return err
		}

		if err = callMarkTaskComplete(task); err != nil {
			return err
		}
	}
	return nil
}
