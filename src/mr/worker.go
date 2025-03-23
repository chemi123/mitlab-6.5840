package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

	fmt.Println(err)
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

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	keyValueList := mapf(task.FileName, string(content))
	fileMap := make(map[string]*os.File)
	for _, kv := range keyValueList {
		reduceId := ihash(kv.Key) % task.NumReduce
		intermediateFileName := fmt.Sprintf("mr-%d-%d", task.ID(), reduceId)
		intermediateFile, ok := fileMap[intermediateFileName]
		if !ok {
			file, err := os.Create(intermediateFileName)
			if err != nil {
				return err
			}
			intermediateFile = file
			fileMap[intermediateFileName] = intermediateFile
		}

		fmt.Fprintf(intermediateFile, "%s %s\n", kv.Key, kv.Value)
	}

	return nil
}

func doReduce(
	reducef func(string, []string) string,
	task *ReduceTask,
) {
}

func callDipatchTask() (*DispatchTaskResponse, bool) {
	request := &DispatchTaskRequest{}
	response := &DispatchTaskResponse{}

	ok := call("Coordinator.DispatchTask", request, response)
	if !ok {
		fmt.Fprintln(os.Stderr, "rpc call for Coordinator.DispatchTask failed")
		return nil, false
	}
	return response, ok
}

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		response, ok := callDipatchTask()
		if !ok {
			continue
		}

		if response.TaskFetchStatus == NoMoreTasks || response.TaskFetchStatus == CoordinatorExit {
			fmt.Println("No more tasks to proceed")
			break
		}

		if response.TaskFetchStatus == TaskNotReady {
			fmt.Println("Next task is not ready yet.")
			time.Sleep(1 * time.Second)
			continue
		}

		switch t := response.Task.(type) {
		case *MapTask:
			err := doMap(mapf, response.Task.(*MapTask))
			// errがnil出ない場合は一旦は出力だけして次に進む
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
			}
		case *ReduceTask:
			doReduce(reducef, response.Task.(*ReduceTask))
		default:
			fmt.Fprintf(os.Stderr, "Unknown or nil Task type: %T\n", t)
		}
	}
}
