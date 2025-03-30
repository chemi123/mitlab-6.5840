package mr

import (
	"sync"
	"time"
)

type TaskEntryQueue struct {
	mu    sync.Mutex
	Tasks []Task
}

func (queue *TaskEntryQueue) Enqueue(task Task) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	metadata := task.GetMetadata()
	metadata.TaskStatus = Inqueue
	queue.Tasks = append(queue.Tasks, task)
}

func (queue *TaskEntryQueue) Dequeue() (Task, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.Tasks) == 0 {
		return nil, false
	}

	task := queue.Tasks[0]
	queue.Tasks = queue.Tasks[1:]

	metadata := task.GetMetadata()
	metadata.StartTime = time.Now()
	metadata.TaskStatus = Running

	return task, true
}
