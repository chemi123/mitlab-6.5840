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
	metadata.SetStatus(Inqueue)
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
	metadata.SetStartTime(time.Now())
	metadata.SetStatus(Running)

	return task, true
}
