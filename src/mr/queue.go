package mr

import "sync"

type TaskQueue struct {
	mu    sync.Mutex
	tasks []Task
}

func (queue *TaskQueue) Enqueue(task Task) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.tasks = append(queue.tasks, task)
}

func (queue *TaskQueue) Dequeue() (Task, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.tasks) == 0 {
		return nil, false
	}

	task := queue.tasks[0]
	queue.tasks = queue.tasks[1:]

	return task, true
}
