package mr

type ReduceTask struct {
	Metadata   *TaskMetadata
	MapTaskNum int
}

var _ Task = &ReduceTask{}

func NewReduceTask(taskMetadata *TaskMetadata, mapTaskNum int) *ReduceTask {
	return &ReduceTask{
		taskMetadata,
		mapTaskNum,
	}
}

func (r *ReduceTask) GetMetadata() *TaskMetadata {
	return r.Metadata
}
