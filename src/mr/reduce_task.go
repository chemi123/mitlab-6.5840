package mr

type ReduceTask struct {
	id uint32
}

var _ Task = &ReduceTask{}

func NewReduceTask(id uint32) *ReduceTask {
	return &ReduceTask{
		id: id,
	}
}

func (r *ReduceTask) ID() uint32 {
	return r.id
}
