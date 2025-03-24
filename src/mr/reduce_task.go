package mr

type ReduceTask struct {
	Id uint32
}

var _ Task = &ReduceTask{}

func NewReduceTask(id uint32) *ReduceTask {
	return &ReduceTask{
		Id: id,
	}
}

func (r *ReduceTask) ID() uint32 {
	return r.Id
}
