package mr

type MapTask struct {
	Metadata  *TaskMetadata
	FileName  string
	NumReduce int
}

var _ Task = &MapTask{}

func NewMapTask(metadata *TaskMetadata, fileName string, numReduce int) *MapTask {
	return &MapTask{
		metadata,
		fileName,
		numReduce,
	}
}

func (m *MapTask) GetMetadata() *TaskMetadata {
	return m.Metadata
}
