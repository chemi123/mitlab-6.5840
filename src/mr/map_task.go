package mr

type MapTask struct {
	Id        uint32
	FileName  string
	NumReduce int
}

var _ Task = &MapTask{}

func NewMapTask(id uint32, fileName string, numReduce int) *MapTask {
	return &MapTask{
		Id:        id,
		FileName:  fileName,
		NumReduce: numReduce,
	}
}

func (mapperTask *MapTask) ID() uint32 {
	return mapperTask.Id
}
