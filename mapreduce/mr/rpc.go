package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


type TaskType int

const (
	maptask TaskType = iota
	reducetask
	finished
	try_again_later
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestTaskArgs struct{
}

type RequestTaskReply struct{
	NReduce int
	NMap int
	MapTaskNumber int
	ReduceTaskNumber int
	Task TaskType
	Filename string
}

type FinishedTaskArgs struct {
	Task TaskType
	TaskNumber int
}

type FinishedTaskReply struct {
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
