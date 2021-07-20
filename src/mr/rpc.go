package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	beginning = 0
	doing = 1
	finished = 2
)

const (
	mapphase = 0
	reducephase = 1
	end = 2
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
//
//type ExampleArgs struct {
//	 X int
//}
//
//type ExampleReply struct {
//	 Y int
//}
//
//type HeartBeatArgs struct {
//	time int64
//}
//
//type HeartBeatReply struct {
//	time int64
//}

type WorkerArgs struct {

	WorkerStatus int
}

type WorkerReply struct {
	MapTaskSum int
	ReduceTaskSum int
	Workid int
}

type TaskArgs struct {
	MapOrReduce int
	FileName string
	Taskid int
	Status int
}

type TaskReply struct {
	MapOrReduce int
	FileName string
	Taskid int
	Status int
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
