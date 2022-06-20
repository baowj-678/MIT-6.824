package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type RegisterRPC struct {
	Mem    int // Memory size (MB)
	CPUNum int // The number of cpu
}

type LogoutRPC struct {
	ID int // ID
}
type LogoutReply struct {
	Status int // 200 for success, 400 for error
}

type RegisterReply struct {
	ReducerNum int
	ID         int
}

type HeartBeat struct {
	Status // worker status
	TaskType
	TaskStatus
	ID     int
	TaskID int
}

type Task struct {
	ID     int
	TaskID int
	TaskType
	TaskStatus
	Key string
}

type ExampleReply struct {
	Y int
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
