package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

type taskType int8

const (
	DefaultTask taskType = iota
	MapTask
	ReduceTask
)

type Task struct {
	ID           int
	WorkerID     string
	Type         taskType
	MapInputFile string
	NMap         int
	NReduce      int
	StartTime    time.Time
	EndTime      time.Time
}

type ApplyTaskArgs struct {
	WorkerID string
	LastTask Task
}

type ApplyTaskReply struct {
	Task Task
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
