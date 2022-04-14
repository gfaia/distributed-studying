package mr

import (
	"fmt"
	"time"
)

type stageType int8

const (
	stageStart stageType = iota
	stageMap
	stageReduce
	stageEnd
)

type taskType int8

const (
	taskDefault taskType = iota
	taskMap
	taskReduce
)

func (t taskType) string() string {
	switch t {
	case taskDefault:
		return "default"
	case taskMap:
		return "map"
	case taskReduce:
		return "reduce"
	}
	return ""
}

type Task struct {
	ID           int
	WorkerID     int
	Type         taskType
	MapInputFile string
	NMap         int
	NReduce      int
	StartTime    time.Time
	EndTime      time.Time
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type keyValues []KeyValue

func (kva keyValues) Len() int {
	return len(kva)
}

func (kva keyValues) Less(i, j int) bool {
	return kva[i].Key < kva[j].Key
}

func (kva keyValues) Swap(i, j int) {
	kva[i], kva[j] = kva[j], kva[i]
}

func tmpMapOutFile(workerID int, taskID int, reduceID int) string {
	return fmt.Sprintf("out/map-%d-%d-%d", workerID, taskID, reduceID)
}

func mapOutFile(taskID int, reduceID int) string {
	return fmt.Sprintf("out/map-%d-%d", taskID, reduceID)
}

func tmpReduceOutFile(workerID int, taskID int) string {
	return fmt.Sprintf("out/reduce-%d-%d", workerID, taskID)
}

func reduceOutFile(taskID int) string {
	return fmt.Sprintf("out/reduce-%d", taskID)
}
