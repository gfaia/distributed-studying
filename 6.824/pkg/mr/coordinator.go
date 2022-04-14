package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu sync.Mutex

	stage     stageType
	nMap      int
	nReduce   int
	tasks     map[string]Task
	taskQueue chan Task
}

func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	// handle the last task.
	lastTask := args.LastTask
	if lastTask.Type != taskDefault {
		c.mu.Lock()

		// judger whether the task has been reassigned.
		if task, ok := c.tasks[genTaskID(lastTask.Type, lastTask.ID)]; ok && task.WorkerID == lastTask.WorkerID {
			log.Printf("mark task %d as finished on worker %d\n", lastTask.ID, lastTask.WorkerID)

			if lastTask.Type == taskMap {
				for i := 0; i < lastTask.NReduce; i++ {
					if err := os.Rename(tmpMapOutFile(lastTask.WorkerID, lastTask.ID, i), mapOutFile(lastTask.ID, i)); err != nil {
						log.Fatalf(err.Error())
						return err
					}
				}
			} else if lastTask.Type == taskReduce {
				for i := 0; i < lastTask.NMap; i++ {
					if err := os.Remove(mapOutFile(i, task.ID)); err != nil {
						log.Fatalf(err.Error())
						return err
					}
				}
				if err := os.Rename(tmpReduceOutFile(lastTask.WorkerID, lastTask.ID), reduceOutFile(lastTask.ID)); err != nil {
					log.Fatalf(err.Error())
					return err
				}
			}
		}

		// delete task only when finshed
		delete(c.tasks, genTaskID(lastTask.Type, lastTask.ID))

		if len(c.tasks) == 0 {
			c.transit()
		}

		c.mu.Unlock()
	}

	task, ok := <-c.taskQueue
	if !ok && len(c.tasks) == 0 {
		return nil
	}

	log.Printf("assign %s task %d to worker %d\n", task.Type.string(), task.ID, args.WorkerID)

	c.mu.Lock()
	defer c.mu.Unlock()
	task.WorkerID = args.WorkerID
	task.StartTime = time.Now()
	c.tasks[genTaskID(task.Type, task.ID)] = task
	reply.Task = task

	return nil
}

func (c *Coordinator) transit() {
	switch c.stage {
	case stageStart:
		log.Fatalf("invalid stage")
	case stageMap:
		log.Printf("all map tasks finished.\n")
		c.stage = stageReduce

		for i := 0; i < c.nReduce; i++ {
			t := Task{
				ID:      i,
				Type:    taskReduce,
				NMap:    c.nMap,
				NReduce: c.nReduce,
			}
			c.tasks[genTaskID(t.Type, i)] = t
			c.taskQueue <- t
		}
	case stageReduce:
		log.Printf("all reduce tasks finished.\n")
		c.stage = stageEnd
		close(c.taskQueue)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.stage == stageEnd {
		ret = true
	}

	return ret
}

func genTaskID(typ taskType, taskID int) string {
	return fmt.Sprintf("%s-%d", typ.string(), taskID)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:     stageStart,
		nMap:      len(files),
		nReduce:   nReduce,
		tasks:     make(map[string]Task, len(files)),
		taskQueue: make(chan Task, max(len(files), nReduce)),
	}

	c.stage = stageMap
	for i, file := range files {
		t := Task{
			ID:           i,
			Type:         taskMap,
			MapInputFile: file,
			NMap:         len(files),
			NReduce:      nReduce,
		}
		c.tasks[genTaskID(t.Type, i)] = t
		c.taskQueue <- t
	}

	c.server()

	go func() {
		time.Sleep(500 * time.Millisecond)

		c.mu.Lock()
		for _, task := range c.tasks {
			if task.WorkerID != 0 && time.Now().After(task.StartTime.Add(time.Second*10)) {
				log.Printf("found timed-out %s task %d previously running on worker %d.\n", task.Type.string(), task.ID, task.WorkerID)
				task.WorkerID = 0
				c.taskQueue <- task
			}
		}
		c.mu.Unlock()
	}()

	return &c
}
