package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	pid := os.Getegid()

	var lastTask Task

	for {
		args := ApplyTaskArgs{
			WorkerID: pid,
			LastTask: lastTask,
		}

		reply := ApplyTaskReply{}

		if err := call("Coordinator.ApplyTask", &args, &reply); err != nil {
			log.Fatalf("call failed! worker id %d\n", pid)
			continue
		}

		task := reply.Task
		task.WorkerID = pid
		log.Printf("task type %s id %d", task.Type.string(), task.ID)

		if task.Type == taskDefault {
			log.Printf("map reduce finished!")
			break
		}

		if task.Type == taskMap {
			if err := handleMapTask(task, mapf); err != nil {
				log.Fatalf("failed to handle map task: %s", err.Error())
				continue
			}
		} else if task.Type == taskReduce {
			if err := handleReduceTask(task, reducef); err != nil {
				log.Fatalf("failed to handle reduce task: %s", err.Error())
				continue
			}
		}

		// update the last task.
		task.EndTime = time.Now()
		lastTask = task
	}
}

func handleMapTask(task Task, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(task.MapInputFile)
	if err != nil {
		log.Fatalf("failed to open map input file %s", task.MapInputFile)
		return err
	}
	kva := mapf(task.MapInputFile, string(content))

	// bucket according to the hash of key.
	hashedKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		hashed := ihash(kv.Key) % task.NReduce
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}

	// write the results to the temporary files and commit in coordinator later.
	for i, kva := range hashedKva {
		outfile := tmpMapOutFile(task.WorkerID, task.ID, i)
		ofile, err := os.Create(outfile)
		if err != nil {
			log.Fatalf("failed to create map output file %s", outfile)
			return err
		}
		for _, kv := range kva {
			fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}
	return nil
}

func handleReduceTask(task Task, reducef func(string, []string) string) error {
	var lines []string
	for i := 0; i < task.NMap; i++ {
		content, err := os.ReadFile(mapOutFile(i, task.ID))
		if err != nil {
			log.Fatalf("failed to open map output file %s", mapOutFile(i, task.ID))
			return err
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   parts[0],
			Value: parts[1],
		})
	}
	ofile, err := os.Create(tmpReduceOutFile(task.WorkerID, task.ID))
	if err != nil {
		log.Fatalf("failed to open temporatory output file %s", tmpReduceOutFile(task.WorkerID, task.ID))
		return err
	}

	// sort the kv according to key.
	sort.Sort(keyValues(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()

	return nil
}

// call send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return err
	}

	return nil
}
