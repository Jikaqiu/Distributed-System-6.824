package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for true {
		newWork := AssignWorker()
		if newWork.WorkerType == IdleTask {
			time.Sleep(WorkerIdleTime * time.Second)
			continue
		}
		if newWork.WorkerType == MapTask {
			if mapper(mapf, newWork.Task, newWork.NReduce) {
				newWork.Task.State = Succeed
			} else {
				newWork.Task.State = Failed
			}
		} else if newWork.WorkerType == ReduceTask {
			if reducer(reducef, newWork.Task, newWork.NMap) {
				newWork.Task.State = Succeed
			} else {
				newWork.Task.State = Failed
			}
		}
		WorkFinished(newWork.Task, newWork.WorkerType)
	}
}

func mapper(mapf func(string, string) []KeyValue, task Task, nReduce int) bool {
	content, err := os.ReadFile(task.File)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
		return false
	}
	kva := mapf(task.File, string(content))
	output := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		index := ihash(kv.Key) % (nReduce)
		output[index] = append(output[index], kv)
	}

	for index, f := range output {
		filename := "mr-" + strconv.Itoa(task.Index) + "-" + strconv.Itoa(index)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("create file failed %v %v", filename, err)
			return false
		}
		enc := json.NewEncoder(file)
		for _, kv := range f {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Encode mapper result failed")
				return false
			}
		}
	}
	return true
}

func reducer(reducef func(string, []string) string, task Task, nMap int) bool {
	inPut := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.Index)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("open reduce file failed")
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := inPut[kv.Key]; !ok {
				inPut[kv.Key] = make([]string, 0, 100)
			}
			inPut[kv.Key] = append(inPut[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, vs := range inPut {
		res = append(res, fmt.Sprintf("%v %v\n", k, reducef(k, vs)))
	}

	if err := ioutil.WriteFile(fmt.Sprintf("mr-out-%d", task.Index), []byte(strings.Join(res, "")), 0600); err != nil {
		return false
	}
	return true
}

func AssignWorker() AssignWorkerReply {
	args := AssignWorkerArgs{}
	reply := AssignWorkerReply{}
	ok := call("Coordinator.AssignWorker", &args, &reply)
	if ok {
		//fmt.Printf("reply.WorkerType %v\n", reply.WorkerType)
	} else {
		fmt.Printf("AssignWorker call failed!\n")
	}
	return reply
}

func WorkFinished(task Task, workerType int) WorkFinishedReply {
	args := WorkFinishedArgs{}
	args.Task = task
	args.WorkerType = workerType
	reply := WorkFinishedReply{}
	ok := call("Coordinator.WorkFinished", &args, &reply)
	if ok {
		//fmt.Printf("reply.WorkFinished success\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
