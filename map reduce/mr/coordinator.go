package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MapTask    = 101
	ReduceTask = 102
	IdleTask   = 103

	Unsigned  = 201
	InProcess = 202
	Succeed   = 203
	Failed    = 204
)

const (
	TaskMaxExecuteTime = 10
	WorkerIdleTime     = 2
)

type Task struct {
	Index int
	State int
	File  string
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	mu      sync.Mutex
	taskCh  chan Task

	mapTaskTotalNum    int
	mapTaskFinishedNum int

	mapTaskResponded map[int]bool

	reduceTaskResponded   map[int]bool
	reduceWorkerNum       int
	reduceTaskFinishedNum int

	mapFinished bool
	finished    bool
}

func (c *Coordinator) AssignWorker(args *AssignWorkerArgs, reply *AssignWorkerReply) error {
	if c.reduceWorkerNum == c.nReduce {
		//fmt.Printf("max reduce worker-new idle worker!\n")
		reply.WorkerType = IdleTask
		return nil
	}
	t, ok := <-c.taskCh
	if !ok {
		fmt.Printf("no task available-new idle worker!\n")
		reply.WorkerType = IdleTask
		return nil
	}

	t.State = InProcess
	reply.Task = t
	if !c.mapFinished {
		//fmt.Printf("new map worker!\n")
		reply.WorkerType = MapTask
	} else if c.reduceWorkerNum < c.nReduce {
		//fmt.Printf("new reduce worker!\n")
		reply.WorkerType = ReduceTask
	}
	reply.NReduce = c.nReduce
	reply.NMap = c.mapTaskTotalNum

	go c.workerTimer(reply.WorkerType, t)
	return nil
}

// sleep 10s then check if work finished.
func (c *Coordinator) workerTimer(WorkerType int, task Task) error {
	time.Sleep(TaskMaxExecuteTime * time.Second)
	if WorkerType == MapTask {
		if c.mapTaskResponded[task.Index] {
			return nil
		}
		//fmt.Printf("map workerTimer!\n")
		task.State = Unsigned
		c.taskCh <- task
	} else if WorkerType == ReduceTask {
		if c.reduceTaskResponded[task.Index] {
			return nil
		}
		//fmt.Printf("reduce workerTimer!\n")
		task.State = Unsigned
		c.taskCh <- task
	}
	return nil
}

// WorkFinished set work finished
func (c *Coordinator) WorkFinished(args *WorkFinishedArgs, reply *WorkFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := args.Task
	if args.WorkerType == MapTask {
		if t.State == Succeed {
			//fmt.Printf("map worker success!\n")
			c.mapTaskFinishedNum += 1
			if c.mapTaskFinishedNum == c.mapTaskTotalNum {
				c.mapFinished = true
				for i := 0; i < c.nReduce; i++ {
					task := Task{
						Index: i,
						State: Unsigned,
					}
					c.taskCh <- task
				}
			}
		}
		c.mapTaskResponded[t.Index] = true
	} else {
		if t.State == Succeed {
			//fmt.Printf("reduce worker success!\n")
			c.reduceTaskFinishedNum += 1
			if c.reduceTaskFinishedNum == c.nReduce {
				c.finished = true
			}
		}
		c.reduceTaskResponded[t.Index] = true
	}
	if t.State == Failed {
		//fmt.Printf("worker fail!\n")

		t.State = Unsigned
		c.taskCh <- t
	}
	return nil
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.finished
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:                 files,
		nReduce:               nReduce,
		mapTaskTotalNum:       len(files),
		mapTaskFinishedNum:    0,
		reduceWorkerNum:       0,
		reduceTaskFinishedNum: 0,
		mapFinished:           false,
		finished:              false,
	}

	chanLen := len(files)
	if len(files) < nReduce {
		chanLen = nReduce
	}

	c.taskCh = make(chan Task, chanLen)
	c.mapTaskResponded = make(map[int]bool)
	c.reduceTaskResponded = make(map[int]bool)

	for i, file := range files { //initialize all map tasks
		task := Task{
			Index: i,
			State: Unsigned,
			File:  file,
		}
		c.taskCh <- task
	}

	c.server()
	return &c
}
