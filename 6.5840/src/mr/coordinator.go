package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lock             sync.RWMutex
	file             []string
	fileNum          int
	nReduce          int
	mapTaskStatus    []int // 0 空闲 1 已分配 2 已完成
	mapTaskContent   []string
	mapTaskTime      []time.Time
	reduceTaskStatus []int // 0 空闲 1 已分配 2 已完成
	reduceTaskTime   []time.Time
	combined         int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskForMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	// 分发出去的map任务是否需要携带token？
	// 超时机制
	// 判断是否所有的map都分发出去了
	c.lock.Lock()
	reply.Status = true
	attribute := false
	for i := 0; i < c.fileNum; i++ {
		if c.mapTaskStatus[i] == 0 {
			reply.Filename = c.file[i]
			reply.Content = c.mapTaskContent[i]
			reply.Index = i
			reply.NReduce = c.nReduce
			reply.Status = false
			c.mapTaskStatus[i] = 1
			c.mapTaskTime[i] = time.Now()
			attribute = true
			break
		}
	}
	// 没有未分发的任务
	if !attribute {
		for i := 0; i < c.fileNum; i++ {
			if c.mapTaskStatus[i] == 1 && c.mapTaskTime[i].Before(time.Now().Add(-10*time.Second)) {
				reply.Filename = c.file[i]
				reply.Content = c.mapTaskContent[i]
				reply.Index = i
				reply.NReduce = c.nReduce
				reply.Status = false
				c.mapTaskStatus[i] = 1
				c.mapTaskTime[i] = time.Now()
				break
			}
		}
	}
	// 判断是否所有的map都做完了
	reply.Completed = true
	for i := 0; i < c.fileNum; i++ {
		if c.mapTaskStatus[i] != 2 {
			reply.Completed = false
		}
	}
	c.lock.Unlock()
	return nil
}
func (c *Coordinator) AskForReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	c.lock.Lock()
	reply.Status = true
	attribute := false
	for i := 0; i < c.nReduce; i++ {
		if c.reduceTaskStatus[i] == 0 {
			reply.Index = i
			reply.FileNum = c.fileNum
			reply.Status = false
			c.reduceTaskStatus[i] = 1
			c.reduceTaskTime[i] = time.Now()
			attribute = true
			break
		}
	}
	if !attribute {
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskStatus[i] == 1 && c.reduceTaskTime[i].Before(time.Now().Add(-10*time.Second)) {
				reply.Index = i
				reply.FileNum = c.fileNum
				reply.Status = false
				c.reduceTaskStatus[i] = 1
				c.reduceTaskTime[i] = time.Now()
				break
			}
		}
	}
	reply.Completed = true
	for i := 0; i < c.nReduce; i++ {
		if c.reduceTaskStatus[i] != 2 {
			reply.Completed = false
		}
	}
	c.lock.Unlock()
	return nil
}
func (c *Coordinator) MapTaskFinish(args *MapTaskArgs, reply *MapTaskReply) error {
	if c.file[args.Index] == args.Filename && c.mapTaskStatus[args.Index] != 2 {
		c.mapTaskStatus[args.Index] = 2
	}
	return nil
}
func (c *Coordinator) ReduceTaskFinish(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	if c.reduceTaskStatus[args.Index] != 2 {
		c.reduceTaskStatus[args.Index] = 2
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	// all m/r tasks has finished
	// Your code here.
	for _, status := range c.mapTaskStatus {
		if status != 2 {
			ret = false
		}
	}
	for _, status := range c.reduceTaskStatus {
		if status != 2 {
			ret = false
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Printf("")
	c := Coordinator{
		file:             files,
		fileNum:          len(files),
		nReduce:          nReduce,
		mapTaskStatus:    make([]int, len(files)),
		mapTaskContent:   make([]string, len(files)),
		reduceTaskStatus: make([]int, nReduce),
		combined:         0,
		mapTaskTime:      make([]time.Time, len(files)),
		reduceTaskTime:   make([]time.Time, nReduce),
	}
	// init coordinator
	for i := 0; i < len(files); i++ {
		// fmt.Println("files input " + strconv.Itoa(i) + " " + files[i])
		file, err := os.Open(files[i])
		if err != nil {
			log.Fatal("error file " + files[i])
		}
		content, err := ioutil.ReadAll(file)
		c.mapTaskContent[i] = string(content)
	}

	/*
		The pg-*.txt arguments to mrcoordinator.go are the input files;
		each file corresponds to one "split",
		and is the input to one Map task.
	*/
	// Your code here.

	c.server()
	return &c
}
