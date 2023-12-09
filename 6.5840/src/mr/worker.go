package mr

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"

// Map functions return a slice of KeyValue.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	exit := true
	for exit {
		mapTaskArgs := MapTaskArgs{}
		mapTaskReply := MapTaskReply{}
		call("Coordinator.AskForMapTask", &mapTaskArgs, &mapTaskReply)
		reduceArgs := ReduceTaskArgs{}
		reduceReply := ReduceTaskReply{}
		if mapTaskReply.Status {
			if mapTaskReply.Completed {
				// 所有的map都做完了，申请reduce任务
				call("Coordinator.AskForReduceTask", &reduceArgs, &reduceReply)
				if reduceReply.Status {
					// 所有的任务都分发完了
					if reduceReply.Completed {
						exit = false
					} else {
						continue
					}
				} else {
					//fmt.Printf("reduce task %v %v\n", reduceReply.Index, reduceReply.FileNum)
					intermeidate := make(map[string][]string)
					for i := 0; i < reduceReply.FileNum; i++ {
						filename := "mr-intermediate-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceReply.Index)
						file, err := os.Open(filename)
						if err == nil {
							content, ioerr := ioutil.ReadAll(file)
							if ioerr == nil {
								kv_pairs := strings.Split(string(content), "\n")
								for _, pair := range kv_pairs {
									kv := strings.Split(pair, " ")
									if len(kv) == 2 {
										if val, ok := intermeidate[kv[0]]; ok {
											intermeidate[kv[0]] = append(val, kv[1])
										} else {
											intermeidate[kv[0]] = append(make([]string, 0), kv[1])
										}
									}
								}
							}
						}
					}

					file, err := os.Create("mr-out-" + strconv.Itoa(reduceReply.Index))
					if err == nil {
						for k, v := range intermeidate {
							result := reducef(k, v)
							fmt.Fprintf(file, "%v %v\n", k, result)
						}
					}
					file.Close()
					reduceArgs.Index = reduceReply.Index
					call("Coordinator.ReduceTaskFinish", &reduceArgs, &reduceReply)
					//fmt.Printf("finish reduce task %v for %v files\n", reduceReply.Index, reduceReply.FileNum)
					// 读取所有对应 nReduce 下标的中间文件的内容 key []value
				}
			} else {
				// 所有的map都分发了，但是不是所有都任务都做完了
				continue
			}
		} else {
			//fmt.Printf("do map task %v %v\n", mapTaskReply.Index, mapTaskReply.Filename)
			mapResult := mapf(mapTaskReply.Filename, mapTaskReply.Content)
			f := make([]*os.File, mapTaskReply.NReduce)
			for i := 0; i < mapTaskReply.NReduce; i++ {
				f[i], _ = os.Create("mr-intermediate-" + strconv.Itoa(mapTaskReply.Index) + "-" + strconv.Itoa(i))
			}
			for i := 0; i < len(mapResult); i++ {
				index := ihash(mapResult[i].Key) % mapTaskReply.NReduce
				fmt.Fprintf(f[index], "%v %v\n", mapResult[i].Key, mapResult[i].Value)
			}
			for _, file := range f {
				file.Close()
			}
			mapTaskArgs.Filename = mapTaskReply.Filename
			mapTaskArgs.Index = mapTaskReply.Index
			// map task completed
			call("Coordinator.MapTaskFinish", &mapTaskArgs, &mapTaskReply)
			//fmt.Printf("finish map task %v %v\n", mapTaskReply.Index, mapTaskReply.Filename)
			// worker 同时承担 Map 和 Reduce 任务
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
