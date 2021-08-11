package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files []string
	//file_index     int
	reduce_tasks int
	//reduce_index   int
	//0 for idle, 1 for assigned, 2 for finished
	record_map     []int
	record_reduce  []int
	map_time       []time.Time
	reduce_time    []time.Time
	status         string
	mu             sync.Mutex
	mu2            sync.Mutex
	finish_maps    int
	finish_reduces int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func (c *Coordinator) beginTimer(task_type int, task_index int) {
	c.mu2.Lock()
	defer c.mu2.Unlock()
	time.Sleep(10 * time.Second)
	if task_index == 0 {
		if c.record_map[task_index] == 1 {
			c.record_map[task_index] = 0
		}
	} else if task_index == 1 {
		if c.record_reduce[task_index] == 1 {
			c.record_reduce[task_index] = 0
		}
	}
}
*/
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Command == "ask" {
		reply.File_numbers = len(c.files)
		if c.status == "map" {
			var i int
			for i = 0; i < len(c.files); i++ {
				if c.record_map[i] == 1 {
					if time.Now().Sub(c.map_time[i]).Seconds() >= 10 {
						c.record_map[i] = 0
					}
				}
			}
			for i = 0; i < len(c.files); i++ {
				if c.record_map[i] == 0 {
					c.record_map[i] = 1
					c.map_time[i] = time.Now()
					break
				}
			}
			if i == len(c.files) {
				reply.Status = "no-job"
				return nil
			}
			reply.Filename = c.files[i]
			reply.File_index = i
			reply.Reduce_tasks = c.reduce_tasks
			reply.Status = "map"
		} else if c.status == "reduce" {
			var i int
			for i = 0; i < c.reduce_tasks; i++ {
				if c.record_reduce[i] == 1 {
					if time.Now().Sub(c.reduce_time[i]).Seconds() >= 10 {
						c.record_reduce[i] = 0
					}
				}
			}
			for i = 0; i < c.reduce_tasks; i++ {
				if c.record_reduce[i] == 0 {
					c.record_reduce[i] = 1
					c.reduce_time[i] = time.Now()
					break
				}
			}
			if i == c.reduce_tasks {
				reply.Status = "no-job"
				return nil
			}
			reply.Reduce_index = i
			reply.Status = "reduce"
		} else if c.status == "no-job" {
			reply.Status = "no-job"
		} else if c.status == "finish" {
			reply.Status = "finish"
		}
	} else if args.Command == "finish" {
		if args.Finish_type == 0 {
			//fmt.Printf("now finish map %d", args.Finish_Task)
			c.record_map[args.Finish_Task] = 2
			c.finish_maps++
			if c.finish_maps == len(c.files) {
				c.status = "reduce"
			}
		} else if args.Finish_type == 1 {
			//fmt.Printf("now finish reduce %d", args.Finish_Task)
			c.record_reduce[args.Finish_Task] = 2
			c.finish_reduces++
			if c.finish_reduces == c.reduce_tasks {
				c.status = "finish"
			}
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == "finish" {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// initializa the coordinator
	c.files = files
	c.record_map = make([]int, len(c.files))
	c.map_time = make([]time.Time, len(c.files))
	//c.file_index = 0
	c.reduce_tasks = nReduce
	c.record_reduce = make([]int, nReduce)
	c.reduce_time = make([]time.Time, nReduce)
	//c.reduce_index = 0
	c.status = "map"
	// end
	c.server()
	return &c
}
