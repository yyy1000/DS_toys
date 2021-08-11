package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//the below is from mrsequential.go
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		//time.Sleep(100 * time.Millisecond)
		// uncomment to send the Example RPC to the coordinator.
		//fmt.Println("now call rpc")

		reply, flag := CallExample()
		if !flag {
			break
		}
		if reply.Status == "finish" {
			break
		} else if reply.Status == "no-job" {
			time.Sleep(time.Second)
			continue
		} else if reply.Status == "map" {
			//fmt.Println("now map")
			filename := reply.Filename
			file_index := reply.File_index
			reduce_tasks := reply.Reduce_tasks
			//mr sequential

			//debug to comment the next line
			//intermediate := []KeyValue{}

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			// change to write k/v value pairs to a JSON file
			//intermediate = append(intermediate, kva...)
			var test_files []os.File
			for i := 0; i < reduce_tasks; i++ {
				//mind here, we shall to use TempFile to replace here
				test_file, _ := os.Create(fmt.Sprintf("mr-%d-%d", file_index, i))
				test_files = append(test_files, *test_file)
				defer test_files[i].Close()
			}
			//test_file, _ := os.Create(fmt.Sprintf("test_json_%d", file_index))
			sort.Sort(ByKey(kva))
			for _, kv := range kva {
				enc := json.NewEncoder(&test_files[ihash(kv.Key)%reduce_tasks])
				err := enc.Encode(&kv)
				if err != nil {
					//fmt.Println("encode error, exit")
					return
				}
			}
			CallFinish(0, file_index)
		} else if reply.Status == "reduce" {
			//fmt.Println("now reduce")
			//
			// a big difference from real MapReduce is that all the
			// intermediate data is in one place, intermediate[],
			// rather than being partitioned into NxM buckets.
			//
			//debug here to comment
			file_numbers := reply.File_numbers
			reduce_index := reply.Reduce_index
			oname := fmt.Sprintf("mr-out-%d", reduce_index)
			ofile, _ := os.Create(oname)

			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			intermediate := []KeyValue{}
			for i := 0; i < file_numbers; i++ {
				file, _ := os.Open(fmt.Sprintf("mr-%d-%d", i, reduce_index))
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			CallFinish(1, reduce_index)
			ofile.Close()

		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() (ExampleReply, bool) {

	// declare an argument structure.
	args := ExampleArgs{"ask", 0, 0}

	// fill in the argument(s).
	//args.Command = "ask"
	// declare a reply structure.
	reply := ExampleReply{}

	//fmt.Println("now going call")
	// send the RPC request, wait for the reply.
	flag := call("Coordinator.Example", &args, &reply)

	//fmt.Println(reply.Filename)
	return reply, flag
}

func CallFinish(task_type int, task_index int) (ExampleReply, bool) {
	// declare an argument structure.
	args := ExampleArgs{"finish", task_type, task_index}

	// fill in the argument(s).
	//args.Command = "ask"
	// declare a reply structure.
	reply := ExampleReply{}

	//fmt.Println("now going call")
	// send the RPC request, wait for the reply.
	flag := call("Coordinator.Example", &args, &reply)

	//fmt.Println(reply.Filename)
	return reply, flag
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	//debug
	//fmt.Println("ready to go sys call")
	//
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
