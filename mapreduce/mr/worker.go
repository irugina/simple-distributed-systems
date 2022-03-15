package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// for sorting by key.
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

	for {
		// ask for task
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		succes := call("Coordinator.RequestTask", &args, &reply)
		if !succes {
			return
		}
		switch reply.Task {
		case maptask:
			doMapTask(mapf, reply)
		case reducetask:
			doReduceTask(reducef, reply)
		case finished:
			return
		case try_again_later:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, reply RequestTaskReply) {
	// unpack reply
	filename := reply.Filename
	mapTaskNumber := reply.MapTaskNumber
	nReduce := reply.NReduce

	// read input from disk
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// apply map to input (filename, content) => list of key-value pairs
	kva := mapf(filename, string(content))

	// create nReduce tmp files and attach json encoders to each
	m := make(map[int]*json.Encoder)
	tmp_filenames := make(map[int]string)
	for i := 0; i < nReduce; i++ {
		intermediate_filename := fmt.Sprintf("tmp-%d-%d",  mapTaskNumber, i)
		tmpfile, err := ioutil.TempFile(".", intermediate_filename)
		if err != nil {
			log.Fatal(err)
		}
		m[i] = json.NewEncoder(tmpfile)
		tmp_filenames[i] = tmpfile.Name()
	}

	// write intermediate key-value pairs to disk
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % nReduce
		m[reduceTaskNumber].Encode(&kv)
	}

	// rename tmp files
	for i := 0; i < nReduce; i++ {
		final_filename := fmt.Sprintf("mr-%d-%d",  mapTaskNumber, i)
		os.Rename(tmp_filenames[i], final_filename)
	}

	// tell coordinator task is finished
	args := FinishedTaskArgs{}
	args.Task = maptask
	args.TaskNumber = mapTaskNumber
	finished_reply := FinishedTaskReply{}
	call("Coordinator.FinishedTask", &args, &finished_reply)
}

func doReduceTask(reducef func(string, []string) string , reply RequestTaskReply) {
	// unpack reply
	reduceTaskNumber := reply.ReduceTaskNumber
	nMap := reply.NMap
	// read from nMap intermediate files
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		// open intermediate file on disk
		intermediate_filename := fmt.Sprintf("mr-%d-%d",  i, reduceTaskNumber)
		file, err := os.Open(intermediate_filename)
		if err != nil {
			log.Fatal(err)
		}
		// json decoder to read file
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// sort intermediate
	sort.Sort(ByKey(intermediate))
	// create output file
	oname := fmt.Sprintf("mr-out-%d", reduceTaskNumber)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	// call Reduce on each distinct key in intermediate[], and print the result to ofile
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	// tell coordinator task is finished
	args := FinishedTaskArgs{}
	args.Task = reducetask
	args.TaskNumber = reduceTaskNumber
	finished_reply := FinishedTaskReply{}
	call("Coordinator.FinishedTask", &args, &finished_reply)
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
