package mr

import "time"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type TaskStatus int

const (
	idle TaskStatus = iota
	in_progress
	completed
)

type Coordinator struct {
	// Your definitions here.
	nMap int
	nReduce int
	inputFiles []string
	map_tasks_status []TaskStatus
	reduce_tasks_status []TaskStatus
	map_tasks_runtime []time.Time
	reduce_tasks_runtime []time.Time
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// lock coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkForCrushes()
	// set some general attributes for reply
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	// see if we did all the map tasks
	map_done := c.unsafeMapDone()
	if !map_done{// look for map task to assign
		for task_idx, task_status := range c.map_tasks_status{
			if task_status == idle {
				// mark task as in-progress now
				c.map_tasks_status[task_idx] = in_progress
				c.map_tasks_runtime[task_idx] = time.Now()
				// send it to worker
				reply.Filename = c.inputFiles[task_idx]
				reply.MapTaskNumber = task_idx
				reply.Task = maptask
				return nil
			}
		}
	}
	// see if we did all the reduce tasks
	reduce_done := c.unsafeReduceDone()
	if map_done && !reduce_done {// look for reduce task to assign
		for task_idx, task_status := range c.reduce_tasks_status{
			if task_status == idle {
				// mark task as in-progress now
				c.reduce_tasks_status[task_idx] = in_progress
				c.reduce_tasks_runtime[task_idx] = time.Now()
				// send it to worker
				reply.ReduceTaskNumber = task_idx
				reply.Task = reducetask
				return nil
			}
		}
	}
	// if all tasks are done
	if map_done && reduce_done {
		reply.Task = finished
		return nil
	}
	reply.Task = try_again_later
	return nil
}


func (c *Coordinator) FinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tasktype := args.Task
	task_idx := args.TaskNumber
	if tasktype == maptask {
		c.map_tasks_status[task_idx] = completed
	}
	if tasktype == reducetask {
		c.reduce_tasks_status[task_idx] = completed
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

func (c *Coordinator) checkForCrushes() {
	for task_idx, task_runtime := range c.map_tasks_runtime{
                if time.Since(task_runtime).Seconds() > 10 && c.map_tasks_status[task_idx] == in_progress  {
                        c.map_tasks_status[task_idx] = idle
                }
        }
	for task_idx, task_runtime := range c.reduce_tasks_runtime{
                if time.Since(task_runtime).Seconds() > 10 && c.reduce_tasks_status[task_idx] == in_progress  {
                        c.reduce_tasks_status[task_idx] = idle
                }
        }
}
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unsafeMapDone() && c.unsafeReduceDone()
}

func (c *Coordinator) unsafeReduceDone() bool {
	ret := true
	for _, task_status := range c.reduce_tasks_status{
                if task_status != completed {
                        ret = false
                }
        }
        return ret
}

func (c *Coordinator) unsafeMapDone() bool {
	ret := true
	for _, task_status := range c.map_tasks_status{
                if task_status != completed {
                        ret = false
                }
        }
        return ret
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.inputFiles = files
	c.map_tasks_status =  make([]TaskStatus, c.nMap)
	c.reduce_tasks_status =  make([]TaskStatus, c.nReduce)
	c.map_tasks_runtime =  make([]time.Time, c.nMap)
	c.reduce_tasks_runtime =  make([]time.Time, c.nReduce)
	c.server()
	return &c
}
