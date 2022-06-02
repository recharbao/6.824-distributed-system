package mr

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	//Your definitions here.
	stage_map_or_reduce int

	file []string

	workState []int
	workerSum int

	mapTask []Task
	mapTaskSum int
	mapFinished int

	reduceTask []Task
	reduceTaskSum int
	reduceFinished int

	Mu sync.Mutex
}


type Task struct {
	map_or_reduce int
	fileName string
	taskid int
	status int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//


func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {

	c.Mu.Lock()
	task := c.Scheduler()

	reply.Status = task.status
	reply.Taskid = task.taskid
	reply.MapOrReduce = task.map_or_reduce
	reply.FileName = task.fileName

	c.Mu.Unlock()

	go c.WaitTask(task)

	return nil
}


func (c *Coordinator) WaitTask(task *Task) {
	time.Sleep(5 * time.Second)
	c.Mu.Lock()
	if task.status == doing {
		task.status = beginning
	}
	c.Mu.Unlock()
}


func (c *Coordinator) Register(args *WorkerArgs, reply *WorkerReply) error {
	c.Mu.Lock()
	c.workerSum++
	reply.Workid = c.workerSum

	c.workState = append(c.workState, args.WorkerStatus)

	reply.MapTaskSum = c.mapTaskSum
	reply.ReduceTaskSum = c.reduceTaskSum
	c.Mu.Unlock()

	return nil
}


func (c *Coordinator) ReportTaskState(args *TaskArgs, reply *TaskReply) error {
	c.Mu.Lock()
	if args.Status == doing {
		index := args.Taskid
		if args.MapOrReduce == mapphase {
			c.mapTask[index].status = finished
			c.mapFinished++
		}else if args.MapOrReduce == reducephase {
			c.reduceTask[index].status = finished
			c.reduceFinished++
		}
	}
	c.Mu.Unlock()
	return nil
}


func (c *Coordinator) Scheduler() *Task{
	if c.stage_map_or_reduce == mapphase {
		if c.mapFinished < c.mapTaskSum {
			for i := 0; i < c.mapTaskSum; i++ {
				if c.mapTask[i].status == beginning {
					c.mapTask[i].status = doing
					return &c.mapTask[i]
				}
			}
			task := Task{}
			task.status = -1
			return &task
		}
		c.stage_map_or_reduce = reducephase
	} 
	if c.stage_map_or_reduce == reducephase {
		if c.reduceFinished < c.reduceTaskSum {
			for i := 0; i < c.reduceTaskSum; i++ {
				if c.reduceTask[i].status == beginning {
					c.reduceTask[i].status = doing
					return &c.reduceTask[i]
				}
			}

			task := Task{}
			task.status = -1
			return &task
		}

		c.stage_map_or_reduce = end
	}

	task := Task{}
	task.map_or_reduce = c.stage_map_or_reduce
	return &task
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

	// Your code here.
	if c.stage_map_or_reduce == end {
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
	c.mapTaskSum = len(files)
	c.reduceTaskSum = nReduce
	c.stage_map_or_reduce = mapphase
	c.file = make([]string, len(files))
	c.workerSum = 0
	// read file
	for i := 0; i < len(files); i++ {
		fileName := files[i]
		c.file[i] = fileName
	}

	c.initMapTask()
	c.initReduceTask()

	c.server()
	return &c
}

func (c *Coordinator) initMapTask() {
	for i := 0; i < c.mapTaskSum; i++ {
		task := Task{}
		task.map_or_reduce = mapphase
		task.taskid = i
		task.status = beginning
		task.fileName = c.file[i]
		c.mapFinished = 0
		c.mapTask = append(c.mapTask, task)
	}
}

func (c *Coordinator) initReduceTask()  {
	for i := 0; i < c.reduceTaskSum; i++ {
		task := Task{}
		task.map_or_reduce = reducephase
		task.taskid = i
		task.status = beginning
		c.reduceFinished = 0
		task.fileName = "mr-out-" + strconv.Itoa(i)
		c.reduceTask = append(c.reduceTask, task)
	}
}

