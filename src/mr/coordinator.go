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

var (
	SchedulerFileName = flag.String("log4", "Scheduler.log", "Log file name")
	RequestTaskFileName = flag.String("log2", "RequestTaskLog.log", "Log file name")
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
	// fmt.Printf("here !")
	task := c.Scheduler()

	reply.Status = task.status
	reply.Taskid = task.taskid
	reply.MapOrReduce = task.map_or_reduce
	reply.FileName = task.fileName

	fmt.Printf("reply.MapOrReduce = %v\n", reply.MapOrReduce)

	{
		logFile, logErr := os.OpenFile(*RequestTaskFileName,os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if logErr != nil {
			fmt.Printf("log error !\n")
		}

		log.SetOutput(logFile)
		log.SetFlags(log.Ldate | log.Lmicroseconds | log.Ltime | log.Lshortfile)
		log.Printf("RequestTask task.fileName = %v\n", reply.FileName)
		log.Printf("RequestTask reply_taskid = %d\n", reply.Taskid)
	}

	c.Mu.Unlock()

	go c.WaitTask(task)

	fmt.Printf("after_wait !\n")

	return nil
}


func (c *Coordinator) WaitTask(task *Task) {

	time.Sleep(5 * time.Second)
	c.Mu.Lock()

	// time.Sleep(1000)  It is important, must sleep before hold lock, because if hold lock at first, the go func mostly have no mean

	// fmt.Printf("wait adress = %d\n", task)
	if task.status == doing {
		task.status = beginning
		fmt.Printf("wait !\n")
	}
	c.Mu.Unlock()
}


func (c *Coordinator) Register(args *WorkerArgs, reply *WorkerReply) error {
	// log.Printf("Register !\n")
	c.Mu.Lock()
	c.workerSum++
	reply.Workid = c.workerSum

	c.workState = append(c.workState, args.WorkerStatus)

	reply.MapTaskSum = c.mapTaskSum
	fmt.Printf("c.mapTaskSum = %v\n", c.mapTaskSum)
	reply.ReduceTaskSum = c.reduceTaskSum
	c.Mu.Unlock()

	return nil
}


func (c *Coordinator) ReportTaskState(args *TaskArgs, reply *TaskReply) error {

	c.Mu.Lock()

	if args.Status == doing {
		//fmt.Printf("ReportTaskState !\n")
		index := args.Taskid
		fmt.Printf("args.MapOrReduce = %v\n", args.MapOrReduce)
		if args.MapOrReduce == mapphase {
			// fmt.Printf("ReportTaskState !\n")
			// fmt.Printf("report adress = %d\n", &c.mapTask[index])
			c.mapTask[index].status = finished
			c.mapFinished++
			// fmt.Printf("ReportTaskState !\n")
		}else if args.MapOrReduce == reducephase {
			c.reduceTask[index].status = finished
			c.reduceFinished++
		}
	}
	c.Mu.Unlock()
	return nil
}


func (c *Coordinator) Scheduler() *Task{
	log.Printf("scheduler stage = %d\n", c.stage_map_or_reduce)

	if c.stage_map_or_reduce == mapphase {
		 fmt.Printf("mapFinished = %v\n", c.mapFinished)
		 fmt.Printf("mapTaskSum = %v\n", c.mapTaskSum)
		 // for c.mapFinished < c.mapTaskSum {
		if c.mapFinished < c.mapTaskSum {
			// fmt.Printf("here !\n")
			for i := 0; i < c.mapTaskSum; i++ {
				//{
				//	logFile, logErr := os.OpenFile(*SchedulerFileName,os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
				//	if logErr != nil {
				//		fmt.Printf("log error !\n")
				//	}
				//
				//	log.SetOutput(logFile)
				//	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Ltime | log.Lshortfile)
				//	log.Printf("scheduler mapTask i status = %v\n", c.mapTask[i].status)
				//}
				if c.mapTask[i].status == beginning {
					// c.mapTask[i].map_or_reduce = c.stage_map_or_reduce
					// c.mapTask[i].fileName = c.file[i]
					// c.mapTask[i].taskid = i
					// c.mapTask[i].map_or_reduce = 1
					c.mapTask[i].status = doing
					return &c.mapTask[i]
				}
			}
			task := Task{}
			task.status = -1
			return &task
		}
		c.stage_map_or_reduce = reducephase
	} //lse if c.stage_map_or_reduce == reducephase {
	if c.stage_map_or_reduce == reducephase {
		// for c.reduceFinished < c.reduceTaskSum {
		if c.reduceFinished < c.reduceTaskSum {
			for i := 0; i < c.reduceTaskSum; i++ {
				if c.reduceTask[i].status == beginning {
					// c.mapTask[i].map_or_reduce = c.stage_map_or_reduce
					// c.reduceTask[i].fileName = c.file[i]
					// c.reduceTask[i].taskid = i
					// c.reduceTask[i].map_or_reduce = reducephase
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
	//log.Printf("here1 !\n")
	rpc.Register(c)
	//log.Printf("here2 !\n")
	rpc.HandleHTTP()
	//log.Printf("here3 !\n")

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
	// fmt.Printf("stage: %d\n", c.stage_map_or_reduce)
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
	// log.Printf("here !\n")
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


//package mr
//
//
//type Coordinator struct {
//	// Your definitions here.
//
//}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}


//
// start a thread that listens for RPCs from worker.go
//
//func (c *Coordinator) server() {
//	rpc.Register(c)
//	rpc.HandleHTTP()
//	//l, e := net.Listen("tcp", ":1234")
//	sockname := coordinatorSock()
//	os.Remove(sockname)
//	l, e := net.Listen("unix", sockname)
//	if e != nil {
//		log.Fatal("listen error:", e)
//	}
//	go http.Serve(l, nil)
//}
//
////
//// main/mrcoordinator.go calls Done() periodically to find out
//// if the entire job has finished.
////
//func (c *Coordinator) Done() bool {
//	ret := false
//
//	// Your code here.
//
//
//	return ret
//}
//
////
//// create a Coordinator.
//// main/mrcoordinator.go calls this function.
//// nReduce is the number of reduce tasks to use.
////
//func MakeCoordinator(files []string, nReduce int) *Coordinator {
//	c := Coordinator{}
//
//	// Your code here.
//
//
//	c.server()
//	return &c
//}
