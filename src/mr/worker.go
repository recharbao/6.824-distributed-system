package mr

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "sort"
import "encoding/json"

var   (

	doTaskLogFileName = flag.String("log1", "doTask.log", "Log file name")
	interdiFilelogName = flag.String("log3", "inter.log", "Log file name")

	)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workid int
	workerStatus int

	mapTaskSum int
	reduceTaskSum int

	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.workerStatus = 1

	w.register()
	//fmt.Printf("here !")
	w.doTask()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}


func (w *worker)register() {
	args := WorkerArgs{}
	args.WorkerStatus = w.workerStatus
	reply := WorkerReply{}
	call("Coordinator.Register", &args, &reply)
	w.mapTaskSum = reply.MapTaskSum
	w.reduceTaskSum = reply.ReduceTaskSum
	// fmt.Printf("register mapTaskSum = %v\n", reply.MapTaskSum)
	w.workid = reply.Workid
	// fmt.Printf("here !")
}

func reportTaskStatus(taskid int, mapOrReduce int) {
	//fmt.Printf("herhe !\n")
	args := TaskArgs{}
	args.Taskid = taskid
	args.Status = doing
	args.MapOrReduce = mapOrReduce
	reply := TaskReply{}
	call("Coordinator.ReportTaskState", &args, &reply)
}


func  requestTask() (TaskReply, bool) {
	// fmt.Printf("here !\n")
	args := TaskArgs{}
	reply:= TaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	//log.Printf("here !")
	return reply, ok
}

func (w *worker)doTask() {
	for {
		// fmt.Printf("here !\n")
		task, ok := requestTask()
		{
			logFile, logErr := os.OpenFile(*doTaskLogFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
			if logErr != nil{
				fmt.Printf("log error !\n")
			}

			log.SetOutput(logFile)
			log.SetFlags(log.Ldate | log.Lmicroseconds | log.Ltime | log.Lshortfile)
			log.Printf("task file = %v\n", task.FileName)
			log.Printf("dotask  MapOrReduce = %v\n", task.MapOrReduce)
		}

		if !ok {
			log.Printf("fail to requestTask!\n")
		}
		if task.Status == -1 {
			fmt.Printf("wait next requestTask !\n")
			continue
		}

		fmt.Printf("dotest task_id = %v\n", task.Taskid)
		fmt.Printf("dotest task_Status = %v\n", task.Status)

		fmt.Printf("task test end = %v\n", task.MapOrReduce)
		if task.MapOrReduce == end {
			fmt.Printf("return worker !\n")
			return
		}

		fmt.Printf("w.reduceTaskSum = %v\n", w.reduceTaskSum)
		//log.Printf("here !\n")
		if task.MapOrReduce == mapphase {
			doMap(w.mapf, task.FileName, task.Taskid, w.reduceTaskSum)
			reportTaskStatus(task.Taskid, mapphase)
		}else if task.MapOrReduce == reducephase {
			doReduce(w.reducef, task.FileName, task.Taskid, w.mapTaskSum)
			reportTaskStatus(task.Taskid, reducephase)
		}
		time.Sleep(100)
	}
}



// do map and reduce with all- to - all
func doMap(mapf func(string, string) []KeyValue, inputFile string, mapTaskNumber int, reduceTaskSum int){
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("can not open %v\n", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("doMap ReadAll error\n")
	}
	file.Close()

	// use mapf
	kva := mapf(inputFile, string(content))

	// create outputfile
	outputFile := make([] *os.File, reduceTaskSum)
	// create jsonEncoder
	outputJsonEd := make([] *json.Encoder, reduceTaskSum)
	for i := 0; i < reduceTaskSum; i++ {
		// The most common numeric conversions are Atoi (string to int) and Itoa (int to string).
		intermediateFileName := "mr" + strconv.Itoa(mapTaskNumber) + "-" + strconv.Itoa(i)
		outputFile[i], err = os.Create(intermediateFileName)
		if err != nil {
			fmt.Printf("create file error !")
		}
		//An Encoder writes JSON values to an output stream.
		//NewEncoder returns a new encoder that writes to w.
		outputJsonEd[i] = json.NewEncoder(outputFile[i])
	}

	for _, kv := range kva{
		// hash
		index := ihash(kv.Key) % reduceTaskSum
		outputJsonEd[index].Encode(&kv)
	}

	// close file
	for i := 0; i < reduceTaskSum; i++ {
		outputFile[i].Close()
	}
}


func doReduce(reducef func(string, []string) string, outputFile string, reduceTaskNumber int, mapTaskSum int) {
	file, err := os.Create(outputFile)

	// jsonOut := json.NewEncoder(file)

	if err != nil{
		fmt.Printf("can not open %v", outputFile)
	}
	intermediateFile := make([] *os.File, mapTaskSum)

	// read intermediateFile
	for i := 0; i < mapTaskSum; i++ {
		intermediateFileName := "mr" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTaskNumber)
		intermediateFile[i], err = os.Open(intermediateFileName)
		if err != nil {
			fmt.Printf("doReudce can not open %v", intermediateFileName)
		}

		{
			logFile, logErr := os.OpenFile(*interdiFilelogName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
			if logErr != nil{
				fmt.Printf("log error !\n")
			}

			log.SetOutput(logFile)
			log.SetFlags(log.Ldate | log.Lmicroseconds | log.Ltime | log.Lshortfile)
			log.Printf("intermeidateFileName = %q\n", intermediateFileName)
			log.Printf("err = %d\n", err)
		}
	}

	// decoder json
	kva := []KeyValue{}
	for i := 0; i < mapTaskSum; i++ {
		dec := json.NewDecoder(intermediateFile[i])
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// steal from mrsequential.go
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		out := kva[i].Key + " " + reducef(kva[i].Key, values) + "\n"
		// jsonOut.Encode(&out)
		file.Write([]byte(out))
		i = j
	}
	file.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

	// declare an argument structure.
	// args := ExampleArgs{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	// reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//log.Printf("here !\n")
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

//func (w *worker) WorkerScheduler() {
//	if w.map_or_reduce == 1 {
//		doMap()
//	}else {
//		doReduce()
//	}
//	call()
//}

