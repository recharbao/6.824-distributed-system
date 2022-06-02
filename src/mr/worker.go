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
	w.workid = reply.Workid
}

func reportTaskStatus(taskid int, mapOrReduce int) {
	args := TaskArgs{}
	args.Taskid = taskid
	args.Status = doing
	args.MapOrReduce = mapOrReduce
	reply := TaskReply{}
	call("Coordinator.ReportTaskState", &args, &reply)
}


func  requestTask() (TaskReply, bool) {
	args := TaskArgs{}
	reply:= TaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	return reply, ok
}

func (w *worker)doTask() {
	for {
		task, ok := requestTask()
	
		if !ok {
			log.Printf("fail to requestTask!\n")
		}
		if task.Status == -1 {
			fmt.Printf("wait next requestTask !\n")
			continue
		}

		if task.MapOrReduce == end {
			fmt.Printf("return worker !\n")
			return
		}

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

