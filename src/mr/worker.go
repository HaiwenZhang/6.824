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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[i], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}

func getIntermediateFile(mapTaskN int, reduceTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, reduceTaskN)
}

func finalizeIntermediateFile(tmpFile string, mapTaskN int, reduceTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, reduceTaskN)
	os.Rename(tmpFile, finalFile)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {

		replyArgs := CallAskTask()

		switch replyArgs.TaskType {
		case Map:
			doMapTask(mapf, replyArgs)
		case Reduce:
			doReduceTask(reducef, replyArgs)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %d", replyArgs.TaskType)
		}

		finishedTaskArgs := FinishedTaskArgs{
			TaskType: replyArgs.TaskType,
			TaskNum:  replyArgs.TaskNum,
		}

		CallTaskDone(&finishedTaskArgs)
	}
}

func doMapTask(mapf func(string, string) []KeyValue, reply *GetTaskReply) {
	file, err := os.Open(reply.MapFile)

	if err != nil {
		log.Fatalf("Cannot open %v", reply.MapFile)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("Cannot read %v", reply.MapFile)
	}
	file.Close()

	kva := mapf(reply.MapFile, string(content))

	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	encoders := []*json.Encoder{}

	for r := 0; r < reply.NReduceTasks; r++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("Cannot open tempfile")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilename := tmpFile.Name()
		tmpFilenames = append(tmpFilenames, tmpFilename)

		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduceTasks
		encoders[r].Encode(&kv)
	}

	for _, f := range tmpFiles {
		f.Close()
	}

	for r := 0; r < reply.NReduceTasks; r++ {
		finalizeIntermediateFile(tmpFilenames[r], reply.TaskNum, r)
	}
}

func doReduceTask(reducef func(string, []string) string, reply *GetTaskReply) {
	kva := []KeyValue{}

	for m := 0; m < reply.NMapTasks; m++ {
		iFileName := getIntermediateFile(m, reply.TaskNum)
		file, err := os.Open(iFileName)
		if err != nil {
			log.Fatalf("Cannot open %v", iFileName)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(kva))
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("Cannot open tmpfile")
	}

	tmpFilename := tmpFile.Name()

	key_begin := 0

	for key_begin < len(kva) {
		key_end := key_begin + 1

		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}
		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[key_begin].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)

		key_begin = key_end
	}

	finalizeReduceFile(tmpFilename, reply.TaskNum)
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

func CallAskTask() *GetTaskReply {

	args := GetTaskArgs{}

	reply := GetTaskReply{}

	call("Coordinator.HandleGetTask", &args, &reply)

	return &reply
}

func CallTaskDone(args *FinishedTaskArgs) {
	reply := FinshedTaskReply{}
	call("Coordinator.HandleFinishedTask", args, &reply)
	fmt.Printf("Task Done status %v\n", args.TaskNum)
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
