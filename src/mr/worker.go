package mr

import (
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strings"
)
import "log"
import "hash/fnv"

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		var taskInfo TaskInfo

		call("register", nil, &taskInfo)

		if taskInfo.done {
			return
		}

		handleJob(&taskInfo, mapf, reducef)
	}
}

func handleJob(info *TaskInfo, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	switch info.jobPhase {
	case "Map":
		doMap(info, mapf)
		break
	case "Reduce":
		doReduce(info, reducef)
		break
	default:
		call("Error", &info, nil)
		log.Fatal("Unknown job")
	}
}

func doMap(info *TaskInfo, mapf func(string, string) []KeyValue) {

	kva := mapf(info.filename, string(readFile(info.filename)))

	intermediate := make([][]KeyValue, info.nReduce)

	for i := 0; i < len(kva); i++ {
		intermediate[ihash(kva[i].Key)%info.nReduce] = append(intermediate[ihash(kva[i].Key)%info.nReduce], kva[i])
	}

	for i := 0; i < info.nReduce; i++ {
		writeFile(fmt.Sprintf("mr-tmp-%d", i+1), intermediate[i])
	}

	call("done", info, nil)
}

func doReduce(info *TaskInfo, reducef func(string, []string) string) {
	content := strings.Split(string(readFile(info.filename)), "\n")

	intermediate := make([]KeyValue, len(content))

	for _, kvString := range content {
		kvList := strings.Split(kvString, " ")
		kv := KeyValue{kvList[0], kvList[1]}
		intermediate = append(intermediate, kv)
	}

	sort.Sort(ByKey(intermediate))

	result := []KeyValue{}

	for i := 0; i < len(intermediate); i++ {
		values := []string{}
		kv := intermediate[i]

		for j := 0; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {
			values = append(values, intermediate[j].Value)
		}

		result = append(result, KeyValue{kv.Key, reducef(intermediate[i].Key, values)})
	}

	writeFile(fmt.Sprintf("mr-out-%d", info.id), result)

	call("done", info, nil)
}

func readFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return content
}

func writeFile(fileName string, data []KeyValue) {
	ofile, _ := os.Create(fileName)

	for i := 0; i < len(data); i++ {
		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, data[i])
	}

	ofile.Close()
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
