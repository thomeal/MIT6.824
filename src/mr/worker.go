package mr

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
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
	gob.Register(TaskInfo{})
	gob.Register(Empty{})

	for {
		var taskInfo TaskInfo

		call("Register", *new(Empty), &taskInfo)

		if !taskInfo.Working {
			return
		}

		if taskInfo.Assigned {
			handleJob(taskInfo, mapf, reducef)
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func handleJob(info TaskInfo, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	switch info.JobPhase {
	case "Map":
		doMap(info, mapf)
		break
	case "Reduce":
		doReduce(info, reducef)
		break
	default:
		//call("Error", &info, nil)
		log.Fatal("Unknown job: " + info.JobPhase)
	}
}

func doMap(info TaskInfo, mapf func(string, string) []KeyValue) {

	kva := mapf(info.FileName, string(readFile(info.FileName)))

	intermediate := make([][]KeyValue, info.NReduce)

	for i := 0; i < len(kva); i++ {
		intermediate[ihash(kva[i].Key)%info.NReduce] = append(intermediate[ihash(kva[i].Key)%info.NReduce], kva[i])
	}

	for i := 0; i < info.NReduce; i++ {
		writeFile(fmt.Sprintf("mr-tmp-%d-%d", i, info.ID), intermediate[i])
	}

	call("Finished", info, new(Empty))
}

func doReduce(info TaskInfo, reducef func(string, []string) string) {
	content := strings.Split(readIntermediateFiles(info.ID), "\n")

	var intermediate []KeyValue

	for _, kvString := range content {
		kvList := strings.Split(kvString, " ")

		if len(kvList) >= 2 {
			kv := KeyValue{kvList[0], kvList[1]}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	var result []KeyValue

	for i := 0; i < len(intermediate); i++ {
		var values []string
		kv := intermediate[i]

		j := i

		for ; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {
			values = append(values, intermediate[j].Value)
		}

		result = append(result, KeyValue{kv.Key, reducef(intermediate[i].Key, values)})

		i = j - 1
	}

	writeFile(fmt.Sprintf("mr-out-%d", info.ID), result)

	call("Finished", &info, new(Empty))
}

func readIntermediateFiles(id int) string {
	files, _ := ioutil.ReadDir("./")

	content := ""

	for _, file := range files {
		if strings.Contains(file.Name(), "mr-tmp-"+strconv.Itoa(id)) {
			content += string(readFile(file.Name()))
		}
	}

	return content
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
		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, data[i].Value)
	}

	ofile.Close()
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call("Coordinator."+rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
