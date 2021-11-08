package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	nReduce      int
	jobIndex     int
	jobList      []*JobInfo
	finishedJobs map[JobKey]bool
	jobNum       int
	jobPhase     string
	finished     bool
	mux          sync.Mutex
}

type JobKey string

type JobInfo struct {
	id        int
	key       string
	start     int
	hasBackup bool
}

var id = 0

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (coordinator *Coordinator) Register(_ Empty, reply *TaskInfo) error {
	coordinator.mux.Lock()

	reply.Assigned = true
	reply.Working = true

	if coordinator.finished {
		reply.Working = false
	} else if coordinator.jobIndex < len(coordinator.jobList) {
		reply.NReduce = coordinator.nReduce
		reply.FileName = coordinator.jobList[coordinator.jobIndex].key
		reply.JobPhase = coordinator.jobPhase
		reply.ID = id

		coordinator.jobList[coordinator.jobIndex].start = int(time.Now().Unix())
		coordinator.jobList[coordinator.jobIndex].id = reply.ID
		coordinator.jobIndex += 1
		id += 1
	} else {
		reply.Assigned = false
	}

	defer coordinator.mux.Unlock()

	return nil

}

func (coordinator *Coordinator) Finished(args TaskInfo, _ *Empty) error {

	coordinator.mux.Lock()

	coordinator.finishedJobs[JobKey(args.FileName)] = true

	// when all the jobs are finished, switch to the next job phase
	if len(coordinator.finishedJobs) == coordinator.jobNum {
		var jobList []string
		if coordinator.jobPhase == "Map" {
			jobList = getIntermediateFiles(coordinator.nReduce)
		}

		coordinator.init(jobList, coordinator.nReduce)
	}

	defer coordinator.mux.Unlock()

	return nil
}

func (coordinator *Coordinator) init(files []string, nReduce int) {
	coordinator.jobList = []*JobInfo{}

	for _, file := range files {
		coordinator.jobList = append(coordinator.jobList, &JobInfo{key: file})
	}

	coordinator.jobPhase = nextJobPhase(coordinator.jobPhase)
	coordinator.jobIndex = 0
	coordinator.finishedJobs = map[JobKey]bool{}
	coordinator.nReduce = nReduce
	coordinator.finished = coordinator.jobPhase == "Finished"
	coordinator.jobNum = len(coordinator.jobList)
}

func (coordinator *Coordinator) checkJobs() {
	for {
		time.Sleep(10 * time.Second)

		coordinator.mux.Lock()

		var executingJobs []*JobInfo
		var backupJobs []*JobInfo

		for _, job := range coordinator.jobList {
			if !coordinator.finishedJobs[JobKey(job.key)] {
				executingJobs = append(executingJobs, job)
				if !job.hasBackup && int(time.Now().Unix())-job.start >= 10 {
					job.hasBackup = true
					backupJobs = append(backupJobs, &JobInfo{key: job.key})
				}
			}
		}

		coordinator.jobIndex = len(executingJobs)
		coordinator.jobList = append(executingJobs, backupJobs...)

		coordinator.mux.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (coordinator *Coordinator) server() {
	rpc.Register(coordinator)
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
func (coordinator *Coordinator) Done() bool {
	coordinator.mux.Lock()
	defer coordinator.mux.Unlock()

	return coordinator.finished
}

func getIntermediateFiles(nReduce int) []string {
	var files []string

	for i := 0; i < nReduce; i++ {
		files = append(files, fmt.Sprintf("mr-tmp-%v", i))
	}

	return files
}

func nextJobPhase(currentJobPhase string) string {
	if currentJobPhase == "" {
		return "Map"
	} else if currentJobPhase == "Map" {
		return "Reduce"
	} else {
		return "Finished"
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	gob.Register(TaskInfo{})
	gob.Register(Empty{})

	c := new(Coordinator)

	c.init(files, nReduce)

	go c.checkJobs()

	fmt.Println("inited")

	// Your code here.

	c.server()
	return c
}
