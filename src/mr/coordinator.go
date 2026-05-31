package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Coordinator struct {
	numMapTasks    int
	numReduceTasks int

	mapIdFile map[int]string
	mapFileId map[string]int

	remMapId    map[int]TaskStatus
	numMapRem   int
	mutexRemMap sync.Mutex

	remReduceId    map[int]TaskStatus
	numReduceRem   int
	mutexRemReduce sync.Mutex
}

// RPC handlers for the worker to call.

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	c.mutexRemMap.Lock()
	defer c.mutexRemMap.Unlock()
	c.mutexRemReduce.Lock()
	defer c.mutexRemReduce.Unlock()

	if c.numMapRem != 0 {
		for i := range c.numMapTasks {
			if c.remMapId[i] == Idle {
				c.remMapId[i] = InProgress
				reply.WorkId = 1
				reply.TaskId = i
				reply.Filename = c.mapIdFile[i]

				return nil
			}
		}
	}

	if c.numReduceRem != 0 {
		for i := range c.numReduceTasks {
			if c.remReduceId[i] == Idle {
				c.remReduceId[i] = InProgress
				reply.WorkId = 2
				reply.TaskId = i
				reply.Filename = ""

				return nil
			}
		}
	}

	reply.WorkId = 0
	return nil
}

func (c *Coordinator) SubmitWork(args *WorkDoneArgs, reply *WorkDoneReply) error {
	if args.WorkId == 1 {
		c.mutexRemMap.Lock()
		defer c.mutexRemMap.Unlock()

		c.remMapId[args.TaskId] = Completed

		return nil
	} else {
		c.mutexRemReduce.Lock()
		defer c.mutexRemReduce.Unlock()

		c.remReduceId[args.TaskId] = Completed

		return nil
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutexRemReduce.Lock()
	defer c.mutexRemReduce.Unlock()

	return c.numReduceRem == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{numMapTasks: len(files), numReduceTasks: nReduce, mapIdFile: make(map[int]string), mapFileId: make(map[string]int), remMapId: make(map[int]TaskStatus), numMapRem: len(files), remReduceId: make(map[int]TaskStatus), numReduceRem: nReduce}

	for i, filename := range files {
		c.mapIdFile[i] = filename
		c.mapFileId[filename] = i
		c.remMapId[i] = Idle
	}

	for i := range nReduce {
		c.remReduceId[i] = Idle
	}

	c.server(sockname)
	return &c
}
