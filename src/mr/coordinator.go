package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	mapIdFile      map[int]string

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
				reply.NumMapTasks = c.numMapTasks
				reply.NumReduceTasks = c.numReduceTasks
				reply.Filename = c.mapIdFile[i]

				go c.monitorTask(1, i)

				return nil
			}
		}
	} else if c.numReduceRem != 0 {
		for i := range c.numReduceTasks {
			if c.remReduceId[i] == Idle {
				c.remReduceId[i] = InProgress
				reply.WorkId = 2
				reply.TaskId = i
				reply.NumMapTasks = c.numMapTasks
				reply.NumReduceTasks = c.numReduceTasks
				reply.Filename = ""

				go c.monitorTask(2, i)

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

		if c.remMapId[args.TaskId] == Completed {
			return nil
		}

		c.remMapId[args.TaskId] = Completed
		c.numMapRem--

		log.Println("Got map work for", args.TaskId)

		return nil
	} else {
		c.mutexRemReduce.Lock()
		defer c.mutexRemReduce.Unlock()

		if c.remReduceId[args.TaskId] == Completed {
			return nil
		}

		c.remReduceId[args.TaskId] = Completed
		c.numReduceRem--

		log.Println("Got reduce work for", args.TaskId)

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
		log.Fatalln("Listen error", sockname, ":", e)
	}
	fmt.Println("Server started.")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutexRemReduce.Lock()
	defer c.mutexRemReduce.Unlock()

	if c.numReduceRem == 0 {
		os.RemoveAll(intermediateDir)
	}

	return c.numReduceRem == 0
}

func (c *Coordinator) monitorTask(workId int, taskId int) {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		if workId == 1 {
			c.mutexRemMap.Lock()

			if c.remMapId[taskId] == Completed {
				c.mutexRemMap.Unlock()
				ticker.Stop()
				return
			}
			c.remMapId[taskId] = Idle
			c.mutexRemMap.Unlock()
		} else {
			c.mutexRemReduce.Lock()

			if c.remReduceId[taskId] == Completed {
				c.mutexRemReduce.Unlock()
				ticker.Stop()
				return
			}
			c.remReduceId[taskId] = Idle
			c.mutexRemReduce.Unlock()
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{numMapTasks: len(files), numReduceTasks: nReduce, mapIdFile: make(map[int]string), remMapId: make(map[int]TaskStatus), numMapRem: len(files), remReduceId: make(map[int]TaskStatus), numReduceRem: nReduce}

	for i, filename := range files {
		c.mapIdFile[i] = filename
		c.remMapId[i] = Idle
	}

	dir, err := os.Getwd()
	if err != nil {
		log.Println("Cannot get directory", err)
		return nil
	}
	intermediateDir = filepath.Join(dir, "IntermediateFiles")
	os.Mkdir(intermediateDir, 0777)

	for i := range nReduce {
		c.remReduceId[i] = Idle
	}

	c.server(sockname)
	return &c
}
