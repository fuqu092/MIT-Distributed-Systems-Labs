package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string   // socket for coordinator
var intermediateDir string // directory for reading and writing intermediate files
var finalDir string        // directory for storing final files

func GetWork() (GetWorkReply, error) {
	args := GetWorkArgs{}
	reply := GetWorkReply{}

	err := call("Coordinator.GetWork", &args, &reply)

	return reply, err
}

func SubmitWork(workId int, taskId int) error {
	args := WorkDoneArgs{}
	reply := WorkDoneReply{}

	args.WorkId = workId
	args.TaskId = taskId

	err := call("Coordinator.SubmitWork", &args, &reply)

	return err
}

// func doMapWork(taskId int, filename string)

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Println("Dialing:", err)
		return err
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return err
	}
	log.Println(os.Getpid(), ": Call failed err", err)
	return err
}

func doMapWork(work GetWorkReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(work.Filename)
	if err != nil {
		log.Println("Cannot open", work.Filename)
		return err
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Println("Cannot read", work.Filename)
		return err
	}

	intermediate := mapf(work.Filename, string(content))
	temp := make([][]KeyValue, work.NumReduceTasks)

	for _, kv := range intermediate {
		reduceTaskNum := ihash(kv.Key) % work.NumReduceTasks
		temp[reduceTaskNum] = append(temp[reduceTaskNum], kv)
	}

	for i, val := range temp {
		f, err := os.CreateTemp(intermediateDir, "temp-*.json")
		if err != nil {
			log.Println("Can not create temp file", err)
			return err
		}

		defer os.Remove(f.Name())
		encoder := json.NewEncoder(f)
		encoder.SetIndent("", "  ")
		newFilename := fmt.Sprintf("mr-%d-%d.json", work.TaskId, i)

		if err := encoder.Encode(val); err != nil {
			log.Println("Can not write to file", newFilename, err)
			return err
		}

		err = f.Close()
		if err != nil {
			log.Println("Error closing file", newFilename, err)
		}

		err = os.Rename(f.Name(), filepath.Join(intermediateDir, newFilename))
		if err != nil {
			log.Println("Error renaming file", newFilename, err)
			return err
		}
	}

	err = SubmitWork(work.WorkId, work.TaskId)
	if err != nil {
		log.Println("Error submitting map work", work.TaskId, err)
		return err
	}

	log.Println("Completed map work for", work.Filename)

	return nil
}

func doReduceWork(work GetWorkReply, reducef func(string, []string) string) error {
	var intermediate []KeyValue

	for i := 0; i < work.NumMapTasks; i++ {
		filename := filepath.Join(intermediateDir, fmt.Sprintf("mr-%d-%d.json", i, work.TaskId))
		f, err := os.Open(filename)
		if err != nil {
			log.Println("Error opening file:", filename)
		}

		f.Seek(0, 0)
		decoder := json.NewDecoder(f)
		var temp []KeyValue
		err = decoder.Decode(&temp)
		if err != nil {
			log.Println("Error reading file:", filename)
			return nil
		}

		intermediate = append(intermediate, temp...)
	}

	sort.Sort(ByKey(intermediate))

	f, err := os.CreateTemp(finalDir, "temp-*.txt")
	if err != nil {
		log.Println("Can not create temp file for reduce task", work.TaskId, err)
		return err
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	outFile := fmt.Sprintf("mr-out-%d", work.TaskId)
	err = os.Rename(f.Name(), filepath.Join(finalDir, outFile))
	if err != nil {
		log.Println("Error renaming file for reduce task", work.TaskId, err)
		return err
	}

	err = SubmitWork(work.WorkId, work.TaskId)
	if err != nil {
		log.Println("Error submitting reduce work", work.TaskId, err)
		return err
	}

	log.Println("Completed reduce work for", work.TaskId)

	return nil
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	dir, err := os.Getwd()
	if err != nil {
		log.Println("Cannot get directory", err)
		return
	}
	intermediateDir = filepath.Join(dir, "..", "IntermediateFiles")
	finalDir = dir

	for {
		work, err := GetWork()

		if err != nil {
			break
		}

		switch work.WorkId {
		case 0:
			log.Println("NO WORK")
			time.Sleep(2 * time.Second)
		case 1:
			log.Println("Got map work", work.TaskId)
			err := doMapWork(work, mapf)
			if err != nil {
				log.Println("Can't complete map work", err)
			}
		case 2:
			log.Println("Got reduce work", work.TaskId)
			err := doReduceWork(work, reducef)
			if err != nil {
				log.Println("Can't complete reduce work", err)
			}
		}
	}
}
