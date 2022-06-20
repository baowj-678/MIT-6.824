package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type BaseWorker struct {
	TaskMutex   sync.RWMutex
	StatusMutex sync.RWMutex
	ID          int
	nReducer    int
	Task        Task
	Status      Status
	MapF        func(string, string) []KeyValue
	ReduceF     func(string, []string) string
	isLog       bool
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	worker := BaseWorker{
		Status:   Prepare,
		MapF:     mapf,
		ReduceF:  reducef,
		nReducer: 10,
		Task: Task{
			TaskID:     -1,
			TaskStatus: None,
		},
		isLog: false,
	}

	// heart beat
	go worker.DaemonThread()
	// main circle
	for {
		worker.StatusMutex.RLock()
		status := worker.Status
		worker.StatusMutex.RUnlock()
		if status == Free {
			// worker Free
			worker.Work()
		}
		time.Sleep(time.Second)
	}

}

func (s *BaseWorker) Register() bool {
	args := RegisterRPC{
		os.Getuid(), os.Getuid(),
	}
	reply := RegisterReply{}
	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		s.ID = reply.ID
		s.nReducer = reply.ReducerNum
		if s.isLog {
			log.Println("Register Succeed: worker-id: " + strconv.Itoa(s.ID) + ", nReducer: " + strconv.Itoa(s.nReducer))
		}
	} else {
		if s.isLog {
			log.Println("Register Failed")
		}
		return false
	}
	return true
}

func (s *BaseWorker) Logout() {
	args := LogoutRPC{
		s.ID,
	}
	reply := LogoutReply{}
	call("Coordinator.Logout", &args, &reply)
}

//
// DaemonThread executes every second
//
func (s *BaseWorker) DaemonThread() {
	if s.isLog {
		log.Println("Start Heart Beat")
	}
	for {
		s.StatusMutex.RLock()
		status := s.Status
		s.StatusMutex.RUnlock()
		if status == Prepare {
			if s.isLog {
				log.Println("Heart Beat: Register")
			}
			ok := s.Register()
			if ok {
				s.StatusMutex.Lock()
				s.Status = Free
				s.StatusMutex.Unlock()
			}
		} else {
			s.HeartBeat()
		}
		time.Sleep(time.Second)
	}
}

//
// HeartBeat
// Sent Heart Beat to Coordinator, including Worker info and Task info
//
func (s *BaseWorker) HeartBeat() {

	s.TaskMutex.RLock()
	args := HeartBeat{
		ID:         s.ID,
		TaskType:   s.Task.TaskType,
		TaskStatus: s.Task.TaskStatus,
		TaskID:     s.Task.TaskID,
	}
	s.TaskMutex.RUnlock()

	s.StatusMutex.RLock()
	args.Status = s.Status
	s.StatusMutex.RUnlock()
	if s.isLog {
		log.Println("Heart Beat Sent: worker-id:" + strconv.Itoa(args.ID) +
			",worker-status:" + string(args.Status) +
			",task-id:" + strconv.Itoa(args.TaskID) +
			",task-status:" + string(args.TaskStatus) +
			",task-type:" + string(args.TaskType))
	}
	reply := Task{}
	ok := call("Coordinator.GetHeartBeat", &args, &reply)
	if s.isLog {
		log.Println("Heart Beat Reply: worker-id:" + strconv.Itoa(args.ID) +
			",worker-status:" + string(args.Status) +
			",task-id:" + strconv.Itoa(args.TaskID) +
			",task-status:" + string(args.TaskStatus) +
			",task-type:" + string(args.TaskType))
	}
	if ok {
		if reply.ID == -1 {
			// not register
			s.ID = -1
			s.StatusMutex.Lock()
			s.Status = Prepare
			s.StatusMutex.Unlock()
		}
		s.TaskMutex.RLock()
		taskID := s.Task.TaskID
		s.TaskMutex.RUnlock()
		if reply.TaskID != taskID {
			if s.isLog {
				log.Println("Heart Beat Reset Task: worker-id:" + strconv.Itoa(reply.ID) +
					",task-id:" + strconv.Itoa(reply.TaskID) +
					",task-status:" + string(reply.TaskStatus) +
					",task-type:" + string(reply.TaskType))
			}
			s.TaskMutex.Lock()
			s.Task = reply
			if reply.TaskID == -1 {
				s.Task.TaskStatus = None
			} else {
				s.Task.TaskStatus = Prepare
			}
			s.TaskMutex.Unlock()
		}
	}
}

//
// MapperWork
// execute BaseWorker.MapF
//
func (s *BaseWorker) MapperWork() error {
	if s.isLog {
		log.Println("Start map work")
	}
	// load file
	file, err := os.Open(s.Task.Key)
	if err != nil {
		log.Fatalf("cannot open %v", s.Task.Key)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", s.Task.Key)
	}
	file.Close()
	// mapper func
	kva := s.MapF(s.Task.Key, string(content))
	// write file
	intermediate := make([][]KeyValue, s.nReducer)
	if s.isLog {
		log.Println("nReducer: " + strconv.Itoa(s.nReducer))
	}
	for _, kv := range kva {
		idx := ihash(kv.Key) % s.nReducer
		intermediate[idx] = append(intermediate[idx], kv)
	}
	intermediateBaseName := "mr-intermediate-"
	for key, kv := range intermediate {
		// reduceID-mapID
		name := intermediateBaseName + strconv.Itoa(key) + "-" + strconv.Itoa(s.Task.TaskID)
		err := s.WriteKeyValue(name, kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *BaseWorker) WriteKeyValue(name string, kva []KeyValue) error {
	if s.isLog {
		log.Println("Write file: " + name)
	}
	ofile, _ := os.Create(name)
	for i := 0; i < len(kva); i++ {
		_, err := fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, kva[i].Value)
		if err != nil {
			return err
		}
	}
	err := ofile.Close()
	if err != nil {
		return err
	}
	return nil
}

//
// ReduceWork
// execute BaseWorker.ReduceF
//
func (s *BaseWorker) ReduceWork() error {
	if s.isLog {
		log.Println("Start reduce work")
	}
	// get file names
	intermediateFileNames, err := s.getIntermediateFileNames("./", "mr-intermediate-"+strconv.Itoa(s.Task.TaskID))
	if err != nil {
		return err
	}
	// load data
	intermediate := map[string]*[]string{}
	for _, name := range intermediateFileNames {
		// load file
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open %v", name)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", name)
		}
		err = file.Close()
		if err != nil {
			return err
		}
		// transform data
		lines := strings.Split(string(content), "\n")
		for _, line := range lines[:len(lines)-1] {
			kv := strings.Split(line, " ")
			if v, ok := intermediate[kv[0]]; ok {
				*v = append(*v, kv[1])
			} else {
				intermediate[kv[0]] = &[]string{kv[1]}
			}
		}
	}
	// reduce func
	outputs := []KeyValue{}
	for key, values := range intermediate {
		output := s.ReduceF(key, *values)
		outputs = append(outputs, KeyValue{key, output})
	}
	// write file
	outputName := "mr-out" + "-" + strconv.Itoa(s.Task.TaskID)
	err = s.WriteKeyValue(outputName, outputs)
	if err != nil {
		return err
	}
	return nil
}

func (s *BaseWorker) getIntermediateFileNames(dir string, prefix string) (names []string, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("cannot open %v", dir)
		return nil, err
	}
	intermediateFileNames := []string{}
	for _, file := range files {
		if !file.IsDir() {
			if len(file.Name()) > len(prefix) && file.Name()[:len(prefix)] == prefix {
				intermediateFileNames = append(intermediateFileNames, dir+file.Name())
			}
		}
	}
	return intermediateFileNames, nil
}

//
// Work
// this func is executed where BaseWorker.Status is Free
//
func (s *BaseWorker) Work() {
	if s.isLog {
		s.TaskMutex.RLock()
		taskStatus := s.Task.TaskStatus
		s.TaskMutex.RUnlock()

		s.StatusMutex.RLock()
		status := s.Status
		s.StatusMutex.RUnlock()
		log.Println("Work circle, task-status: " + string(taskStatus) + ", worker-status: " + string(status))

	}
	// start work
	s.TaskMutex.RLock()
	taskID := s.Task.TaskID
	taskStatus := s.Task.TaskStatus
	taskType := s.Task.TaskType
	s.TaskMutex.RUnlock()
	if taskID != -1 {
		// task is preparing
		if taskStatus == Prepare {
			s.TaskMutex.Lock()
			s.Task.TaskStatus = Work
			s.TaskMutex.Unlock()

			s.StatusMutex.Lock()
			s.Status = Work
			s.StatusMutex.Unlock()
			// start work
			var err error
			if taskType == Map {
				err = s.MapperWork()
			} else if taskType == Reduce {
				err = s.ReduceWork()
			}

			if err == nil {
				// success
				s.StatusMutex.Lock()
				s.Status = Free
				s.StatusMutex.Unlock()

				s.TaskMutex.Lock()
				s.Task.TaskStatus = Success
				s.TaskMutex.Unlock()
			} else {
				// fail
				s.StatusMutex.Lock()
				s.Status = Free
				s.StatusMutex.Unlock()

				s.TaskMutex.Lock()
				s.Task.TaskStatus = Fail
				s.TaskMutex.Unlock()
			}
		}
	}
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
	return false
}
