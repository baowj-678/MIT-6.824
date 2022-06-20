package mr

import (
	"container/list"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	ReducersMutex    sync.RWMutex
	MappersMutex     sync.RWMutex
	WorkersMutex     sync.RWMutex
	Worker2TaskMutex sync.RWMutex
	MaxMutex         sync.RWMutex
	QueueMutex       sync.RWMutex
	StatusMutex      sync.RWMutex

	Max         int
	Reducers    map[int]int // ID to TaskID
	Mappers     map[int]int
	Workers     map[int]*WorkerStatus
	Worker2Task map[int]*Task
	nReducer    int       // The number of reducers
	files       []string  // input files
	Queue       list.List // waiting task Queue
	Status      CoordinatorStatus
	isLog       bool
}

type WorkerStatus struct {
	Status   string
	LastPing int64
}

// Your code here -- RPC handlers for the worker to call.

//
// Register receive workers RPC, calculate the workers' number
//
func (c *Coordinator) Register(args *RegisterRPC, reply *RegisterReply) error {
	workerID := 0
	c.MaxMutex.Lock()
	// get worker ID
	c.Max += 1
	workerID = c.Max
	c.MaxMutex.Unlock()

	// set worker status
	c.WorkersMutex.Lock()
	c.Workers[workerID] = &WorkerStatus{
		Status:   Free,
		LastPing: time.Now().Unix(),
	}
	c.WorkersMutex.Unlock()
	// set worker task

	c.Worker2TaskMutex.Lock()
	c.Worker2Task[workerID] = &Task{
		TaskID: -1,
		ID:     workerID,
	}
	c.Worker2TaskMutex.Unlock()

	if c.isLog {
		log.Println("Register workers-number:", len(c.Workers))
		//c.workerRWMutex.RUnlock()
	}

	// reply
	reply.ID = workerID
	reply.ReducerNum = c.nReducer
	if c.isLog {
		log.Println("Register Reply: nReducer: " + strconv.Itoa(reply.ReducerNum) + ", worker-id: " + strconv.Itoa(reply.ID))
	}
	return nil
}

//
// DistributeMapTask
// distribute map task
//
func (c *Coordinator) DistributeMapTask() error {
	if c.isLog {
		log.Println("Distribute Map Task: enter")
	}
	c.QueueMutex.RLock()
	queueLen := c.Queue.Len()
	c.QueueMutex.RUnlock()
	if queueLen == 0 {
		// queue is empty
		return nil
	} else {
		// queue is not empty
		c.WorkersMutex.RLock()
		workerLen := len(c.Workers)
		c.WorkersMutex.RUnlock()

		c.MappersMutex.RLock()
		mapperLen := len(c.Mappers)
		c.MappersMutex.RUnlock()

		if workerLen > mapperLen {
			//if c.isLog {
			//	log.Println("Distribute Map Task: start")
			//}

			// Attention for Dead Lock
			c.WorkersMutex.Lock()
			for key, value := range c.Workers {
				// distribute task
				c.Worker2TaskMutex.RLock()
				taskID := c.Worker2Task[key].TaskID
				c.Worker2TaskMutex.RUnlock()

				if value.Status == Free && taskID == -1 {
					// worker's task is empty
					c.Workers[key].Status = Map

					c.ReducersMutex.Lock()
					delete(c.Reducers, key)
					c.ReducersMutex.Unlock()

					c.MappersMutex.Lock()
					delete(c.Mappers, key)
					c.MappersMutex.Unlock()

					task := Task{}

					//////// atomic ///////
					c.QueueMutex.Lock()
					v := c.Queue.Front()
					if v == nil {
						c.QueueMutex.Unlock()
						break
					}
					task.TaskID = v.Value.(int)
					c.Queue.Remove(c.Queue.Front())
					c.QueueMutex.Unlock()
					///////////////////////

					c.MappersMutex.Lock()
					c.Mappers[key] = task.TaskID
					c.MappersMutex.Unlock()

					task.TaskStatus = Prepare
					task.ID = key
					task.TaskType = Map
					task.Key = c.files[task.TaskID]

					if c.isLog {
						log.Println("Distribute Map Task: worker-id:" + strconv.Itoa(task.ID) +
							",task-id:" + strconv.Itoa(task.TaskID) +
							",task-status:" + string(task.TaskStatus) +
							",task-type:" + string(task.TaskType))
					}

					c.Worker2TaskMutex.Lock()
					*c.Worker2Task[key] = task
					c.Worker2TaskMutex.Unlock()
				}
			}
			c.WorkersMutex.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) DistributeReduceTask() error {
	c.QueueMutex.RLock()
	queueLen := c.Queue.Len()
	c.QueueMutex.RUnlock()
	if queueLen == 0 {
		// queue is empty
		return nil
	} else {
		// queue is not empty
		c.WorkersMutex.RLock()
		workerLen := len(c.Workers)
		c.WorkersMutex.RUnlock()

		c.ReducersMutex.RLock()
		reducerLen := len(c.Reducers)
		c.ReducersMutex.RUnlock()

		if workerLen > reducerLen {
			if c.isLog {
				log.Println("Distribute Reduce task")
			}
			c.WorkersMutex.Lock()
			for key, value := range c.Workers {
				// distribute task
				c.Worker2TaskMutex.RLock()
				taskID := c.Worker2Task[key].TaskID
				c.Worker2TaskMutex.RUnlock()
				if value.Status == Free && taskID == -1 {
					c.Workers[key].Status = Reduce

					c.MappersMutex.Lock()
					delete(c.Mappers, key)
					c.MappersMutex.Unlock()

					c.ReducersMutex.Lock()
					delete(c.Reducers, key)
					c.ReducersMutex.Unlock()

					task := Task{}

					//////// atomic ///////
					c.QueueMutex.Lock()
					v := c.Queue.Front()
					if v == nil {
						c.QueueMutex.Unlock()
						break
					}
					task.TaskID = v.Value.(int)
					c.Queue.Remove(c.Queue.Front())
					c.QueueMutex.Unlock()
					///////////////////////

					c.ReducersMutex.Lock()
					c.Reducers[key] = task.TaskID
					c.ReducersMutex.Unlock()

					task.TaskStatus = Prepare
					task.ID = key
					task.TaskType = Reduce
					task.Key = ""
					if c.isLog {
						log.Println("Distribute Task: worker-id:" + strconv.Itoa(task.ID) +
							",task-id:" + strconv.Itoa(task.TaskID) +
							",task-status:" + string(task.TaskStatus) +
							",task-type:" + string(task.TaskType))
					}
					c.Worker2TaskMutex.Lock()
					*c.Worker2Task[key] = task
					c.Worker2TaskMutex.Unlock()
				}
			}
			c.WorkersMutex.Unlock()
		}
	}
	return nil
}

//
// TODO: Logout receive workers RPC, calculate the workers' number
//
func (c *Coordinator) Logout(args *LogoutRPC, reply *LogoutReply) error {
	return nil
}

//
// heart beat
//
func (c *Coordinator) GetHeartBeat(args *HeartBeat, reply *Task) error {
	c.WorkersMutex.Lock()
	workerStatus, ok := c.Workers[args.ID]
	if ok {
		workerStatus.Status = string(args.Status)
		workerStatus.LastPing = time.Now().Unix()
	}
	c.WorkersMutex.Unlock()

	if !ok {
		// not register
		reply.ID = -1
		reply.TaskID = -1
		reply.TaskStatus = None
		return nil
	}

	reply.TaskID = args.TaskID
	reply.TaskType = args.TaskType
	reply.ID = args.ID
	reply.TaskStatus = args.TaskStatus
	// worker has task
	if args.TaskID != -1 {
		if args.TaskStatus == Fail {
			// task failed
			if c.isLog {
				log.Println("Heart Beat Task Failed: worker-id:" + strconv.Itoa(args.ID) +
					",task-id:" + strconv.Itoa(args.TaskID) +
					",task-status:" + string(args.TaskStatus) +
					",task-type:" + string(args.TaskType))
			}
			reply.TaskID = -1

			c.QueueMutex.Lock()
			c.Queue.PushFront(args.TaskID)
			c.QueueMutex.Unlock()

			if args.TaskType == Reduce {
				c.ReducersMutex.Lock()
				delete(c.Reducers, args.ID)
				c.ReducersMutex.Unlock()
			} else if args.TaskType == Map {
				c.MappersMutex.Lock()
				delete(c.Mappers, args.ID)
				c.MappersMutex.Unlock()
			}

			c.WorkersMutex.Lock()
			c.Workers[args.ID].Status = Free
			c.WorkersMutex.Unlock()

			c.Worker2TaskMutex.Lock()
			c.Worker2Task[args.ID].TaskID = -1
			c.Worker2TaskMutex.Unlock()
		} else if args.TaskStatus == Success {
			// task is succeed
			if c.isLog {
				log.Println("Heart Beat Task Succeed: worker-id:" + strconv.Itoa(args.ID) +
					",task-id:" + strconv.Itoa(args.TaskID) +
					",task-status:" + string(args.TaskStatus) +
					",task-type:" + string(args.TaskType))
			}
			reply.TaskID = -1
			if args.TaskType == Reduce {
				c.ReducersMutex.Lock()
				delete(c.Reducers, args.ID)
				c.ReducersMutex.Unlock()
			} else if args.TaskType == Map {
				c.MappersMutex.Lock()
				delete(c.Mappers, args.ID)
				c.MappersMutex.Unlock()
			}
			c.WorkersMutex.Lock()
			c.Workers[args.ID].Status = Free
			c.WorkersMutex.Unlock()

			c.Worker2TaskMutex.Lock()
			c.Worker2Task[args.ID].TaskID = -1
			c.Worker2TaskMutex.Unlock()
		} else if args.TaskStatus == Work {
			if c.isLog {
				log.Println("Heart Beat Task Work: worker-id:" + strconv.Itoa(args.ID) +
					",task-id:" + strconv.Itoa(args.TaskID) +
					",task-status:" + string(args.TaskStatus) +
					",task-type:" + string(args.TaskType))
			}
			c.Worker2TaskMutex.Lock()
			c.Worker2Task[args.ID].TaskStatus = Work
			c.Worker2TaskMutex.Unlock()
		}
	} else {
		// worker has not task
		if args.Status == Free {
			c.Worker2TaskMutex.RLock()
			task, ok := c.Worker2Task[args.ID]
			if ok && task.TaskID != -1 {
				*reply = *task
				if c.isLog {
					log.Println("Heart Beat Reset Task: worker-id:" + strconv.Itoa(reply.ID) +
						",task-id:" + strconv.Itoa(reply.TaskID) +
						",task-status:" + string(reply.TaskStatus) +
						",task-type:" + string(reply.TaskType) +
						",task-key:" + reply.Key)
				}
			}
			c.Worker2TaskMutex.RUnlock()
		}
	}
	return nil
}

func (c *Coordinator) CheckCrash() {
	tmpTime := time.Now().Unix()
	workerIDs := list.List{}

	c.WorkersMutex.RLock()
	for key, value := range c.Workers {
		if (tmpTime - value.LastPing) >= 10 {
			workerIDs.PushBack(key)
		}
	}
	c.WorkersMutex.RUnlock()

	for front := workerIDs.Front(); front != nil; front = workerIDs.Front() {
		if c.isLog {
			log.Println("Daemon Crash: worker-id:" + strconv.Itoa(front.Value.(int)))
		}
		c.Worker2TaskMutex.RLock()
		task := Task{}
		task = *c.Worker2Task[front.Value.(int)]
		c.Worker2TaskMutex.RUnlock()

		c.QueueMutex.Lock()
		c.Queue.PushFront(task.TaskID)
		c.QueueMutex.Unlock()

		if task.TaskType == Reduce {
			c.WorkersMutex.Lock()
			c.ReducersMutex.Lock()
			delete(c.Workers, task.ID)
			delete(c.Reducers, task.ID)
			c.ReducersMutex.Unlock()
			c.WorkersMutex.Unlock()
		} else if task.TaskType == Map {
			c.WorkersMutex.Lock()
			c.MappersMutex.Lock()
			delete(c.Workers, task.ID)
			delete(c.Mappers, task.ID)
			c.MappersMutex.Unlock()
			c.WorkersMutex.Unlock()
		}

		c.Worker2TaskMutex.Lock()
		delete(c.Worker2Task, task.ID)
		c.Worker2TaskMutex.Unlock()

		workerIDs.Remove(workerIDs.Front())
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() error {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Println("listen error:", e)
	}
	go http.Serve(l, nil)
	return nil
}

//
// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.StatusMutex.RLock()
	status := c.Status
	c.StatusMutex.RUnlock()
	if c.isLog {
		log.Println("Main circle, status: " + status)
	}
	if status == Prepare {
		// finish prepare if there are more than 2 workers.
		c.WorkersMutex.RLock()
		workerLen := len(c.Workers)
		c.WorkersMutex.RUnlock()

		if workerLen >= 2 {
			c.StatusMutex.Lock()
			c.Status = Free
			c.StatusMutex.Unlock()
		}

	} else if status == Free {
		// if there are some tasks change to map
		if len(c.files) > 0 {
			c.StatusMutex.Lock()
			c.Status = Map
			c.StatusMutex.Unlock()
			c.PrepareMap()
		}
	} else if status == Map {
		c.QueueMutex.RLock()
		queueLen := c.Queue.Len()
		c.QueueMutex.RUnlock()

		c.MappersMutex.RLock()
		mapperLen := len(c.Mappers)
		c.MappersMutex.RUnlock()

		if queueLen == 0 && mapperLen == 0 {
			// finish mapper phase, change to reduce
			c.files = []string{}

			c.StatusMutex.Lock()
			c.Status = Reduce
			c.StatusMutex.Unlock()

			c.PrepareReduce()
		} else {
			c.DistributeMapTask()
		}
	} else if status == Reduce {
		c.QueueMutex.RLock()
		queueLen := c.Queue.Len()
		c.QueueMutex.RUnlock()

		c.ReducersMutex.RLock()
		reducerLen := len(c.Reducers)
		c.ReducersMutex.RUnlock()
		if queueLen == 0 && reducerLen == 0 {
			// finish reduce phase change to Free
			c.StatusMutex.Lock()
			c.Status = Free
			c.StatusMutex.Unlock()
			return true
		} else {
			c.DistributeReduceTask()
		}
	}
	// chech crash
	c.CheckCrash()
	return false
}

//
// PrepareMap do prepare work for Map, create map task Queue and mapper map
//
func (c *Coordinator) PrepareMap() {
	if c.isLog {
		log.Println("Prepare Map Phase")
	}
	// add task to the Queue, create mappers set
	c.QueueMutex.Lock()
	for i := range c.files {
		c.Queue.PushBack(i)
	}
	c.QueueMutex.Unlock()

	c.MappersMutex.Lock()
	c.Mappers = make(map[int]int)
	c.MappersMutex.Unlock()
}

func (c *Coordinator) PrepareReduce() {
	// add task to the Queue, create reducers set
	c.QueueMutex.Lock()
	for i := 0; i < c.nReducer; i++ {
		c.Queue.PushBack(i)
	}
	c.QueueMutex.Unlock()

	c.ReducersMutex.Lock()
	c.Reducers = make(map[int]int)
	c.ReducersMutex.Unlock()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		ReducersMutex:    sync.RWMutex{},
		MappersMutex:     sync.RWMutex{},
		WorkersMutex:     sync.RWMutex{},
		Worker2TaskMutex: sync.RWMutex{},
		MaxMutex:         sync.RWMutex{},
		QueueMutex:       sync.RWMutex{},
		StatusMutex:      sync.RWMutex{},

		Max:         -1,
		Reducers:    map[int]int{},
		Mappers:     map[int]int{},
		Workers:     map[int]*WorkerStatus{},
		Worker2Task: map[int]*Task{},
		nReducer:    nReduce,
		files:       files,
		Queue:       list.List{},
		Status:      CoordinatorStatus(Prepare),
		isLog:       false,
	}
	c.server()
	return &c
}
