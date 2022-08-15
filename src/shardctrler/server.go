package shardctrler

import (
	"6.824/raft"
	"log"
	"math"
	"sort"
	"strconv"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	killed               bool
	configs              []Config // indexed by config num
	replyChMap           map[string]chan *Config
	clientLastCommandIdx map[string]int
}

type Op struct {
	OpType     string
	OpArgs     interface{}
	ClientID   string
	CommandIdx int
	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("Join(%v): args(%v)\n", sc.me, args)
	op := Op{
		OpType:     "Join",
		OpArgs:     *args,
		ClientID:   args.ClientID,
		CommandIdx: args.CommandIdx,
	}
	isLeader, index, ch := sc.RPCHandler(&op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	tmp := sc.GetReply(index, ch, "Join").(JoinReply)
	reply.Err = tmp.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("Leave(%v): args(%v)\n", sc.me, args)
	op := Op{
		OpType:     "Leave",
		OpArgs:     *args,
		ClientID:   args.ClientID,
		CommandIdx: args.CommandIdx,
	}
	isLeader, index, ch := sc.RPCHandler(&op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	tmp := sc.GetReply(index, ch, "Leave").(LeaveReply)
	reply.Err = tmp.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("Move(%v): args(%v)\n", sc.me, args)
	// Your code here.
	op := Op{
		OpType:     "Move",
		OpArgs:     *args,
		ClientID:   args.ClientID,
		CommandIdx: args.CommandIdx,
	}
	isLeader, index, ch := sc.RPCHandler(&op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	tmp := sc.GetReply(index, ch, "Move").(MoveReply)
	reply.Err = tmp.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("Query(%v), args(%v)\n", sc.me, args)
	op := Op{
		OpType:     "Query",
		OpArgs:     *args,
		ClientID:   args.ClientID,
		CommandIdx: args.CommandIdx,
	}
	isLeader, index, ch := sc.RPCHandler(&op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	tmp := sc.GetReply(index, ch, "Query").(QueryReply)
	reply.Err = tmp.Err
	reply.Config = tmp.Config
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.rf.Kill()
	sc.killed = true
	// Your code here, if desired.
}

func (sc *ShardCtrler) Killed() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.killed
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) OpHandler(args interface{}) *Config {
	op := args.(Op)
	configNew := Config{
		Num:    0,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	// check command idx increase
	if id, ok := sc.clientLastCommandIdx[op.ClientID]; ok {
		if id < op.CommandIdx {
			sc.clientLastCommandIdx[op.ClientID] = op.CommandIdx
		} else {
			return nil
		}
	} else {
		sc.clientLastCommandIdx[op.ClientID] = op.CommandIdx
	}
	var configLatest *Config
	// init
	configLatest = &sc.configs[len(sc.configs)-1]
	// set new config num
	configNew.Num = configLatest.Num + 1
	// copy old replica groups
	sc.mapCopy(configLatest.Groups, configNew.Groups)
	// copy old shards
	configNew.Shards = configLatest.Shards

	switch op.OpType {
	case "Join":
		joinArgs := op.OpArgs.(JoinArgs)
		DPrintf("OpHandler(%v): Join, args(%v)\n", sc.me, joinArgs)
		keys := make([]int, len(joinArgs.Servers))
		tmp := 0
		for key, _ := range joinArgs.Servers {
			keys[tmp] = key
			tmp++
		}
		sort.Ints(keys)
		for _, key := range keys {
			value := joinArgs.Servers[key]
			servers := make([]string, len(value))
			copy(servers, value)
			configNew.Groups[key] = servers
			sc.reBalance(&configNew, "Join", key)
		}
		sc.configs = append(sc.configs, configNew)
		return nil
	case "Leave":
		leaveArgs := op.OpArgs.(LeaveArgs)
		DPrintf("OpHandler(%v): Leave, args(%v)\n", sc.me, leaveArgs)
		// remove gid
		sort.Ints(leaveArgs.GIDs)
		for _, gid := range leaveArgs.GIDs {
			delete(configNew.Groups, gid)
			// set shard -> gid(0)
			for i := 0; i < NShards; i++ {
				if configNew.Shards[i] == gid {
					configNew.Shards[i] = 0
				}
			}
		}
		sc.reBalance(&configNew, "Leave", 0)
		sc.configs = append(sc.configs, configNew)
		return nil
	case "Move":
		moveArgs := op.OpArgs.(MoveArgs)
		DPrintf("OpHandler(%v): Move, args(%v)\n", sc.me, moveArgs)
		// add new replica groups
		configNew.Shards[moveArgs.Shard] = moveArgs.GID
		sc.configs = append(sc.configs, configNew)
		return nil
	case "Query":
		queryArgs := op.OpArgs.(QueryArgs)
		DPrintf("OpHandler(%v): Query, args(%v)\n", sc.me, queryArgs)
		if configLatest == nil {
			return nil
		}
		if queryArgs.Num <= -1 || queryArgs.Num >= len(sc.configs) {
			return &sc.configs[len(sc.configs)-1]
		} else {
			return &sc.configs[queryArgs.Num]
		}
	}
	return nil
}

func (sc *ShardCtrler) RPCHandler(op *Op) (bool, string, chan *Config) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(*op)
	DPrintf("RPCHandler(%v): type(%v), index(%v), isLeader(%v), CommandIdx(%v) from Client(%v)\n", sc.me, op.OpType, index, isLeader, op.CommandIdx, op.ClientID)
	if !isLeader {
		return false, "", nil
	}
	ch := make(chan *Config)
	key := op.ClientID + ":" + strconv.Itoa(op.CommandIdx)
	sc.replyChMap[key] = ch
	return true, key, ch
}

func (sc *ShardCtrler) mapCopy(src map[int][]string, dst map[int][]string) {
	DPrintf("mapCopy(%v)\n", sc.me)
	for key, value := range src {
		servers := make([]string, len(value))
		copy(servers, value)
		dst[key] = servers
	}
}

func (sc *ShardCtrler) GetReply(key string, ch chan *Config, opType string) interface{} {
	select {
	case newConfig := <-ch:
		DPrintf("GetReply(%v): OK, key(%v), opType(%v)\n", sc.me, key, opType)
		sc.mu.Lock()
		sc.delAndCloseCh(key)
		sc.mu.Unlock()
		switch opType {
		case "Join":
			reply := JoinReply{Err: OK}
			return reply
		case "Leave":
			reply := LeaveReply{Err: OK}
			return reply
		case "Move":
			reply := MoveReply{Err: OK}
			return reply
		case "Query":
			if newConfig == nil {
				return QueryReply{
					Err: ErrRedo,
				}
			} else {
				return QueryReply{
					Err:    OK,
					Config: *newConfig,
				}
			}
		}
	case <-time.After(5 * time.Second):
		DPrintf("GetReply(%v): Timeout, key(%v), opType(%v)\n", sc.me, key, opType)
		sc.mu.Lock()
		sc.delAndCloseCh(key)
		sc.mu.Unlock()
		switch opType {
		case "Join":
			reply := JoinReply{Err: ErrTimeout}
			return reply
		case "Leave":
			reply := LeaveReply{Err: ErrTimeout}
			return reply
		case "Move":
			reply := MoveReply{Err: ErrTimeout}
			return reply
		case "Query":
			reply := QueryReply{
				Err: ErrTimeout,
			}
			return reply
		}

	}
	return nil
}
func (sc *ShardCtrler) getCh(key string, createIfNotExists bool) chan *Config {
	DPrintf("getCh(%v): key(%v), createIfNotExists(%v)\n", sc.me, key, createIfNotExists)
	var ch chan *Config = nil
	var ok bool
	if ch, ok = sc.replyChMap[key]; !ok {
		if createIfNotExists {
			ch = make(chan *Config, 1)
			sc.replyChMap[key] = ch
		}
	}
	return ch
}

func (sc *ShardCtrler) delAndCloseCh(key string) {
	DPrintf("delAndCloseCh(%v): key(%v)\n", sc.me, key)
	if ch, ok := sc.replyChMap[key]; ok {
		delete(sc.replyChMap, key)
		close(ch)
	}
}

func (sc *ShardCtrler) applier() {
	DPrintf("applier(%v): Start\n", sc.me)
	for !sc.Killed() {
		msg := <-sc.applyCh
		DPrintf("applier(%v): Loop, index(%v), term(%v), msg(%v)\n", sc.me, msg.CommandIndex, msg.CommandTerm, msg)
		// apply entry
		sc.mu.Lock()
		ret := sc.OpHandler(msg.Command)
		// reply channel
		key := msg.Command.(Op).ClientID + ":" + strconv.Itoa(msg.Command.(Op).CommandIdx)
		ch, exist := sc.replyChMap[key]
		if exist {
			delete(sc.replyChMap, key)
		}
		sc.mu.Unlock()

		if exist {
			go func() {
				ch <- ret
				close(ch)
			}()
		}
	}
}

func (sc *ShardCtrler) reBalance(cfg *Config, opType string, joinGid int) {
	DPrintf("reBalance(%v): Start, cfg(%v), opType(%v), joinGid(%v)\n", sc.me, cfg, opType, joinGid)
	gid2shards := map[int][]int{}
	for key, _ := range cfg.Groups {
		gid2shards[key] = []int{}
	}
	gid2shards[0] = []int{}
	for shard, gid := range cfg.Shards {
		if v, ok := gid2shards[gid]; ok {
			gid2shards[gid] = append(v, shard)
		} else {
			panic("Err: shards contain wrong gid")
		}
	}

	getMaxShardGid := func(m map[int][]int) int {
		maxGid, maxShardNum := 0, 0
		for key, value := range m {
			if len(value) > maxShardNum {
				maxShardNum = len(value)
				maxGid = key
			} else if len(value) == maxShardNum && key > maxGid {
				maxGid = key
			}
		}
		return maxGid
	}

	getMinShardGid := func(m map[int][]int) int {
		minGid, minShardNum := 0, math.MaxInt
		for key, value := range m {
			if key == 0 {
				continue
			}
			if len(value) < minShardNum {
				minShardNum = len(value)
				minGid = key
			} else if len(value) == minShardNum && key < minGid {
				minGid = key
			}
		}
		return minGid
	}
	switch opType {
	case "Join":
		// not init
		if cfg.Shards[0] == 0 {
			for i := 0; i < NShards; i++ {
				cfg.Shards[i] = joinGid
			}
		} else {
			floor := NShards / len(cfg.Groups)
			for i := 0; i < floor; i++ {
				maxGid := getMaxShardGid(gid2shards)
				cfg.Shards[gid2shards[maxGid][0]] = joinGid
				gid2shards[maxGid] = gid2shards[maxGid][1:]
			}
		}
	case "Leave":
		for _, shard := range gid2shards[0] {
			minGid := getMinShardGid(gid2shards)
			cfg.Shards[shard] = minGid
			gid2shards[minGid] = append(gid2shards[minGid], shard)
		}
	}
	DPrintf("reBalance(%v): End, cfg(%v), opType(%v), joinGid(%v)\n", sc.me, cfg, opType, joinGid)

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.killed = false

	sc.configs = make([]Config, 1)
	sc.configs[0].Num = 0
	sc.configs[0].Groups = map[int][]string{}
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.replyChMap = map[string]chan *Config{}
	sc.clientLastCommandIdx = map[string]int{}
	go sc.applier()

	return sc
}
