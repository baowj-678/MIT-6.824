package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	Config interface{}
	BaseArgs
}

func (cfg *ShardConfig) Copy() *ShardConfig {
	servers := make([]string, len(cfg.Servers))
	copy(servers, cfg.Servers)
	tmp := ShardConfig{
		Num:     cfg.Num,
		Shard:   cfg.Shard,
		Servers: servers,
	}
	return &tmp
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state              State
	mck                *shardctrler.Clerk
	snapshotInterval   time.Duration
	pollConfigInterval time.Duration

	killed     int32
	replyChMap map[string]chan *ReplyMsg

	// database info to persist
	kvDatabase           [shardctrler.NShards]map[string]string
	lastAppliedIdx       int
	lastAppliedTerm      int
	clientLastCommandIdx map[string]int
	shardConfig          ShardConfig

	// for migration
	receivedEmigrationShardTimes [shardctrler.NShards]int
	needEmigrationShardTimes     [shardctrler.NShards]int // default -1
	receivedShardLastTerm        [shardctrler.NShards]int
	receivedShardLastIndex       [shardctrler.NShards]int
	receivedSnapshot             [shardctrler.NShards][]byte
	needEmigrationShard          [shardctrler.NShards]bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("ShardKV-Get(%v): args(%v)\n", kv.me, args)
	op := Op{
		OpType:   "Get",
		Key:      args.Key,
		Value:    "",
		BaseArgs: args.BaseArgs,
	}
	opKey := args.ClientId + ":" + strconv.Itoa(args.CommandIdx)
	err, ch := kv.RPCHandler(&op, opKey)
	if err == ErrWrongLeader || err == ErrRedo || err == ErrWrongGroup {
		reply.Err = err
		return
	}
	rep := kv.waitForChan(ch, opKey)
	reply.Err = rep.Err
	reply.Value = rep.Value
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("ShardKV-PutAppend(%v): args(%v)\n", kv.me, args)
	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		BaseArgs: args.BaseArgs,
	}
	opKey := args.ClientId + ":" + strconv.Itoa(args.CommandIdx)
	err, ch := kv.RPCHandler(&op, opKey)
	if err == ErrWrongLeader || err == ErrRedo || err == ErrWrongGroup {
		reply.Err = err
		return
	}
	rep := kv.waitForChan(ch, opKey)
	reply.Err = rep.Err
	return
}

func (kv *ShardKV) RPCHandler(op *Op, opKey string) (Err, chan *ReplyMsg) {
	DPrintf("ShardKV-RPCHandler(%v): op(%v)\n", kv.me, op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check is work
	if kv.state != Work {
		DPrintf("ShardKV-RPCHandler(%v): op(%v) [%v]Not Work\n", kv.me, op, kv.state)
		return ErrWrongGroup, nil
	}
	// check is right server
	if !kv.checkServer(&op.Key) {
		DPrintf("ShardKV-RPCHandler(%v): op(%v) Not Right Server\n", kv.me, op)
		return ErrWrongGroup, nil
	}
	// check is redo
	if max, ok := kv.clientLastCommandIdx[op.ClientId]; ok {
		if op.CommandIdx <= max {
			return ErrRedo, nil
		}
	}
	index, term, isLeader := kv.rf.Start(*op)
	DPrintf("ShardKV-RPCHandler(%v): Start: index(%v), term(%v), isLeader(%v)\n", kv.me, index, term, isLeader)
	if isLeader {
		// make channel
		ch := make(chan *ReplyMsg)
		kv.replyChMap[opKey] = ch
		return OK, ch
	} else {
		return ErrWrongLeader, nil
	}
}

func (kv *ShardKV) waitForChan(ch chan *ReplyMsg, key string) *ReplyMsg {
	DPrintf("ShardKV-waitForChan(%v): key(%v)\n", kv.me, key)
	select {
	case rep := <-ch:
		kv.mu.Lock()
		kv.delAndCloseCh(key)
		kv.mu.Unlock()
		return rep
	case <-time.After(5 * time.Second):
		kv.mu.Lock()
		kv.delAndCloseCh(key)
		kv.mu.Unlock()
		rep := &ReplyMsg{}
		rep.Err = ErrTimeout
		return rep
	}
}

func (kv *ShardKV) delAndCloseCh(key string) {
	if ch, ok := kv.replyChMap[key]; ok {
		delete(kv.replyChMap, key)
		close(ch)
	}
}

// [No Lock]
func (kv *ShardKV) checkServer(key *string) bool {
	shard := key2shard(*key)
	return kv.shardConfig.Shard[shard]
}

func (kv *ShardKV) doOp(op Op) ReplyMsg {
	DPrintf("ShardKV-doOp(%v): op(%v)\n", kv.me, op)
	rep := ReplyMsg{
		Err:   OK,
		Value: "",
	}
	// check server
	if !kv.checkServer(&op.Key) {
		panic("ErrWrongGroup")
	}
	// check command idx
	if max, ok := kv.clientLastCommandIdx[op.ClientId]; ok {
		if op.CommandIdx > max {
			kv.clientLastCommandIdx[op.ClientId] = op.CommandIdx
		} else {
			rep.Err = ErrRedo
			return rep
		}
	} else {
		kv.clientLastCommandIdx[op.ClientId] = op.CommandIdx
	}
	// do op
	// get db for shard
	db := kv.kvDatabase[key2shard(op.Key)]
	switch op.OpType {
	case "Get":
		if s, ok := db[op.Key]; ok {
			rep.Value = s
			rep.Err = OK
		} else {
			rep.Err = ErrNoKey
		}
	case "Put":
		db[op.Key] = op.Value
		rep.Err = OK
	case "Append":
		if s, ok := db[op.Key]; ok {
			db[op.Key] = s + op.Value
		} else {
			db[op.Key] = op.Value
		}
		rep.Err = OK
	}
	return rep
}

func (kv *ShardKV) Snapshot() {
	DPrintf("ShardKV-Snapshot(%v): Start", kv.me)
	for kv.killed == 0 {
		kv.mu.Lock()
		size := kv.rf.RaftStateSize()
		if kv.maxraftstate != -1 {
			if float64(kv.maxraftstate)*0.8 < float64(size) {
				// generate snapshot
				w := kv.GenerateTotalSnapshot()
				data := w.Bytes()
				kv.rf.Snapshot(kv.lastAppliedIdx, data)
			}
		}
		kv.mu.Unlock()
		time.Sleep(kv.snapshotInterval * time.Millisecond)
	}
}

// GenerateTotalSnapshot [No Lock]
func (kv *ShardKV) GenerateTotalSnapshot() *bytes.Buffer {
	// generate snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastAppliedIdx)
	e.Encode(kv.lastAppliedTerm)
	e.Encode(kv.kvDatabase)
	e.Encode(kv.clientLastCommandIdx)
	e.Encode(kv.shardConfig)
	return w
}

// GenerateShardSnapshot generate some shard's snapshot for migration.
func (kv *ShardKV) GenerateShardSnapshot(shard int) *bytes.Buffer {
	// generate snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDatabase[shard])
	e.Encode(kv.clientLastCommandIdx)
	return w
}

func (kv *ShardKV) ReadShardSnapshot(snapshot []byte, shard int) {
	DPrintf("ShardKV-ReadShardSnapshot(%v)\n", kv.me)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var state map[string]string
	var m map[string]int
	if d.Decode(&state) != nil ||
		d.Decode(&m) != nil {
	} else {
		kv.kvDatabase[shard] = state
		kv.MergeLastCommandIdx(m)
	}
}

// MergeLastCommandIdx  [No Lock]
// merge map to kv.clientLastCommandIdx by pick the max value for every key.
func (kv *ShardKV) MergeLastCommandIdx(m map[string]int) {
	for key, value := range m {
		if v, ok := kv.clientLastCommandIdx[key]; ok && v < value {
			kv.clientLastCommandIdx[key] = value
		} else {
			kv.clientLastCommandIdx[key] = value
		}
	}
}

func (kv *ShardKV) ReadSnapshotFromRaft(snapshot []byte) {
	DPrintf("ShardKV-ReadSnapshotFromRaft(%v)\n", kv.me)
	// snapshot
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var state [shardctrler.NShards]map[string]string
	var m map[string]int
	var shardConfig ShardConfig
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&m) != nil ||
		d.Decode(&shardConfig) != nil {
	} else {
		kv.lastAppliedIdx = lastIncludedIndex
		kv.lastAppliedTerm = lastIncludedTerm
		kv.kvDatabase = state
		kv.clientLastCommandIdx = m
		kv.shardConfig = shardConfig
	}
}

// Follower's apply goroutine
// always waiting for apply raft's entry to machine
// and send commandCh if it's Leader
func (kv *ShardKV) applier() {
	DPrintf("ShardKV-applier(%v): Start\n", kv.me)
	for kv.killed == 0 {
		msg := <-kv.applyCh
		//DPrintf("ShardKV-applier(%v): Loop\n", kv.me)
		if msg.SnapshotValid {
			// install snapshot
			kv.mu.Lock()
			kv.ReadSnapshotFromRaft(msg.Snapshot)
			kv.mu.Unlock()
		} else {
			// apply entry
			if msg.Command.(Op).OpType == "Config" {
				// re config
				kv.ConfigHandler(msg.Command.(Op).Config.(shardctrler.Config))
			} else {
				// do op
				kv.mu.Lock()
				val := kv.doOp(msg.Command.(Op))
				kv.lastAppliedIdx = msg.CommandIndex
				kv.lastAppliedTerm = msg.CommandTerm
				// reply channel
				opId := msg.Command.(Op).ClientId + ":" + strconv.Itoa(msg.Command.(Op).CommandIdx)
				ch, exist := kv.replyChMap[opId]
				if exist {
					delete(kv.replyChMap, opId)
				}
				kv.mu.Unlock()
				if exist {
					go func() {
						ch <- &val
						close(ch)
					}()
				}
			}
		}
	}
}

// PollConfig TODO only leader do
func (kv *ShardKV) PollConfig() {
	DPrintf("ShardKV-PollConfig(%v): Start\n", kv.me)
	for kv.killed == 0 && kv.state != Migration {
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf("ShardKV-PollConfig(%v): Loop, State(%v)\n", kv.me, kv.state)
			tmp := kv.mck.Query(-1)
			newShard := [shardctrler.NShards]bool{}
			for i := 0; i < shardctrler.NShards; i++ {
				if tmp.Shards[i] == kv.gid {
					newShard[i] = true
				}
			}
			var isLeader = false
			kv.mu.Lock()
			if kv.shardConfig.Num < tmp.Num && kv.shardConfig.Shard != newShard {
				DPrintf("ShardKV-PollConfig(%v, %v): Re Config, oldShard(%v), newShard(%v), State(%v), cfg.Shard(%v)\n", kv.gid, kv.me, kv.shardConfig.Shard, newShard, kv.state, tmp.Shards)
				// stop server
				atomic.StoreInt32((*int32)(&kv.state), Migration)
				_, _, isLeader = kv.rf.Start(
					Op{
						OpType:   "Config",
						Key:      "",
						Value:    "",
						Config:   tmp,
						BaseArgs: BaseArgs{},
					})
			}
			kv.mu.Unlock()
			if isLeader {
				// stop polling config for a while, wait for applier.
				time.Sleep(kv.pollConfigInterval * 10 * time.Millisecond)
			}
		}
		time.Sleep(kv.pollConfigInterval * time.Millisecond)
	}
}

// [No Lock]
// installConfig set local shard-config by cfg
func (kv *ShardKV) installConfig(cfg shardctrler.Config) {
	kv.shardConfig.Num = cfg.Num
	for i := 0; i < shardctrler.NShards; i++ {
		if cfg.Shards[i] == kv.gid {
			kv.shardConfig.Shard[i] = true
		} else {
			kv.shardConfig.Shard[i] = false
			// clean db
			kv.kvDatabase[i] = nil
		}
	}
}

func (kv *ShardKV) ConfigHandler(cfg shardctrler.Config) {
	DPrintf("ShardKV-ConfigHandler(%v, %v): cfg:(%v)\n", kv.gid, kv.me, cfg)
	kv.mu.Lock()
	// stop server
	atomic.StoreInt32((*int32)(&kv.state), Migration)
	kv.needEmigrationShard = [shardctrler.NShards]bool{}

	isChanged := false
	needEmigration := false
	for i := 0; i < len(cfg.Shards); i++ {
		if (cfg.Shards[i] == kv.gid) != kv.shardConfig.Shard[i] {
			if cfg.Shards[i] != kv.gid {
				// immigration by shard
				kv.ImmigrationSingleShard(i, cfg.Groups[cfg.Shards[i]], cfg.Num)
			} else {
				// emigration by shard
				kv.needEmigrationShard[i] = true
				needEmigration = true
			}
			isChanged = true
		}
	}
	// check config is changed
	if !isChanged {
		log.Fatalf("Configuration not Changed(%v, %v)\n", kv.gid, kv.me)
	}
	kv.installConfig(cfg)
	if needEmigration {
		// prepare emigration
		kv.initEmigrationInfo()
	} else {
		// don't need to immigration, start work
		atomic.StoreInt32((*int32)(&kv.state), Work)
	}
	kv.mu.Unlock()
}

// ImmigrationSingleShard
// immigration single shard from this server to other servers.
func (kv *ShardKV) ImmigrationSingleShard(shard int, serversName []string, configNum int) {
	// prepare immigration data
	snapshot := kv.GenerateShardSnapshot(shard)
	data := snapshot.Bytes()
	// get target servers
	servers := make([]*labrpc.ClientEnd, len(serversName))
	for i, name := range serversName {
		servers[i] = kv.make_end(name)
	}
	args := MigrationArgs{
		Snapshot:        data,
		LastAppliedTerm: kv.lastAppliedTerm,
		LastAppliedIdx:  kv.lastAppliedIdx,
		ShardNum:        shard,
		ConfigNum:       configNum,
		ServerCnt:       len(kv.shardConfig.Servers),
	}

	// start immigration
	go kv.Immigration(args, servers)
}

// Immigration send snapshot to servers.
func (kv *ShardKV) Immigration(args MigrationArgs, servers []*labrpc.ClientEnd) {
	limit := len(servers)/2 + 1
	ch := make(chan bool, len(servers))
	successCnt, total := 0, 0
	for _, server := range servers {
		go kv.ImmigrationSend(args, server, ch, 0)
	}
	for res := range ch {
		total++
		if res {
			successCnt++
			if successCnt >= limit {
				// migration out success.
				DPrintf("Immigration(%v): Success\n", kv.me)
				break
			}
		}
		if total >= len(servers) {
			// migration out not success
			DPrintf("Immigration Fail\n")
			break
		}
	}
}

func (kv *ShardKV) ImmigrationSend(msg MigrationArgs, server *labrpc.ClientEnd, ch chan bool, tryTimes int) {
	reply := MigrationReply{}
	ok := server.Call("ShardKV.InstallMigration", &msg, &reply)
	if ok {
		ch <- true
	} else {
		tryTimes++
		if tryTimes > 3 {
			ch <- false
			return
		}
		time.Sleep(time.Duration(tryTimes) * 200 * time.Millisecond)
		go kv.ImmigrationSend(msg, server, ch, tryTimes)
	}
}

// [No Lock]
func (kv *ShardKV) initEmigrationInfo() {
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.needEmigrationShard[i] {
			// need wait for emigration
			kv.needEmigrationShardTimes = [shardctrler.NShards]int{}
			kv.receivedEmigrationShardTimes = [shardctrler.NShards]int{}
			kv.receivedShardLastTerm = [shardctrler.NShards]int{}
			kv.receivedShardLastIndex = [shardctrler.NShards]int{}
			return
		}
	}
}

func (kv *ShardKV) InstallMigration(args *MigrationArgs, reply *MigrationReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Success = false
	// check state is migration
	if kv.state != Migration {
		return
	}
	// check config num
	if args.ConfigNum != kv.shardConfig.Num {
		return
	}
	reply.Success = true
	// check shard num
	if kv.needEmigrationShard[args.ShardNum] {
		if kv.needEmigrationShardTimes[args.ShardNum] == 0 {
			kv.needEmigrationShardTimes[args.ShardNum] = args.ServerCnt
		}
	} else {
		reply.Success = true
		return
	}

	// save snapshot
	if args.LastAppliedTerm > kv.receivedShardLastTerm[args.ShardNum] && args.LastAppliedIdx > kv.receivedShardLastIndex[args.ShardNum] {
		kv.receivedShardLastIndex[args.ShardNum] = args.LastAppliedIdx
		kv.receivedShardLastTerm[args.ShardNum] = args.LastAppliedTerm
		kv.receivedSnapshot[args.ShardNum] = args.Snapshot
	}
	kv.receivedEmigrationShardTimes[args.ShardNum]++

	// check single shard finished
	if kv.receivedEmigrationShardTimes[args.ShardNum] > kv.needEmigrationShardTimes[args.ShardNum]/2 {
		// install snapshot
		kv.ReadShardSnapshot(kv.receivedSnapshot[args.ShardNum], args.ShardNum)
		// set shard finished
		kv.needEmigrationShard[args.ShardNum] = false
		// remove snapshot data
		kv.receivedSnapshot[args.ShardNum] = nil
	}

	// check total emigration finished
	isOK := true
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.needEmigrationShard[i] {
			isOK = false
		}
	}
	if isOK {
		// start work
		atomic.StoreInt32((*int32)(&kv.state), Work)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.killed, 1)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(MigrationArgs{})
	labgob.Register(MigrationReply{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.state = Prepare
	kv.killed = 0
	kv.clientLastCommandIdx = map[string]int{}
	kv.replyChMap = map[string]chan *ReplyMsg{}
	kv.kvDatabase = [shardctrler.NShards]map[string]string{}
	kv.snapshotInterval = 100
	kv.pollConfigInterval = 100
	kv.lastAppliedIdx = 0
	kv.lastAppliedTerm = 0
	// init migration
	kv.shardConfig = ShardConfig{
		Num:     0,
		Shard:   [shardctrler.NShards]bool{false},
		Servers: nil,
	}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardConfig.Shard[i] = true
		kv.kvDatabase[i] = map[string]string{}
	}
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.Snapshot()
	go kv.PollConfig()
	return kv
}
