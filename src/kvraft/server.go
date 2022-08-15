package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"context"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

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
	Op        string
	ClientId  string
	CommandId int
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	snapshotInterval   time.Duration
	kvDatabase         map[string]string
	waitChannel        map[string]chan ChReply
	clientMaxCommandId map[string]int
	lastApplied        int
	lastAppliedTerm    int
}

// TODO Get不用redoError
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// is Leader
	op := Op{
		Op:        "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	DPrintf("Server-Get(%v): CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
	opId := args.ClientId + strconv.Itoa(args.CommandId)
	kv.mu.Lock()
	// check is redo
	if max, ok := kv.clientMaxCommandId[args.ClientId]; ok {
		if args.CommandId <= max {
			DPrintf("Server-Get(%v): CommadId(%v) from ClientId(%v) has done", kv.me, op.CommandId, op.ClientId)
			reply.Err = ErrReDo
			kv.mu.Unlock()
			DPrintf("Server-Get(%v): reply(%v), value(%v), CommadId(%v) from ClientId(%v)", kv.me, reply.Err, reply.Value, op.CommandId, op.ClientId)
			return
		}
	}
	DPrintf("Server-Get(%v): start, CommadId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("Server-Get(%v): after start, CommadId(%v) from ClientId(%v), term(%v), index(%v)\n", kv.me, op.CommandId, op.ClientId, term, index)
	kv.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if isLeader {
		kv.mu.Lock()
		// make channel
		ch := make(chan ChReply)
		kv.waitChannel[opId] = ch
		kv.mu.Unlock()
		DPrintf("Server-Get(%v): wait, CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
		select {
		case rep := <-ch:
			kv.mu.Lock()
			kv.delAndCloseCh(opId)
			kv.mu.Unlock()
			DPrintf("Server-Get(%v): get response, CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
			reply.Err = rep.Err
			reply.Value = rep.Value
		case <-ctx.Done():
			kv.mu.Lock()
			kv.delAndCloseCh(opId)
			kv.mu.Unlock()
			DPrintf("Server-Get(%v): timeout, CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
			reply.Err = ErrTimeOut
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("Server-Get(%v): reply(%v), value(%v), CommandId(%v) from ClientId(%v)", kv.me, reply.Err, reply.Value, op.CommandId, op.ClientId)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// is Leader
	op := Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	DPrintf("Server-PutAppend(%v): CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
	opId := args.ClientId + strconv.Itoa(args.CommandId)
	kv.mu.Lock()
	// check is redo
	if max, ok := kv.clientMaxCommandId[args.ClientId]; ok {
		if args.CommandId <= max {
			DPrintf("Server-PutAppend(%v): CommandId(%v) from ClientId(%v) has done", kv.me, op.CommandId, op.ClientId)
			reply.Err = ErrReDo
			kv.mu.Unlock()
			DPrintf("Server-PutAppend(%v): reply(%v), CommandId(%v) from ClientId(%v)", kv.me, reply.Err, op.CommandId, op.ClientId)
			return
		}
	}
	DPrintf("Server-PutAppend(%v): start, CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("Server-PutAppend(%v): after start, CommadId(%v) from ClientId(%v), term(%v), index(%v)\n", kv.me, op.CommandId, op.ClientId, term, index)
	kv.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if isLeader {
		kv.mu.Lock()
		// make channel
		ch := make(chan ChReply)
		kv.waitChannel[opId] = ch
		kv.mu.Unlock()
		DPrintf("Server-PutAppend(%v): wait, CommandId(%v) from ClientId(%v), commandIndex(%v)", kv.me, op.CommandId, op.ClientId, index)
		select {
		case rep := <-ch:
			kv.mu.Lock()
			kv.delAndCloseCh(opId)
			kv.mu.Unlock()
			DPrintf("Server-PutAppend(%v): get response, CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
			reply.Err = rep.Err
		case <-ctx.Done():
			kv.mu.Lock()
			kv.delAndCloseCh(opId)
			kv.mu.Unlock()
			DPrintf("Server-PutAppend(%v): timeout, CommandId(%v) from ClientId(%v)", kv.me, op.CommandId, op.ClientId)
			reply.Err = ErrTimeOut
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("Server-PutAppend(%v): reply(%v), CommandId(%v) from ClientId(%v), term(%v), index(%v)\n", kv.me, reply.Err, op.CommandId, op.ClientId, term, index)
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kvDatabase = make(map[string]string)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientMaxCommandId = map[string]int{}
	kv.kvDatabase = map[string]string{}
	kv.waitChannel = map[string]chan ChReply{}
	kv.snapshotInterval = 100

	DPrintf("Server(%v), Start", kv.me)
	// start applier
	go kv.applier()
	go kv.Snapshot()
	return kv
}

func (kv *KVServer) delAndCloseCh(key string) {
	if ch, ok := kv.waitChannel[key]; ok {
		delete(kv.waitChannel, key)
		close(ch)
	}
}

func (kv *KVServer) doOp(op Op) (bool, ChReply) {
	rep := ChReply{
		Err:   OK,
		Value: "",
	}
	if max, ok := kv.clientMaxCommandId[op.ClientId]; ok {
		if op.CommandId > max {
			kv.clientMaxCommandId[op.ClientId] = op.CommandId
		} else {
			DPrintf("Server-doOp(%v): CommadId(%v) from ClientId(%v) has done", kv.me, op.CommandId, op.ClientId)
			rep.Err = ErrReDo
			return false, rep
		}
	} else {
		kv.clientMaxCommandId[op.ClientId] = op.CommandId
	}
	switch op.Op {
	case "Get":
		if s, ok := kv.kvDatabase[op.Key]; ok {
			rep.Value = s
			rep.Err = OK
			DPrintf("Server-doOp(%v): CommandId(%v) from ClientId(%v), %v Key(%v), Value(%v)", kv.me, op.CommandId, op.ClientId, op.Op, op.Key, s)
			return true, rep
		} else {
			rep.Err = ErrNoKey
			DPrintf("Server-doOp(%v): CommandId(%v) from ClientId(%v), %v Key(%v), Value(None)", kv.me, op.CommandId, op.ClientId, op.Op, op.Key)
			return false, rep
		}
	case "Put":
		kv.kvDatabase[op.Key] = op.Value
		rep.Err = OK
		DPrintf("Server-doOp(%v): CommandId(%v) from ClientId(%v), %v Key(%v), Value(%v)", kv.me, op.CommandId, op.ClientId, op.Op, op.Key, op.Value)
		return true, rep
	case "Append":
		if s, ok := kv.kvDatabase[op.Key]; ok {
			kv.kvDatabase[op.Key] = s + op.Value
		} else {
			kv.kvDatabase[op.Key] = op.Value
		}
		DPrintf("Server-doOp(%v): CommandId(%v) from ClientId(%v), %v Key(%v), Value(%v)", kv.me, op.CommandId, op.ClientId, op.Op, op.Key, op.Value)
		rep.Err = OK
		return true, rep
	}
	return false, rep
}

// Follower's apply goroutine
// always waiting for apply raft's entry to machine
// and send commandCh if it's Leader
func (kv *KVServer) applier() {
	DPrintf("Server-applier(%v): Start", kv.me)
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.SnapshotValid {
			// install snapshot
			kv.mu.Lock()
			kv.ReadSnapshotFromRaft(msg.Snapshot)
			kv.mu.Unlock()
			DPrintf("Server-applier(%v): Snapshot", kv.me)
		} else {
			// apply entry
			kv.mu.Lock()
			_, val := kv.doOp(msg.Command.(Op))
			DPrintf("Server-applier(%v): Command, CommandId(%v) from ClientId(%v), applyIndex(%v)", kv.me, msg.Command.(Op).CommandId, msg.Command.(Op).ClientId, msg.CommandIndex)
			kv.lastApplied = msg.CommandIndex
			kv.lastAppliedTerm = msg.CommandTerm
			// reply channel
			opId := msg.Command.(Op).ClientId + strconv.Itoa(msg.Command.(Op).CommandId)
			ch, exist := kv.waitChannel[opId]
			if exist {
				delete(kv.waitChannel, opId)
			}
			kv.mu.Unlock()
			// TODO optimize, thread
			if exist {
				go func() {
					ch <- val
				}()
			}
		}
	}
}

func (kv *KVServer) Snapshot() {
	DPrintf("Server-Snapshot(%v): Start", kv.me)
	for !kv.killed() {
		kv.mu.Lock()
		size := kv.rf.RaftStateSize()
		if kv.maxraftstate != -1 {
			if float64(kv.maxraftstate)*0.8 < float64(size) {
				// generate snapshot
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.lastApplied)
				e.Encode(kv.lastAppliedTerm)
				e.Encode(kv.kvDatabase)
				e.Encode(kv.clientMaxCommandId)
				data := w.Bytes()
				kv.rf.Snapshot(kv.lastApplied, data)
			}
		}
		kv.mu.Unlock()
		time.Sleep(kv.snapshotInterval * time.Millisecond)
	}
}

func (kv *KVServer) ReadSnapshotFromRaft(snapshot []byte) {
	// snapshot

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var state map[string]string
	var m map[string]int
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&m) != nil {
	} else {
		kv.lastApplied = lastIncludedIndex
		kv.lastAppliedTerm = lastIncludedTerm
		kv.kvDatabase = state
		kv.clientMaxCommandId = m
		DPrintf("ReadSnapshotFromRaft(%v): lastApplied(%v), lastAppliedTerm(%v)\n", kv.me, kv.lastApplied, kv.lastAppliedTerm)
	}
}
