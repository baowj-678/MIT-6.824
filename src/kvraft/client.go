package kvraft

import (
	"6.824/labrpc"
	"strconv"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader    int
	clientId  string
	commandId int

	wrongLeaderTimer map[int]int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = strconv.FormatInt(nrand(), 10)
	ck.wrongLeaderTimer = map[int]int64{}
	for i := 0; i < len(servers); i++ {
		ck.wrongLeaderTimer[i] = 0
	}
	ck.commandId = 0
	DPrintf("Client(%v); Start", ck.clientId)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string { // TODO Get需要发给Leader吗？
	// You will have to modify this function.
	ck.commandId += 1
	request := GetArgs{
		Key: key,
		RPCArgs: RPCArgs{
			CommandId: ck.commandId,
			ClientId:  ck.clientId,
		},
	}
	reply := GetReply{}
	server := ck.PickLeaderServer(false, -1)
	for {
		DPrintf("Client-Get(%v): CommandId(%v) to Server(%v), Get-Key(%v)", ck.clientId, request.CommandId, server, request.Key)
		ok := ck.servers[server].Call("KVServer.Get", &request, &reply)
		if ok {
			if reply.Err == OK {
				DPrintf("Client-Get(%v), Successfully get Key(%v), Value(%v), for CommandId(%v)\n", ck.clientId, key, reply.Value, request.CommandId)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("Client-Get(%v), No Key(%v), for CommandId(%v)", ck.clientId, key, request.CommandId)
				return ""
			} else if reply.Err == ErrWrongLeader {
				ck.wrongLeaderTimer[server] = time.Now().UnixMilli()
				// pick new server
				server = ck.PickLeaderServer(true, -1)
			} else if reply.Err == ErrTimeOut {
				server = ck.PickLeaderServer(true, -1)
			} else if reply.Err == ErrReDo {
				DPrintf("Client-Get(%v), Redo Key(%v), for CommandId(%v)", ck.clientId, key, request.CommandId)
				ck.commandId += 1
				request.CommandId = ck.commandId
			}
		} else {
			// pick new server
			server = ck.PickLeaderServer(true, -1)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandId += 1
	request := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		RPCArgs: RPCArgs{
			CommandId: ck.commandId,
			ClientId:  ck.clientId,
		},
	}
	reply := PutAppendReply{}
	server := ck.PickLeaderServer(false, -1)
	for {
		DPrintf("Client-%v(%v): CommandId(%v) to Server(%v), %v-Key(%v) Value(%v)", op, ck.clientId, request.CommandId, server, request.Op, request.Key, request.Value)
		ok := ck.servers[server].Call("KVServer.PutAppend", &request, &reply)
		if ok {
			if reply.Err == OK {
				DPrintf("Client-%v(%v), Successfully PutAppend Key(%v), Value(%v), for CommandId(%v)\n", op, ck.clientId, key, value, request.CommandId)
				return
			} else if reply.Err == ErrWrongLeader {
				ck.wrongLeaderTimer[server] = time.Now().UnixMilli()
				// pick new server
				server = ck.PickLeaderServer(true, -1)
			} else if reply.Err == ErrNoKey {
				panic("PutAppend should not reply ErrNoKey")
			} else if reply.Err == ErrTimeOut {
				server = ck.PickLeaderServer(true, -1)
			} else if reply.Err == ErrReDo {
				DPrintf("Client-%v(%v), Redo PutAppend Key(%v), Value(%v), for CommandId(%v)", op, ck.clientId, key, value, request.CommandId)
				return
			}

		} else {
			// pick new server TODO:是否立马重新选择serevr
			server = ck.PickLeaderServer(true, -1)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) PickLeaderServer(change bool, newLeader int) int {
	if change {
		if newLeader == ck.leader && newLeader != -1 {
			ck.leader = newLeader
		} else {
			leader := ck.leader
			for i := 1; i < len(ck.servers); i++ {
				server := (leader + i) % len(ck.servers)
				if time.Now().UnixMilli()-ck.wrongLeaderTimer[server] > 100 {
					leader = server
					break
				}
			}
			if leader == ck.leader {
				// all not timeout
				time.Sleep(60 * time.Millisecond)
			} else {
				ck.leader = leader
			}
		}
	}
	return ck.leader
}
