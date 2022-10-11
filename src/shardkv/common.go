package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRedo        = "ErrRedo"
	ErrTimeout     = "ErrTimeout"

	// state
	Work      = 1
	Migration = 0
	Prepare   = 2
)

type Err string
type State int32

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	BaseArgs
}

type ShardConfig struct {
	Num     int                       // config number
	Shard   [shardctrler.NShards]bool // shard
	Servers []string                  // servers[]
}

type PutAppendReply struct {
	Err Err
}

type ReplyMsg struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	BaseArgs
}

type GetReply struct {
	Err   Err
	Value string
}

type BaseArgs struct {
	ClientId   string
	CommandIdx int
}

type MigrationArgs struct {
	ShardNum        int
	ConfigNum       int
	ServerCnt       int
	LastAppliedIdx  int
	LastAppliedTerm int
	Snapshot        []byte
}

type MigrationReply struct {
	Success bool
}
