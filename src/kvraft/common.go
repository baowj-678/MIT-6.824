package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	ErrReDo        = "ErrReDo"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RPCArgs
}

type RPCArgs struct {
	ClientId  string
	CommandId int
}

type PutAppendReply struct {
	Err       Err
	NewLeader int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RPCArgs
}

type GetReply struct {
	Err       Err
	Value     string
	NewLeader int
}

type ChReply struct {
	Err   Err
	Value string
}
