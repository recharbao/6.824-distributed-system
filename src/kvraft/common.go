package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongLeaderFor = "ErrWrongLeaderFor"
	TooLongTime = "TooLongTime"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	RpcId int64
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	RpcId int64
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

