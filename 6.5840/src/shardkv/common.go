package shardkv

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
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Clerk     int
	Request   int
	Shard     int
	ConfigNum int
	Op        string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	Clerk     int
	Request   int
	Shard     int
	ConfigNum int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type TakeOverShardArgs struct {
	Shard     int
	Data      map[int]map[string]string
	Group     int
	ConfigNum int
}

type TakeOverShardReply struct {
	Err Err
}

type AskForShardArgs struct {
	Shard     int
	ConfigNum int
}

type AskForShardReply struct {
	Err            string
	Data           map[string]string
	DuplicateTable map[int]int
}
