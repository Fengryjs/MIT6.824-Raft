package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	Option    string
	Key       string
	Value     string
	Clerk     int
	RequestId int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
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

	mck *shardctrler.Clerk

	dead           int32
	kvPair         map[string]string
	waitCh         map[[2]int]chan raft.ApplyMsg
	waitTimer      map[[2]int]*time.Timer
	duplicateTable map[int]int
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Option:    "Get",
		Key:       args.Key,
		Value:     "",
		Clerk:     args.Clerk,
		RequestId: args.Request,
	}
	_, _, isLeader := kv.rf.Start(command)
	if isLeader {
		kv.mu.Lock()
		waitCh := make(chan raft.ApplyMsg)
		waitTimer := time.NewTimer(time.Second)
		key := [2]int{command.Clerk, command.RequestId}
		kv.waitCh[key] = waitCh
		kv.waitTimer[key] = waitTimer
		kv.mu.Unlock()
		select {
		case <-waitCh:
			kv.mu.Lock()
			fmt.Printf("[ShardKV]: Gid %v %v Get key %v value %v\n", kv.gid, kv.me, args.Key, kv.kvPair[args.Key])
			reply.Value = kv.kvPair[args.Key]
			kv.mu.Unlock()
			//logger.Printf("[KVGet]: server %v return key %v reply %v", kv.me, args.Key, kv.kvPair[args.Key])
			reply.Err = OK
		case <-waitTimer.C:
			logger.Printf("[KVGet]: Server %v Client %v Request %v timeout limit exceeds", kv.me, command.Clerk, command.RequestId)
			//delete(kv.waitCh, [2]int{command.Clerk, command.RequestId})
			reply.Err = ErrWrongLeader
		}
		//logger.Printf("[KVGet]: %v", msg)
	} else {
		reply.Err = ErrWrongLeader
	}
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{
		Option:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Clerk:     args.Clerk,
		RequestId: args.Request,
	}
	_, _, isLeader := kv.rf.Start(command)
	if isLeader {
		fmt.Printf("[ShardKV]: Gid %v %v %v key %v value %v\n", kv.gid, kv.me, args.Op, args.Key, args.Value)
		kv.mu.Lock()
		waitCh := make(chan raft.ApplyMsg)
		waitTimer := time.NewTimer(time.Second)
		key := [2]int{command.Clerk, command.RequestId}
		kv.waitCh[key] = waitCh
		kv.waitTimer[key] = waitTimer
		//fmt.Printf("[PutAppend]: %v %v kvpair %v\n", kv.gid, kv.me, kv.kvPair)
		kv.mu.Unlock()
		//logger.Printf("[KVPutAppend]: kv %v key %v value %v", kv.me, args.Key, kv.kvPair[args.Key])
		select {
		case <-waitCh:
			reply.Err = OK
			//delete(kv.waitCh, [2]int{command.Clerk, command.RequestId})
		case <-waitTimer.C:
			logger.Printf("[KVPutAppend]: Server %v Client %v Request %v timeout", kv.me, command.Clerk, command.RequestId)
			//delete(kv.waitCh, [2]int{command.Clerk, command.RequestId})
			reply.Err = ErrWrongLeader
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	// Your code here.
}

func (kv *ShardKV) ApplyOperation() {
	for kv.Killed() == false {
		for msg := range kv.applyCh {
			kv.mu.Lock()
			if msg.CommandValid {
				//logger.Printf("[ApplyOperation]: KV %v Receive %v", kv.me, msg.Command)
				op := msg.Command.(Op)
				key := [2]int{op.Clerk, op.RequestId}
				if kv.duplicateTable[op.Clerk] < op.RequestId {
					kv.duplicateTable[op.Clerk] = op.RequestId
					//kv.duplicateTable[key] = &op
					switch op.Option {
					case "Get":
					case "Put":
						kv.kvPair[op.Key] = op.Value
					case "Append":
						kv.kvPair[op.Key] = kv.kvPair[op.Key] + op.Value
					default:
						logger.Printf("[ApplyOp]: Wrong Operation Type")
					}
				}
				// Manual Chosen Number: Snapshot Interval
				if kv.maxraftstate != -1 && msg.CommandIndex%100 == 0 {
					logger.Printf("[KVSnapshot]: KV %v snap index %v currentSize %v\ncurrent state %v", kv.me, msg.CommandIndex, kv.rf.GetRaftStateSize(), kv.kvPair)
					b := new(bytes.Buffer)
					e := labgob.NewEncoder(b)
					if e.Encode(kv.kvPair) == nil && e.Encode(kv.duplicateTable) == nil {
						kv.rf.Snapshot(msg.CommandIndex, b.Bytes())
					}
				}
				if kv.waitCh[key] != nil && kv.waitTimer[key].Stop() == true {
					waitCh := kv.waitCh[key]
					logger.Printf("[ApplyOperation]: KV Gid %v %v Receive %v and go through waitCh", kv.gid, kv.me, msg.Command)
					kv.mu.Unlock()
					waitCh <- msg
					continue
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.ReadPersist(msg.Snapshot)
				logger.Printf("[KVSnapshot]: KV %v read snapshot %v", kv.me, kv.kvPair)
				kv.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}
func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) ReadPersist(snapshot []byte) {
	if snapshot != nil && len(snapshot) > 0 {
		w := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(w)
		if d.Decode(&kv.kvPair) != nil || d.Decode(&kv.duplicateTable) != nil {
			logger.Printf("[ReadPersist]: KV %v load persist error", kv.me)
		}
	}
}

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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.waitTimer = make(map[[2]int]*time.Timer)
	kv.waitCh = make(map[[2]int]chan raft.ApplyMsg)
	kv.duplicateTable = make(map[int]int)
	kv.kvPair = make(map[string]string)

	go kv.ApplyOperation()
	return kv
}
