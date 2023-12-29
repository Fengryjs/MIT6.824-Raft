package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
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
	Option    string
	Key       string
	Value     string
	Clerk     int
	RequestId int
}

type KVServer struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	dead           int32 // set by Kill()
	maxraftstate   int   // snapshot if log grows this big
	kvPair         map[string]string
	waitCh         map[[2]int]chan raft.ApplyMsg
	waitTimer      map[[2]int]*time.Timer
	duplicateTable map[int]int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 每个非Leader节点的applyCh也会接收到msg，如果不接受会阻塞
	command := Op{
		Option:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
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
func (kv *KVServer) ApplyOperation() {
	for kv.killed() == false {
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
					logger.Printf("[ApplyOperation]: KV %v Receive %v and go through waitCh", kv.me, msg.Command)
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ReadPersist(snapshot []byte) {
	var data []byte
	if snapshot == nil {
		data = snapshot
	} else {
		data = kv.rf.GetSnapshot()
	}
	if data != nil {
		w := bytes.NewBuffer(data)
		d := labgob.NewDecoder(w)
		if d.Decode(&kv.kvPair) != nil || d.Decode(&kv.duplicateTable) != nil {
			logger.Printf("[ReadPersist]: KV %v load persist error", kv.me)
		}
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvPair = make(map[string]string)
	kv.duplicateTable = make(map[int]int)
	kv.ReadPersist(nil)
	kv.waitCh = make(map[[2]int]chan raft.ApplyMsg)
	kv.waitTimer = make(map[[2]int]*time.Timer)
	// You may need initialization code here.
	go kv.ApplyOperation()
	return kv
}
