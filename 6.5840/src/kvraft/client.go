package kvraft

import (
	"6.5840/labrpc"
	"log"
	"os"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

var kvraftLog = "KVRaftLog.txt"
var f, _ = os.Create(kvraftLog)
var logger = log.New(f, "", log.Lmicroseconds)
var clerkId = 0

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader    int
	id        int
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	//logger.Printf("[MakeClerk]: make clerk")
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.id = clerkId
	clerkId += 1
	// init leader 0
	// You'll have to add code here.
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:     key,
		Clerk:   ck.id,
		Request: ck.requestId,
	}
	logger.Printf("[Clerk]: Clerk %v Request %v Get Key %v", ck.id, ck.requestId, key)
	ck.requestId += 1
	reply := GetReply{
		Err:   OK,
		Value: "",
	}
	for {
		//logger.Printf("[ClerkGet]: Clerk call GetRPC to server %v", ck.leader)
		if ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == OK {
				logger.Printf("[ClerkGet]: Clerk %v Request %v reply key %v value %v", ck.id, ck.requestId, args.Key, reply.Value)
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
		} else {
			//logger.Printf("[Get]: Timeout")
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		Clerk:   ck.id,
		Request: ck.requestId,
	}
	logger.Printf("[Clerk]: Clerk %v Request %v %v Key %v Value %v", ck.id, ck.requestId, op, key, value)
	ck.requestId += 1
	reply := PutAppendReply{
		Err: OK,
	}
	for {
		//logger.Printf("[ClerkPutAppend]: Clerk try to call KVServer %v", ck.leader)
		if ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.Err == OK {
				logger.Printf("[ClerkPutAppend]: Clerk %v Request %v reply %v", ck.id, ck.requestId, reply)
				return
			} else if reply.Err == ErrWrongLeader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
		} else {
			//logger.Printf("[PutAppend]: Timeout")
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
		//logger.Printf("[ClerkPutAppend]: server %v reply %v", ck.leader, reply)
		//time.Sleep(1 * time.Second)
	}

	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
