package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
	"log"
	"os"
)
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

var shardKVRaftLog = "shardKVRaft.log"
var f, _ = os.Create(shardKVRaftLog)
var applyLog, _ = os.Create("applyCh.log")
var logger = log.New(f, "", log.Lmicroseconds)
var applyChLogger = log.New(applyLog, "", log.Lmicroseconds)
var clerkId = 0

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	id       int
	request  int
	// You will have to modify this struct.
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.id = clerkId
	clerkId += 1
	ck.request = 1
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:     key,
		Clerk:   ck.id,
		Request: ck.request,
	}
	ck.request += 1
	for {
		shard := key2shard(key)
		args.Shard = shard
		args.ConfigNum = ck.config.Num
		gid := ck.config.Shards[shard]
		logger.Printf("[ClerkGet]: OpKey [Clerk:%v, Request:%v] Config %v Get [Shard:%v, Key:%v]\n", args.Clerk, args.Request, args.ConfigNum, shard, key)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		Clerk:   ck.id,
		Request: ck.request,
	}
	ck.request += 1
	for {
		shard := key2shard(key)
		args.Shard = shard
		args.ConfigNum = ck.config.Num
		gid := ck.config.Shards[shard]
		logger.Printf("[ClerkPutAppend]: OpKey [Clerk:%v, Request:%v] Config %v %v [Shard:%v, Key:%v] Value %v\n", args.Clerk, args.Request, ck.config.Num, op, shard, key, value)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
