package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"log"
	"os"
)
import "time"
import "crypto/rand"
import "math/big"

var shardCtrlerLog = "ShardCtrler.log"
var f, _ = os.Create(shardCtrlerLog)
var logger = log.New(f, "", log.Lmicroseconds)
var ClerkId = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id      int
	request int
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
	// Your code here.
	ClerkId += 1
	ck.id = ClerkId
	ck.request = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:     num,
		Clerk:   ck.id,
		Request: ck.request,
	}
	ck.request += 1
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				//fmt.Printf("[Clerk]: query %v reply %v\n", num, reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	logger.Printf("[Clerk]: join %v\n", servers)
	args := &JoinArgs{
		Servers: servers,
		Clerk:   ck.id,
		Request: ck.request,
	}
	ck.request += 1
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	logger.Printf("[Clerk]: leave %v\n", gids)
	args := &LeaveArgs{
		GIDs:    gids,
		Clerk:   ck.id,
		Request: ck.request,
	}
	ck.request += 1
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	logger.Printf("[Clerk]: move shard %v gid %v\n", shard, gid)
	args := &MoveArgs{
		Shard:   shard,
		GID:     gid,
		Clerk:   ck.id,
		Request: ck.request,
	}
	ck.request += 1
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
