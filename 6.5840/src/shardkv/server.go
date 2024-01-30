package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	Option         string
	Key            string
	Value          string
	Shard          int
	Config         shardctrler.Config
	Data           map[string]string
	DuplicateTable map[int]int
	Clerk          int
	RequestId      int
	ConfigNum      int
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

	mck       *shardctrler.Clerk
	cfgChange int
	curConfig shardctrler.Config

	dead int32

	kvPair         map[int]map[string]string
	msgReceiver    map[[2]int]*Receiver
	duplicateTable map[int]int

	configRc map[int]*Receiver

	migrateRc map[[2]int]*Receiver

	migrationDupTable map[int]int

	shardMigrationWaitCh map[int]int
	// a server reply RPC requires:
	// 1. args.configNum = kv.curConfig.Num
	// 2. kv.shardMigrationWaitCh[args.Shard] == kv.curConfig.Num
	// Your definitions here.
}

type Receiver struct {
	// TODO
	ch    chan bool
	timer *time.Timer
}

const DefaultTimeout = 1 * time.Second

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//fmt.Printf("[Get]: %v\n", kv.shardMigrationWaitCh)
	//fmt.Printf("[Get]: %v\n", kv.curConfig)
	kv.mu.Lock()
	reply.Err = ErrWrongLeader
	if kv.curConfig.Shards[args.Shard] == kv.gid && kv.shardMigrationWaitCh[args.Shard] == args.ConfigNum {
		kv.mu.Unlock()
		command := Op{
			Option:    "Get",
			Key:       args.Key,
			Shard:     args.Shard,
			ConfigNum: args.ConfigNum,
			Clerk:     args.Clerk,
			RequestId: args.Request,
		}
		_, _, isLeader := kv.rf.Start(command)
		if isLeader {
			kv.mu.Lock()
			rc := Receiver{
				ch:    make(chan bool),
				timer: time.NewTimer(DefaultTimeout),
			}
			key := [2]int{command.Clerk, command.RequestId}
			kv.msgReceiver[key] = &rc
			kv.mu.Unlock()
			select {
			case msg := <-rc.ch:
				if msg {
					kv.mu.Lock()
					fmt.Printf("[ShardKV]: OpKey %v Config %v Gid %v Get Shard %v key %v value %v\n",
						[2]int{command.Clerk, command.RequestId}, args.ConfigNum, kv.gid, args.Shard, args.Key, kv.kvPair[args.Shard][args.Key])
					reply.Value = kv.kvPair[args.Shard][args.Key]
					kv.mu.Unlock()
					//logger.Printf("[KVGet]: server %v return key %v reply %v", kv.me, args.Key, kv.kvPair[args.Key])
					reply.Err = OK
				}
			case <-rc.timer.C:

			}
		}
	} else {
		fmt.Printf("[ErrorGet]: Args.ConfigNum %v Config %v Shard,Key {%v, %v} MigratingWaiting %v\n", args.ConfigNum, kv.curConfig.Num, args.Shard, args.Key, kv.shardMigrationWaitCh[args.Shard])
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// PutAppend 操作如果在 raft 层实现共识时，config已经更换了，应该更换 group
	kv.mu.Lock()
	reply.Err = ErrWrongLeader
	if kv.curConfig.Shards[args.Shard] == kv.gid && kv.shardMigrationWaitCh[args.Shard] == args.ConfigNum {
		command := Op{
			Option:    args.Op,
			Key:       args.Key,
			Value:     args.Value,
			ConfigNum: args.ConfigNum,
			Shard:     args.Shard,
			Clerk:     args.Clerk,
			RequestId: args.Request,
		}
		kv.mu.Unlock()
		_, _, isLeader := kv.rf.Start(command)
		if isLeader {
			kv.mu.Lock()
			key := [2]int{command.Clerk, command.RequestId}
			rc := Receiver{
				ch:    make(chan bool),
				timer: time.NewTimer(DefaultTimeout),
			}
			kv.msgReceiver[key] = &rc
			kv.mu.Unlock()
			select {
			case msg := <-rc.ch:
				if msg {
					fmt.Printf("[ShardKV]: OpKey %v Config %v Gid %v %v Shard %v key %v value %v\n",
						[2]int{command.Clerk, command.RequestId}, args.ConfigNum, kv.gid, args.Op, args.Shard, args.Key, args.Value)
					reply.Err = OK
				}
			case <-rc.timer.C:

			}
		}
	} else {
		fmt.Printf("[ErrorPut]: Args.ConfigNum %v Config %v Shard,Key {%v, %v} MigratingWaiting %v\n", args.ConfigNum, kv.curConfig.Num, args.Shard, args.Key, kv.shardMigrationWaitCh[args.Shard])
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) ApplyOperation() {
	for kv.Killed() == false {
		for msg := range kv.applyCh {
			kv.mu.Lock()
			if msg.CommandValid {
				// logger.Printf("[ApplyOperation]: KV %v Receive %v", kv.me, msg.Command)
				// 每个操作时需要携带当时接受RPC请求时的Config如果说在一个请求发送到apply期间，发生了config的转换，那么就需要拒绝该请求
				op := msg.Command.(Op)
				var rc *Receiver
				b := false
				if op.Option == "Get" || op.Option == "Put" || op.Option == "Append" {
					key := [2]int{op.Clerk, op.RequestId}
					rc = kv.msgReceiver[key]
					if kv.duplicateTable[op.Clerk] < op.RequestId {
						if kv.curConfig.Num == op.ConfigNum {
							b = true
							kv.duplicateTable[op.Clerk] = op.RequestId
							switch op.Option {
							case "Get":
							case "Put":
								kv.kvPair[op.Shard][op.Key] = op.Value
							case "Append":
								initial := kv.kvPair[op.Shard][op.Key]
								kv.kvPair[op.Shard][op.Key] = initial + op.Value
							default:
								logger.Printf("[ApplyOp]: Wrong Operation Type")
							}
						}
					} else {
						b = true
					}
					// Manual Chosen Number: Snapshot Interval
				} else if op.Option == "ConfigChange" {
					key := op.Config.Num
					rc = kv.configRc[key]
					if op.Config.Num > kv.curConfig.Num {
						for i := 0; i < shardctrler.NShards; i++ {
							if kv.curConfig.Shards[i] != kv.gid && op.Config.Shards[i] == kv.gid && kv.curConfig.Shards[i] != 0 {
								// 需要等待migrating的shard
							} else {
								kv.shardMigrationWaitCh[i] = op.Config.Num
							}
						}
						kv.curConfig = op.Config
						kv.cfgChange += 1
					}
				} else if op.Option == "Migrating" {
					key := [2]int{op.Shard, op.ConfigNum}
					rc = kv.migrateRc[key]
					if kv.shardMigrationWaitCh[op.Shard] < op.ConfigNum {
						b = true
						for k, v := range op.Data {
							kv.kvPair[op.Shard][k] = v
						}
						kv.shardMigrationWaitCh[op.Shard] = op.ConfigNum
						for clerk, maxRequestId := range op.DuplicateTable {
							kv.duplicateTable[clerk] = max(maxRequestId, kv.duplicateTable[clerk])
						}
					}
				}
				if kv.maxraftstate != -1 && msg.CommandIndex%100 == 0 {
					kv.WritePersist(msg.CommandIndex)
				}
				kv.mu.Unlock()
				kv.SendChannelMsg(rc, b)
			} else if msg.SnapshotValid {
				kv.ReadPersist(msg.Snapshot)
				logger.Printf("[KVSnapshot]: KV %v read snapshot %v", kv.me, kv.kvPair)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) SendChannelMsg(rc *Receiver, msg bool) {
	if rc != nil && rc.timer.Stop() == true {
		rc.ch <- msg
	}
}

// [ListenConfigChange]: Gid 100 Config change from 1 [100 100 100 100 100 100 100 100 100 100] to 2 [101 101 101 101 101 100 100 100 100 100] in Command 2
// [ListenConfigChange]: Gid 102 Config change from 1 [100 100 100 100 100 100 100 100 100 100] to 2 [101 101 101 101 101 100 100 100 100 100] in Command 2
// [Clerk]: leave [100]
// [ListenConfigChange]: Gid 101 Config change from 1 [100 100 100 100 100 100 100 100 100 100] to 3 [102 102 101 101 101 102 100 100 100 100] in Command 3
// 如果说，我们一下子进行了多个config的切换，
// 那么，几个config之间的迁移还没有完成，那么就会有多个shard的切换，每个server的waiting，都需要从1-N线性地变换
// 就是说，必须从n到n+1到n+2这样变换，都必须

func (kv *ShardKV) ListenConfigChange() {
	for kv.Killed() == false {
		kv.mu.Lock()
		cfg := kv.mck.Query(-1)
		// todo
		// maybe the shard one server take for are unchanged, the shard for other changed of mapping changed
		// the config should change too?
		lastCfg := kv.curConfig
		kv.mu.Unlock()
		if lastCfg.Num != cfg.Num {
			if cfg.Num != lastCfg.Num+1 {
				cfg = kv.mck.Query(lastCfg.Num + 1)
			}
			kv.mu.Lock()
			updated := true
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.shardMigrationWaitCh[i] != lastCfg.Num {
					updated = false
				}
			}
			kv.mu.Unlock()
			if updated == false {
				continue
			}
			command := Op{
				Option: "ConfigChange",
				Config: cfg,
			}
			_, _, isLeader := kv.rf.Start(command)
			if isLeader {
				kv.mu.Lock()
				rc := Receiver{
					ch:    make(chan bool),
					timer: time.NewTimer(DefaultTimeout),
				}
				key := command.Config.Num
				fmt.Printf("[ListenConfigChange]: Gid %v Config change from %v %v to %v %v\n", kv.gid, lastCfg.Num, lastCfg.Shards, cfg.Num, cfg.Shards)
				kv.configRc[key] = &rc
				kv.mu.Unlock()
				select {
				case msg := <-rc.ch:
					if msg {
						shards := make([]int, 0)
						for i := 0; i < shardctrler.NShards; i++ {
							if lastCfg.Shards[i] != kv.gid && cfg.Shards[i] == kv.gid && lastCfg.Shards[i] != 0 {
								shards = append(shards, i)
							}
						}
						kv.mu.Lock()
						fmt.Printf("[ConfigChange]: Gid %v Current Shard %v waiting %v\n", kv.gid, kv.shardMigrationWaitCh, shards)
						kv.mu.Unlock()
					}
				case <-rc.timer.C:

				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (kv *ShardKV) ReplicateShard(args *AskForShardArgs, reply *AskForShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.curConfig.Num >= args.ConfigNum {
		reply.Err = OK
		reply.Data = kv.kvPair[args.Shard]
		reply.DuplicateTable = kv.duplicateTable
	}
}
func (kv *ShardKV) UpdateShard() {
	for kv.Killed() == false {
		kv.mu.Lock()
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.curConfig.Shards[i] == kv.gid && kv.shardMigrationWaitCh[i] != kv.curConfig.Num {
				go kv.AskForShard(i, kv.curConfig.Num)
			}
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
func (kv *ShardKV) AskForShard(shard int, configNum int) {
	// 需要负责的主动寻求上任Group进行复制
	args := AskForShardArgs{
		Shard:     shard,
		ConfigNum: configNum,
	}
	reply := AskForShardReply{
		Err: ErrWrongLeader,
	}
	kv.mu.Lock()
	lastCfg := kv.mck.Query(configNum - 1)
	kv.mu.Unlock()
	for _, name := range lastCfg.Groups[lastCfg.Shards[shard]] {
		server := kv.make_end(name)
		if ok := server.Call("ShardKV.ReplicateShard", &args, &reply); ok {
			if reply.Err == OK {
				// 将DuplicateTable一并复制
				command := Op{
					Option:         "Migrating",
					Shard:          shard,
					Data:           reply.Data,
					DuplicateTable: reply.DuplicateTable,
					ConfigNum:      configNum,
				}
				_, _, isLeader := kv.rf.Start(command)
				if isLeader {
					kv.mu.Lock()
					rc := Receiver{
						ch:    make(chan bool),
						timer: time.NewTimer(DefaultTimeout),
					}
					key := [2]int{shard, configNum}
					kv.migrateRc[key] = &rc
					kv.mu.Unlock()
					select {
					case msg := <-rc.ch:
						if msg {
							kv.mu.Lock()
							fmt.Printf("[AskFor]: Gid %v Replicate Shard %v Config %v Table %v Data %v\n", kv.gid, shard, configNum, kv.duplicateTable, reply.Data)
							kv.mu.Unlock()
							reply.Err = OK
						}
					case <-rc.timer.C:
					}
				}
				return
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
		if d.Decode(&kv.kvPair) != nil || d.Decode(&kv.duplicateTable) != nil ||
			d.Decode(&kv.curConfig) != nil || d.Decode(&kv.shardMigrationWaitCh) != nil {
			log.Fatalf("[ReadPersist]: KV %v load persist error", kv.me)
		} else {
			fmt.Printf("[ShardKV]: Start Config\n"+
				"===  Data     %v\n"+
				"===  Config   %v\n"+
				"===  DupTable %v\n", kv.kvPair, kv.curConfig, kv.duplicateTable)
		}
	}
}
func (kv *ShardKV) WritePersist(index int) {
	// TestSnapshot 会检查所有持久化变量的大小，而不是raft的log的大小，所以调整snapshot的频率没有用，只能调整这边的内容
	// 而这边必然要有一个结构来存储数据是否迁移成功的信息
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	if e.Encode(kv.kvPair) == nil && e.Encode(kv.duplicateTable) == nil &&
		e.Encode(kv.curConfig) == nil && e.Encode(kv.shardMigrationWaitCh) == nil {
		kv.rf.Snapshot(index, b.Bytes())
	}
}

func Copy(m map[string]string) map[string]string {
	r := make(map[string]string)
	for k, v := range m {
		r[k] = v
	}
	return r
}
func CopyMapInt2Int(m map[int]int) map[int]int {
	r := make(map[int]int)
	for k, v := range m {
		r[k] = v
	}
	return r
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
	//kv.rShard = make([]int, 0)
	kv.cfgChange = 1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.msgReceiver = make(map[[2]int]*Receiver)
	kv.configRc = make(map[int]*Receiver)
	kv.migrateRc = make(map[[2]int]*Receiver)

	kv.duplicateTable = make(map[int]int)
	kv.kvPair = make(map[int]map[string]string)

	kv.migrationDupTable = make(map[int]int)
	kv.shardMigrationWaitCh = make(map[int]int)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardMigrationWaitCh[i] = 0
		kv.kvPair[i] = make(map[string]string)
	}
	kv.ReadPersist(kv.rf.GetSnapshot())
	go kv.ApplyOperation()
	go kv.ListenConfigChange()
	go kv.UpdateShard()
	return kv
}
