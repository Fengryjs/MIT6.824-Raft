package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"os"
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

	t []int

	logger log.Logger
	// a server reply RPC requires:
	// 1. args.configNum = kv.curConfig.Num
	// 2. kv.shardMigrationWaitCh[args.Shard] == kv.curConfig.Num
	// Your definitions here.
}

type Receiver struct {
	ch    chan bool
	timer *time.Timer
}

const DefaultTimeout = 1 * time.Second

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//logger.Printf("[Get]: %v\n", kv.shardMigrationWaitCh)
	//logger.Printf("[Get]: %v\n", kv.curConfig)
	reply.Err = ErrWrongLeader
	logger.Printf("[Get]: Gid %v Server %v Op %v [Shard, Config] %v Get Key %v", kv.gid, kv.me, [2]int{args.Clerk, args.Request}, [2]int{args.Shard, args.ConfigNum}, args.Key)
	kv.mu.Lock()
	logger.Printf("[Get]: Gid %v Server %v Op %v [Shard, Config] %v Get Key %v Lock Success", kv.gid, kv.me, [2]int{args.Clerk, args.Request}, [2]int{args.Shard, args.ConfigNum}, args.Key)
	condition := kv.curConfig.Shards[args.Shard] == kv.gid && kv.shardMigrationWaitCh[args.Shard] == args.ConfigNum
	kv.mu.Unlock()
	if condition {
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
					//logger.Printf("[ShardKV]: OpKey %v Config %v Gid %v Get Shard %v key %v value %v\n",
					//	[2]int{command.Clerk, command.RequestId}, args.ConfigNum, kv.gid, args.Shard, args.Key, kv.kvPair[args.Shard][args.Key])
					reply.Value = kv.kvPair[args.Shard][args.Key]
					kv.mu.Unlock()
					reply.Err = OK
				}
			case <-rc.timer.C:
			}
		}
	}
	//} else {
	//	logger.Printf("[ErrorGet]: Args.ConfigNum %v Config %v Shard,Key {%v, %v} MigratingWaiting %v\n", args.ConfigNum, kv.curConfig.Num, args.Shard, args.Key, kv.shardMigrationWaitCh[args.Shard])
	//}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	logger.Printf("[PutAppend]: Gid %v Server %v Op %v [Shard, Config] %v %v Key %v %v", kv.gid, kv.me, [2]int{args.Clerk, args.Request}, [2]int{args.Shard, args.ConfigNum}, args.Op, args.Key, args.Value)
	reply.Err = ErrWrongLeader
	kv.mu.Lock()
	logger.Printf("[PutAppend]: Gid %v Server %v Op %v [Shard, Config] %v %v Key %v %v Lock Success", kv.gid, kv.me, [2]int{args.Clerk, args.Request}, [2]int{args.Shard, args.ConfigNum}, args.Op, args.Key, args.Value)
	condition := kv.curConfig.Shards[args.Shard] == kv.gid && kv.shardMigrationWaitCh[args.Shard] == args.ConfigNum
	kv.mu.Unlock()
	if condition {
		command := Op{
			Option:    args.Op,
			Key:       args.Key,
			Value:     args.Value,
			ConfigNum: args.ConfigNum,
			Shard:     args.Shard,
			Clerk:     args.Clerk,
			RequestId: args.Request,
		}
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
					//logger.Printf("[ShardKV]: OpKey %v Config %v Gid %v %v Shard %v key %v value %v\n",
					//	[2]int{command.Clerk, command.RequestId}, args.ConfigNum, kv.gid, args.Op, args.Shard, args.Key, args.Value)
					reply.Err = OK
				}
			case <-rc.timer.C:
			}
		}
	}
	//else {
	//	logger.Printf("[ErrorPut]: Args.ConfigNum %v Config %v Shard,Key {%v, %v} MigratingWaiting %v\n", args.ConfigNum, kv.curConfig.Num, args.Shard, args.Key, kv.shardMigrationWaitCh[args.Shard])
	//}
}

func (kv *ShardKV) ApplyOperation() {
	for kv.Killed() == false {
		for msg := range kv.applyCh {
			kv.mu.Lock()
			if msg.CommandValid {
				op := msg.Command.(Op)
				var rc *Receiver = nil
				b := false
				if op.Option == "Get" || op.Option == "Put" || op.Option == "Append" {
					kv.logger.Printf("[ApplyOperation]: Gid %v Server %v Msg %v %v Shard %v Config %v", kv.gid, kv.me, msg.CommandIndex, op.Option, op.Shard, op.ConfigNum)
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
								kv.logger.Printf("[ApplyOp]: Wrong Operation Type")
							}
						}
					} else {
						b = true
					}
					if rc != nil && rc.timer.Stop() == true {
						//kv.logger.Printf("[Send]: Gid %v Server %v try to send", kv.gid, kv.me)
						rc.ch <- b
						//kv.logger.Printf("[Send]: Gid %v Server %v try to send Success", kv.gid, kv.me)
					}
				} else if op.Option == "ConfigChange" {
					kv.logger.Printf("[ApplyOperation]: Gid %v Server %v Msg %v %v %v", kv.gid, kv.me, msg.CommandIndex, op.Option, op.Config)
					if op.Config.Num == kv.curConfig.Num+1 {
						for i := 0; i < shardctrler.NShards; i++ {
							waitMigrateFromOther := kv.curConfig.Shards[i] != kv.gid && op.Config.Shards[i] == kv.gid && kv.curConfig.Shards[i] != 0
							waitMigrateToOther := kv.curConfig.Shards[i] == kv.gid && op.Config.Shards[i] != kv.gid && op.Config.Shards[i] != 0
							if waitMigrateFromOther {
								continue
							} else if waitMigrateToOther {
								kv.t = append(kv.t, i)
							}
							kv.shardMigrationWaitCh[i] = op.Config.Num
						}
						kv.curConfig = op.Config
						kv.cfgChange += 1
					}
				} else if op.Option == "Receive" {
					kv.logger.Printf("[ApplyOperation]: Gid %v Server %v Msg %v %v Shard %v Data %v Table %v", kv.gid, kv.me, msg.CommandIndex, op.Option, op.Shard, op.Data, op.DuplicateTable)
					if kv.shardMigrationWaitCh[op.Shard] < op.ConfigNum {
						for k, v := range op.Data {
							kv.kvPair[op.Shard][k] = v
						}
						kv.shardMigrationWaitCh[op.Shard] = op.ConfigNum
						for clerk, maxRequestId := range op.DuplicateTable {
							kv.duplicateTable[clerk] = max(maxRequestId, kv.duplicateTable[clerk])
						}
					}
				} else if op.Option == "Transfer" {
					kv.logger.Printf("[ApplyOperation]: Gid %v Server %v Msg %v %v Shard %v", kv.gid, kv.me, msg.CommandIndex, op.Option, op.Shard)
					length := len(kv.t)
					for i := 0; i < length; i++ {
						if kv.t[i] == op.Shard {
							for k := range kv.kvPair[op.Shard] {
								delete(kv.kvPair[op.Shard], k)
							}
							kv.t = append(kv.t[:i], kv.t[i+1:]...)
							//logger.Printf("[Transfer]: Gid %v Server %v Transfer Shard %v Config %v Current t %v\n", kv.gid, kv.me, op.Shard, op.ConfigNum, kv.t)
							break
						}
					}
					kv.logger.Printf(
						"[AfterTransfer]: KVPair %v\n"+
							"[AfterTransfer]: Duplicate %v\n"+
							"[AfterTransfer]: ShardConfig %v\n"+
							"[AfterTransfer]: t %v", kv.kvPair, kv.duplicateTable, kv.shardMigrationWaitCh, kv.t)
				}
				// Manual Chosen Number: Snapshot Interval
				if kv.maxraftstate != -1 && (msg.CommandIndex%50 == 0 || kv.rf.GetRaftStateSize() > kv.maxraftstate*8) {
					//fmt.Printf("Gid %v Server %v %v\n", kv.gid, kv.me, kv.rf.GetRaftStateSize())
					//kv.logger.Printf("[WritePersist]: Gid %v Server %v Try to write", kv.gid, kv.me)
					kv.WritePersist(msg.CommandIndex)
					//kv.logger.Printf("[WritePersist]: Gid %v Server %v write success", kv.gid, kv.me)
				}
			} else if msg.SnapshotValid {
				kv.logger.Printf("[KVSnapshot]: Gid %v Server %v Read snapshot %v", kv.gid, kv.me, msg.SnapshotIndex)
				kv.ReadPersist(msg.Snapshot)
			}
			//kv.logger.Printf("[ApplyOperation]: Gid %v Server %v try to unlock", kv.gid, kv.me)
			kv.mu.Unlock()
			//kv.logger.Printf("[ApplyOperation]: Gid %v Server %v Msg %v Unlock Success", kv.gid, kv.me, msg.CommandIndex)
		}
	}
}

func (kv *ShardKV) ListenConfigChange() {
	for kv.Killed() == false {
		var condition = false
		//logger.Printf("[ListenConfig]: Gid %v Server %v", kv.gid, kv.me)
		kv.mu.Lock()
		//logger.Printf("[ListenConfig]: Gid %v Server %v Lock Success", kv.gid, kv.me)
		cfg := kv.mck.Query(-1)
		lastCfg := kv.curConfig
		if lastCfg.Num != cfg.Num {
			condition = true
			if cfg.Num != lastCfg.Num+1 {
				cfg = kv.mck.Query(lastCfg.Num + 1)
			}
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.shardMigrationWaitCh[i] != lastCfg.Num {
					condition = false
				}
			}
			condition = condition && len(kv.t) == 0
		}
		kv.mu.Unlock()
		if condition {
			command := Op{
				Option: "ConfigChange",
				Config: cfg,
			}
			kv.rf.Start(command)
			//if isLeader {
			//	kv.mu.Lock()
			//	rc := Receiver{
			//		ch:    make(chan bool),
			//		timer: time.NewTimer(DefaultTimeout),
			//	}
			//	key := command.Config.Num
			//	logger.Printf("[ListenConfigChange]: Gid %v Config change from %v %v to %v %v\n", kv.gid, lastCfg.Num, lastCfg.Shards, cfg.Num, cfg.Shards)
			//	kv.configRc[key] = &rc
			//	kv.mu.Unlock()
			//	select {
			//	case msg := <-rc.ch:
			//		if msg {
			//			kv.mu.Lock()
			//			logger.Printf("[ConfigChange]: Gid %v Current ShardConfig %v kv.t %v\n", kv.gid, kv.shardMigrationWaitCh, kv.t)
			//			kv.mu.Unlock()
			//		}
			//	case <-rc.timer.C:
			//	}
			//}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
func (kv *ShardKV) ReplicateShard(args *AskForShardArgs, reply *AskForShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.curConfig.Num >= args.ConfigNum {
		reply.Err = OK
		reply.Data = CopyMapStr2Str(kv.kvPair[args.Shard])
		reply.DuplicateTable = CopyMapInt2Int(kv.duplicateTable)
	}
}
func (kv *ShardKV) CheckShard(args *CheckTransferArgs, reply *CheckTransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.curConfig.Num >= args.ConfigNum && kv.shardMigrationWaitCh[args.Shard] >= args.ConfigNum {
		reply.Err = OK
		reply.Complete = true
	}
}
func (kv *ShardKV) UpdateShard() {
	for kv.Killed() == false {
		//fmt.Printf("[UpdateShard] Still run\n")
		kv.mu.Lock()
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.curConfig.Shards[i] == kv.gid && kv.shardMigrationWaitCh[i] != kv.curConfig.Num {
				go kv.AskForShard(i, kv.curConfig.Num)
			}
		}
		for _, shard := range kv.t {
			go kv.CheckTransfer(shard, kv.curConfig.Num)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) AskForShard(shard int, configNum int) {
	//fmt.Printf("[AskForShard] Still run\n")
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
				kv.mu.Lock()
				if kv.shardMigrationWaitCh[shard] >= configNum {
					kv.mu.Unlock()
					return
				}
				kv.mu.Unlock()
				// 将DuplicateTable一并复制
				command := Op{
					Option:         "Receive",
					Shard:          shard,
					Data:           reply.Data,
					DuplicateTable: reply.DuplicateTable,
					ConfigNum:      configNum,
				}
				kv.rf.Start(command)
				return
			}
		}
	}
}
func (kv *ShardKV) CheckTransfer(shard int, configNum int) {
	//fmt.Printf("[CheckTransfer] still run\n")
	// 需要负责的主动寻求上任Group进行复制
	args := CheckTransferArgs{
		Shard:     shard,
		ConfigNum: configNum,
	}
	reply := CheckTransferReply{
		Err:      ErrWrongLeader,
		Complete: false,
	}
	kv.mu.Lock()
	lastCfg := kv.curConfig
	kv.mu.Unlock()
	for _, name := range lastCfg.Groups[lastCfg.Shards[shard]] {
		server := kv.make_end(name)
		if ok := server.Call("ShardKV.CheckShard", &args, &reply); ok {
			if reply.Err == OK {
				// 将DuplicateTable一并复制
				command := Op{
					Option:    "Transfer",
					Shard:     shard,
					ConfigNum: configNum,
				}
				kv.rf.Start(command)
				//if isLeader {
				//	kv.mu.Lock()
				//	rc := Receiver{
				//		ch:    make(chan bool),
				//		timer: time.NewTimer(DefaultTimeout),
				//	}
				//	key := [2]int{shard, configNum}
				//	kv.migrateRc[key] = &rc
				//	kv.mu.Unlock()
				//	select {
				//	case msg := <-rc.ch:
				//		if msg {
				//			logger.Printf("[TransferShard]: Gid %v Remove Shard %v Config %v\n", kv.gid, shard, configNum)
				//			reply.Err = OK
				//		}
				//	case <-rc.timer.C:
				//	}
				//}
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
		if d.Decode(&kv.kvPair) != nil || d.Decode(&kv.duplicateTable) != nil || d.Decode(&kv.curConfig) != nil ||
			d.Decode(&kv.shardMigrationWaitCh) != nil || d.Decode(&kv.t) != nil {
			log.Fatalf("[ReadPersist]: KV %v load persist error", kv.me)
		} else {
			applyChLogger.Printf("[ShardKV]: Gid %v Server %v Read Config\n"+
				"===  Data     %v\n"+
				"===  Config   %v\n"+
				"===  DupTable %v\n"+
				"===  ShardCfg %v\n"+
				"===  T        %v\n", kv.gid, kv.me,
				kv.kvPair, kv.curConfig, kv.duplicateTable, kv.shardMigrationWaitCh, kv.t)
		}
	}
}
func (kv *ShardKV) WritePersist(index int) {
	// TestSnapshot 会检查所有持久化变量的大小，而不是raft的log的大小，所以调整snapshot的频率没有用，只能调整这边的内容
	// 而这边必然要有一个结构来存储数据是否迁移成功的信息
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	if e.Encode(kv.kvPair) == nil && e.Encode(kv.duplicateTable) == nil &&
		e.Encode(kv.curConfig) == nil && e.Encode(kv.shardMigrationWaitCh) == nil && e.Encode(kv.t) == nil {
		kv.logger.Printf("[WritePersist]: Gid %v Server %v Snapshot %v", kv.gid, kv.me, index)
		kv.rf.Snapshot(index, b.Bytes())
		kv.logger.Printf("[WritePersist]: Gid %v Server %v Snapshot success", kv.gid, kv.me)
	}
}

func CopyMapStr2Str(m map[string]string) map[string]string {
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
	applyChLogger.Printf("Gid %v Server %v Start", kv.gid, kv.me)
	// 没有restart
	name := fmt.Sprintf("Gid-%v-Server-%v.log", gid, me)
	file, _ := os.Create(name)
	kv.logger = *log.New(file, "", log.Lmicroseconds)
	kv.ReadPersist(kv.rf.GetSnapshot())
	go kv.ApplyOperation()
	go kv.ListenConfigChange()
	go kv.UpdateShard()
	return kv
}
