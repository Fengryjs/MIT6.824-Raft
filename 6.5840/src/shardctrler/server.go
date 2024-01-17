package shardctrler

import (
	"6.5840/raft"
	"math"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	waitCh         map[[2]int]chan raft.ApplyMsg
	waitTimer      map[[2]int]*time.Timer
	duplicateTable map[int]int
}

type Op struct {
	Option    string
	JoinArg   JoinArgs
	LeaveArg  LeaveArgs
	MoveArg   MoveArgs
	QueryArg  QueryArgs
	Clerk     int
	RequestId int

	// Your data here.
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	cmd := Op{
		Option:    "Join",
		JoinArg:   *args,
		Clerk:     args.Clerk,
		RequestId: args.Request,
	}
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		reply.WrongLeader = sc.WaitConsensus(cmd.Clerk, cmd.RequestId)
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	cmd := Op{
		Option:    "Leave",
		LeaveArg:  *args,
		Clerk:     args.Clerk,
		RequestId: args.Request,
	}
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		reply.WrongLeader = sc.WaitConsensus(cmd.Clerk, cmd.RequestId)
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	cmd := Op{
		Option:    "Move",
		MoveArg:   *args,
		Clerk:     args.Clerk,
		RequestId: args.Request,
	}
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		reply.WrongLeader = sc.WaitConsensus(cmd.Clerk, cmd.RequestId)
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	cmd := Op{
		Option:    "Query",
		QueryArg:  *args,
		Clerk:     args.Clerk,
		RequestId: args.Request,
	}
	_, _, isLeader := sc.rf.Start(cmd)
	if isLeader {
		reply.WrongLeader = sc.WaitConsensus(cmd.Clerk, cmd.RequestId)
		sc.mu.Lock()
		if args.Num == -1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		//fmt.Printf("[ShardCtrler]: Query %v reply %v\n", args.Num, reply)
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}

	// Your code here.
}

func (sc *ShardCtrler) WaitConsensus(Clerk int, RequestId int) bool {
	sc.mu.Lock()
	waitCh := make(chan raft.ApplyMsg)
	waitTimer := time.NewTimer(time.Second)
	key := [2]int{Clerk, RequestId}
	sc.waitCh[key] = waitCh
	sc.waitTimer[key] = waitTimer
	sc.mu.Unlock()
	select {
	case <-waitCh:
		return false
	case <-waitTimer.C:
		return true
	}
}

func (sc *ShardCtrler) JoinConfig(args JoinArgs) {
	//fmt.Printf("[ShardCtrler]: join %v\n", args)
	curConfig := sc.configs[len(sc.configs)-1]
	var cfg = Config{
		Num:    curConfig.Num + 1,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	// deep copy for (map)Groups
	for i, v := range curConfig.Shards {
		cfg.Shards[i] = v
	}
	for k, v := range curConfig.Groups {
		cfg.Groups[k] = v
	}
	for key, value := range args.Servers {
		cfg.Groups[key] = append(cfg.Groups[key], value...)
	}
	sc.ReBalanceShards(&cfg)
	sc.configs = append(sc.configs, cfg)
	//fmt.Printf("[ShardCtrler]: Config After Join %v \n", cfg)
}
func (sc *ShardCtrler) LeaveConfig(args LeaveArgs) {
	//fmt.Printf("[ShardCtrler]: Leave %v\n", args)
	lastCfg := sc.configs[len(sc.configs)-1]
	cfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	for i, v := range lastCfg.Shards {
		cfg.Shards[i] = v
	}
	for k, v := range lastCfg.Groups {
		cfg.Groups[k] = v
	}
	for _, gid := range args.GIDs {
		delete(cfg.Groups, gid)
	}
	sc.ReBalanceShards(&cfg)
	sc.configs = append(sc.configs, cfg)
	//fmt.Printf("[ShardCtrler]: Config After Leave %v\n", cfg)
}
func (sc *ShardCtrler) MoveConfig(args MoveArgs) {
	//fmt.Printf("[ShardCtrler]: Move %v\n", args)
	lastCfg := sc.configs[len(sc.configs)-1]
	cfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	for i, v := range lastCfg.Shards {
		cfg.Shards[i] = v
	}
	for k, v := range lastCfg.Groups {
		cfg.Groups[k] = v
	}
	sc.ReBalanceShards(&cfg)
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] == args.GID {
			cfg.Shards[i] = cfg.Shards[args.Shard]
			cfg.Shards[args.Shard] = args.GID
			break
		}
	}
	sc.configs = append(sc.configs, cfg)
	//fmt.Printf("[ShardCtrler]: Config After Move %v\n", cfg)
}
func (sc *ShardCtrler) MaxMinUse(keys []int, Shards [10]int, Groups map[int][]string) (int, int, int, map[int]int) {
	useful := 0
	minUse := 999
	maxUse := -1
	timeMap := make(map[int]int)
	for k, _ := range Groups {
		n := 0
		for _, gid := range Shards {
			if gid == k {
				n += 1
			}
		}
		useful += n
		timeMap[k] = n
		maxUse = max(maxUse, n)
		minUse = min(minUse, n)
	}
	return maxUse, minUse, useful, timeMap
}
func (sc *ShardCtrler) ReBalanceShards(cfg *Config) {
	//fmt.Printf("[ReBalance]: config %v\n", cfg)
	if len(cfg.Groups) == 0 {
		cfg.Shards = [10]int{}
		return
	}
	keys := make([]int, 0)
	for k, _ := range cfg.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// 负载均衡
	maxUse, minUse, useful, timeMap := sc.MaxMinUse(keys, cfg.Shards, cfg.Groups)
	//fmt.Printf("[ReBalance]: max %v min %v useful %v timeMap %v\n", maxUse, minUse, useful, timeMap)
	for useful != NShards || maxUse > NShards/len(cfg.Groups)+1 || minUse < NShards/len(cfg.Groups) {
		for i, gid := range cfg.Shards {
			// 去除所有的已leave的gid
			if cfg.Groups[gid] == nil {
				for _, k := range keys {
					if timeMap[k] == minUse {
						cfg.Shards[i] = k
						timeMap[k] = timeMap[k] + 1
						useful += 1
						break
					}
				}
			}
		}
		maxUse, minUse, useful, timeMap = sc.MaxMinUse(keys, cfg.Shards, cfg.Groups)
		if useful == NShards {
			// balance
			for _, k := range keys {
				if timeMap[k] < NShards/len(keys) {
					for i, gid := range cfg.Shards {
						if timeMap[gid] == maxUse {
							cfg.Shards[i] = k
							maxUse, minUse, useful, timeMap = sc.MaxMinUse(keys, cfg.Shards, cfg.Groups)
							break
						}
					}
				} else if timeMap[k] > int(math.Ceil(float64(NShards)/float64(len(keys)))) {
					for _, replaceKey := range keys {
						if timeMap[replaceKey] == minUse {
							for i, gid := range cfg.Shards {
								if gid == k {
									cfg.Shards[i] = replaceKey
									maxUse, minUse, useful, timeMap = sc.MaxMinUse(keys, cfg.Shards, cfg.Groups)
									break
								}
							}
							break
						}
					}
				}
			}
		}
	}
}

func (sc *ShardCtrler) ApplyOperation() {
	for {
		for msg := range sc.applyCh {
			sc.mu.Lock()
			if msg.CommandValid {
				//logger.Printf("[ApplyOperation]: sc %v Receive %v", sc.me, msg.Command)
				op := msg.Command.(Op)
				key := [2]int{op.Clerk, op.RequestId}
				if sc.duplicateTable[op.Clerk] < op.RequestId {
					sc.duplicateTable[op.Clerk] = op.RequestId
					//sc.duplicateTable[key] = &op
					switch op.Option {
					case "Join":
						sc.JoinConfig(op.JoinArg)
					case "Leave":
						sc.LeaveConfig(op.LeaveArg)
					case "Move":
						sc.MoveConfig(op.MoveArg)
					case "Query":

					default:
						logger.Printf("[ApplyOp]: Wrong Operation Type")
					}
				}
				if sc.waitCh[key] != nil && sc.waitTimer[key].Stop() == true {
					waitCh := sc.waitCh[key]
					logger.Printf("[ApplyOperation]: sc %v Receive %v and go through waitCh", sc.me, msg.Command)
					sc.mu.Unlock()
					waitCh <- msg
					continue
				}
				sc.mu.Unlock()
			}
		}
	}

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.waitCh = make(map[[2]int]chan raft.ApplyMsg)
	sc.waitTimer = make(map[[2]int]*time.Timer)
	sc.duplicateTable = make(map[int]int)
	// Your code here.
	go sc.ApplyOperation()
	return sc
}
