package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg
	state   ServerState
	// Persistent state on all servers
	currentTerm int
	votedFor    int // -1 代表 null
	log         []Log
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	ElectionTimer time.Time

	snapshot         Snapshot
	snapshotLock     bool
	snapshotMsgApply bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type ServerState string

const (
	Leader          = "Leader"
	Candidate       = "Candidate"
	Follower        = "Follower"
	ElectionTimeout = 1000 * time.Millisecond
)

type Log struct {
	Command      interface{}
	CommandIndex int
	Term         int
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	State             interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshot)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	//fmt.Printf("[persist]: %v %v %v\n", rf.currentTerm, rf.votedFor, rf.log)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	//fmt.Printf("[readPersist]: begin\n")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	var snapshot Snapshot
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshot) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshot = snapshot
	}
	//fmt.Printf("[readPersist]: %v %v %v\n", currentTerm, votedFor, log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// Raft Lab2D 中， config.go 测试用例的 applierSnap 在每收到一次数量的 applyMsg 之后，
// 会调用一次 SnapShot 此时若是之前的 ApplyLog 上了锁，就会导致死锁
// Snapshot 不一定需要向 applyCh 发送消息
// 当存在disconnect的server时，若是长时间的隔离，再次融入时，已经有多个snapshot
// 此时需要先调用InstallSnapshot，使得那边的内容丢弃，仅保留可能需要用到的log
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.snapshotLock = true
	fmt.Printf("[Snapshot]: snapshot before lock\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("[Snapshot]: Raft %v, Index %v Bytes %v\n", rf.me, index, snapshot)
	applyMsg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  0,
		SnapshotIndex: index,
	}
	if rf.snapshot.LastIncludedIndex >= index {
		go rf.ApplySnapshotMsg(applyMsg, index)
		return
	}
	logIndex := 0
	for rf.log[logIndex].CommandIndex != index {
		logIndex++
	}
	applyMsg.SnapshotTerm = rf.log[logIndex].Term
	rf.log = rf.log[logIndex+1:]
	// Save
	rf.snapshot = Snapshot{
		LastIncludedIndex: applyMsg.SnapshotIndex,
		LastIncludedTerm:  applyMsg.SnapshotTerm,
		State:             nil,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshot)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	go rf.ApplySnapshotMsg(applyMsg, index)
}
func (rf *Raft) ApplySnapshotMsg(applyMsg ApplyMsg, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyCh <- applyMsg
	rf.snapshotLock = false
	fmt.Printf("[Snapshot]: apply msg %v, %v\n", index, applyMsg)
}
func (rf *Raft) ReadS() {
	r := bytes.NewBuffer(rf.persister.ReadSnapshot())
	d := labgob.NewDecoder(r)
	var snapshot interface{}
	if d.Decode(snapshot) != nil {
		fmt.Printf("[ReadS]%v\n", snapshot)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Printf("[RequestVoteRPC]: %v try to get vote from %v\n", args.CandidateId, rf.me)
	rf.mu.Lock()
	//fmt.Printf("[RequestVote]: lock")
	defer rf.mu.Unlock()
	rf.persist()
	rfInitialTerm := rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		if args.Term > rf.currentTerm {
			rf.state = Follower
		}
		// Raft determines which of two logs is more up-to-date
		// by comparing the index and term of the last entries in the logs.
		// If the logs have last entries with different terms,
		// then the log with the later term is more up-to-date.
		// If the logs end with the same term,
		// then whichever log is longer is more up-to-date.
		CandidateMoreUpToDate := false
		if len(rf.log) == 0 {
			CandidateMoreUpToDate = true
		} else {
			if rf.log[len(rf.log)-1].Term != args.LastLogTerm {
				CandidateMoreUpToDate = args.LastLogTerm > rf.log[len(rf.log)-1].Term
			} else {
				CandidateMoreUpToDate = args.LastLogIndex >= rf.log[len(rf.log)-1].CommandIndex
			}
		}
		if CandidateMoreUpToDate && ((rf.votedFor == -1 || rf.votedFor == args.CandidateId && rf.currentTerm == args.Term) || (rf.currentTerm < args.Term)) {
			rf.state = Follower
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.ElectionTimer = time.Now().Add(ElectionTimeout)
		}
	}
	rf.currentTerm = args.Term
	fmt.Printf("[RequestVoteRPC] %v Raft %v %v InitialTerm %v VotedFor %v Result %v\n", args, rf.me, rf.currentTerm, rfInitialTerm, rf.votedFor, reply.VoteGranted)
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) > 0 {
		//fmt.Printf("[AppendEntries]: args %v to %v\n", args, rf.me)
		//fmt.Printf("[AppendEntries]: rf %v\n", rf)
	}
	rf.persist()
	//fmt.Printf("[AppendEntries]: %v %v %v\n", args, rf.me, rf.commitIndex)
	// If AppendEntries RPC received from new leader: convert to follower
	// 1. Reply false if Term < currenTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.state = Follower
		rf.ElectionTimer = time.Now().Add(ElectionTimeout)
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose Term matches prevLogTerm (§5.3)
	// 前面没有log，prevLogIndex = 0，此时Follower接收到RPC时，应该不用返回false，后面的都是需要覆盖的内容
	// prevLogIndex 是否存在
	// 1. 如果说Raft的snapshot不存在
	if args.PrevLogIndex != 0 {
		if rf.snapshot.LastIncludedIndex >= args.PrevLogIndex {
			// 有 PrevLogIndex 对应的内容

		} else {
			// rf.snapshot.LastIncludedIndex < args.PrevLogIndex
			// 那么，就需要从当前的prevLogIndex中寻找是否有PrevLogIndex的内容
			if (args.PrevLogIndex-rf.snapshot.LastIncludedIndex > len(rf.log)) || (rf.log[args.PrevLogIndex-rf.snapshot.LastIncludedIndex-1].Term != args.PrevLogTerm) {
				// 不存在
				reply.Success = false
				if args.PrevLogIndex >= len(rf.log) {
					reply.XLen = len(rf.log)
				} else {
					reply.XTerm = rf.log[args.PrevLogIndex].Term
					reply.XIndex = args.PrevLogIndex
					for rf.log[reply.XIndex].Term == reply.XTerm && reply.XIndex > 0 {
						reply.XIndex = reply.XIndex - 1
					}
				}
				return
			}
		}
	}
	if len(args.Entries) == 0 {
		if args.LeaderCommit > rf.commitIndex && len(rf.log) > 0 && args.PrevLogIndex > 0 {
			fmt.Printf("[AppendingEntries]: %v commitIndex %v with args %v\n", rf.me, rf.commitIndex, args)
			if args.PrevLogIndex == rf.snapshot.LastIncludedIndex {
				rf.commitIndex = rf.snapshot.LastIncludedIndex
			} else {
				rf.commitIndex = min(args.LeaderCommit, rf.log[args.PrevLogIndex-rf.snapshot.LastIncludedIndex-1].CommandIndex)
			}
		}
		return
	}
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	lastSameIndex := args.PrevLogIndex - rf.snapshot.LastIncludedIndex
	appendIndex := 0
	for i := args.PrevLogIndex - rf.snapshot.LastIncludedIndex; i < len(rf.log) && i-(args.PrevLogIndex-rf.snapshot.LastIncludedIndex) < len(args.Entries); i++ {
		if rf.log[i].Term == args.Entries[i-(args.PrevLogIndex-rf.snapshot.LastIncludedIndex)].Term {
			lastSameIndex++
			appendIndex++
		} else {
			break
		}
	}
	rf.log = rf.log[:lastSameIndex]
	// 4. Append any new entries not already in the log
	for i := appendIndex; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Printf("%v %v\n", args.LeaderCommit, args.Entries[len(args.Entries)-1].CommandIndex)
		rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].CommandIndex)
	}
	if len(args.Entries) > 0 {
		//fmt.Printf("[AppendEntries]: Args %v\nRaft %v %v %v\n", args, rf.me, rf.log, rf.commitIndex)
	}
	//fmt.Printf("[AppendEntries]: Reply %v\n", reply)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if reply.Term > args.Term {
		fmt.Printf("[InstallSnapshot]: receiver Raft %v term %v is larger than Leader %v term %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.state = Follower
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	if rf.snapshot.LastIncludedIndex < args.LastIncludedIndex {
		rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
		rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
		rf.persister.Save(rf.persister.ReadRaftState(), args.Data)
		rf.snapshotMsgApply = true
	} else {
		rf.mu.Unlock()
		return
	}
	go rf.ApplySnapshotCommandMsg(args.Data)
	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	index := 0
	for index < len(rf.log) {
		if rf.log[index].CommandIndex == args.LastIncludedIndex && rf.log[index].Term == args.LastIncludedTerm {
			rf.log = rf.log[index+1:]
			fmt.Printf("[InstallSnapshot]: Raft %v retain log %v\n", rf.me, rf.log)
			rf.mu.Unlock()
			return
		}
		index = index + 1
	}
	// 7. Discard the entire log
	rf.log = make([]Log, 0)
	rf.mu.Unlock()
	fmt.Printf("[InstallSnapshot]: Raft %v discard all log\n", rf.me)
}
func (rf *Raft) ApplySnapshotCommandMsg(snapshot []byte) {
	data := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(data)
	var lastIncludedIndex int
	var xlog []interface{}
	if decoder.Decode(&lastIncludedIndex) == nil && decoder.Decode(&xlog) == nil {
		fmt.Printf("[ApplySnapshotCommandMsg]: lastIndex %v xlog %v\n", lastIncludedIndex, xlog)
		for {
			rf.mu.Lock()
			if rf.snapshotLock {
				rf.mu.Unlock()
				time.Sleep(1 * time.Millisecond)
				continue
			}
			if rf.lastApplied < lastIncludedIndex {
				rf.lastApplied = rf.lastApplied + 1
				//rf.commitIndex = max(rf.commitIndex, rf.lastApplied)
				applyMsg := ApplyMsg{
					CommandValid:  true,
					Command:       xlog[rf.lastApplied],
					CommandIndex:  rf.lastApplied,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				fmt.Printf("[InstallSnapshot]: Raft %v Apply Msg %v %v\n", rf.me, applyMsg.CommandIndex, applyMsg.Command)
				rf.applyCh <- applyMsg
				rf.mu.Unlock()
			} else {
				rf.snapshotMsgApply = false
				rf.mu.Unlock()
				return
			}
			time.Sleep(3 * time.Millisecond)
		}
	}
}
func (rf *Raft) SendInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.nextIndex[server] >= rf.snapshot.LastIncludedIndex || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	installSnapshotArgs := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshot.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              false,
	}
	fmt.Printf("[SendInstallSnapshot]: Raft %v to %v\n", rf.me, server)
	fmt.Printf("[SendInstallSnapshot]: args %v\n", installSnapshotArgs)
	rf.mu.Unlock()
	installSnapshotReply := InstallSnapshotReply{
		Term: 0,
	}
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", &installSnapshotArgs, &installSnapshotReply); ok {
		rf.mu.Lock()
		if installSnapshotReply.Term > installSnapshotArgs.Term {
			rf.state = Follower
			fmt.Printf("[SendInstallSnapshot]: Raft %v smaller term return Follower\n", rf.me)
		} else {
			rf.nextIndex[server] = rf.snapshot.LastIncludedIndex + 1
		}
		rf.mu.Unlock()
		fmt.Printf("[Replicate]: %v install snapshot to %v success before replicate\n", rf.me, server)
	} else {
		fmt.Printf("[Replicate]: %v install snapshot to %v fail exceeds time limit\n", rf.me, server)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Replicate(server int) {
	if len(rf.log) > 0 && rf.state == Leader {
		// 存在log
		if rf.nextIndex[server] <= rf.log[len(rf.log)-1].CommandIndex {
			// 不断复制最后一个
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  0,
				Entries:      make([]Log, 0),
				LeaderCommit: rf.commitIndex,
			}
			if args.PrevLogIndex > 0 {
				if args.PrevLogIndex > rf.snapshot.LastIncludedIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1-rf.snapshot.LastIncludedIndex].Term
				} else if args.PrevLogIndex == rf.snapshot.LastIncludedIndex {
					args.PrevLogTerm = rf.snapshot.LastIncludedTerm
				} else {
					fmt.Printf("[Error]: Raft %v to %v replicate %v < %v\n", rf.me, server, args.PrevLogIndex, rf.snapshot.LastIncludedIndex)
					go rf.SendInstallSnapshot(server)
					rf.mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					return
				}
			}
			args.Entries = rf.log[rf.nextIndex[server]-rf.snapshot.LastIncludedIndex-1:]
			rf.mu.Unlock()
			//fmt.Printf("[Replicate]: Replicate to Raft %v %v\n", server, args)
			reply := AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			if ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply); ok {
				rf.mu.Lock()
				if reply.Success {
					fmt.Printf("[Replicate]: Replicate to Raft %v %v success\n", server, args)
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					fmt.Printf("[Replicate]: server %v matchIndex %v nextIndex %v\n", server, rf.matchIndex[server], rf.nextIndex[server])
					//rf.matchIndex[server] = args.Entries[len(args.Entries)-1].CommandIndex
					//fmt.Printf("[Replicate]: %v replicate %v %v Success\n", rf.me, server, args)
				} else if args.Term == reply.Term && rf.nextIndex[server] == args.PrevLogIndex+1 {
					// 来自同一个routine
					if reply.XLen != 0 {
						rf.nextIndex[server] = reply.XLen
					} else {
						index := len(rf.log) - 1
						for index >= 0 {
							if rf.log[index].Term == reply.XTerm {
								rf.nextIndex[server] = rf.log[index].CommandIndex
								break
							}
							index = index - 1
						}
						if index == -1 {
							rf.nextIndex[server] = max(reply.XIndex, 1)
						}
					}
					//rf.nextIndex[server]--
					go rf.Replicate(server)
					fmt.Printf("[Replicate]: %v %v dismatch at %v\n", rf.me, server, args.PrevLogIndex)
				} else if args.Term < reply.Term {
					// 当前任期已经过时了，应当更换为Follower
					//fmt.Printf("[Replicate]: not leader\n")
				}
				rf.mu.Unlock()
			} else {
				//fmt.Printf("[Replicate]: %v to %v call Error\n", rf.me, server)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) checkReplicate() {
	rf.mu.Lock()
	//fmt.Printf("[CheckReplicate]: Raft %v Replicate\n", rf.me)
	for i := len(rf.log) + rf.snapshot.LastIncludedIndex; i > rf.commitIndex; i-- {
		matchNum := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= i {
				matchNum++
			}
		}
		if matchNum > len(rf.peers)/2 && rf.log[i-1-rf.snapshot.LastIncludedIndex].Term == rf.currentTerm {
			rf.commitIndex = i
			break
		}
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state == Leader {
				//fmt.Printf("[CheckReplicate]: Raft %v replicate to %v\n", rf.me, i)
				go rf.Replicate(i)
			} else {
				break
			}
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	// index是该条记录如果被commit的话，它将被插在哪个地方(leader.log的最后)
	index = len(rf.log) + rf.snapshot.LastIncludedIndex + 1
	term = rf.currentTerm
	isLeader = rf.state == Leader
	// Your code here (2B).
	if isLeader {
		// If command received from client: append entry to local log,
		// respond after entry applied to state machine (§5.3)
		rf.log = append(rf.log, Log{
			Command: command,
			// first log index is 1
			CommandIndex: index,
			Term:         rf.currentTerm,
		})
		rf.persist()
		fmt.Printf("[Start]: %v receive %v command %v \n", rf.me, index, command)
	}
	rf.mu.Unlock()
	return index, term, isLeader
}
func (rf *Raft) ApplyLog() {
	//fmt.Printf("%v %v\n", rf.log, rf.lastApplied)
	for {
		rf.mu.Lock()
		//fmt.Printf("[ApplyLog]: Raft %v LastApplied %v CommitIndex %v %v %v\n", rf.me, rf.lastApplied, rf.commitIndex, rf.snapshotLock, rf.snapshotMsgApply)
		if rf.commitIndex <= rf.lastApplied || rf.snapshotLock == true || rf.snapshotMsgApply == true {
			rf.mu.Unlock()
			return
		}
		fmt.Printf("[ApplyLog]: %v\n", rf)
		logIndex := 0
		for len(rf.log) > 0 && rf.log[logIndex].CommandIndex-1 != rf.lastApplied {
			logIndex++
		}
		applyMsg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[logIndex].Command,
			CommandIndex:  rf.log[logIndex].CommandIndex,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		fmt.Printf("[Ticker]: Raft %v Apply Msg %v %v\n", rf.me, applyMsg.CommandIndex, applyMsg.Command)
		rf.applyCh <- applyMsg
		rf.lastApplied = rf.lastApplied + 1
		time.Sleep(3 * time.Millisecond)
		rf.mu.Unlock()
		//time.Sleep(3 * time.Millisecond)
		//fmt.Printf("[Ticker]: %v %v\n", rf.commitIndex, rf.lastApplied)

	}
	//fmt.Printf("[Ticker]: Raft %v ticker end\n", rf.me)

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	// On conversion to candidate, start election:
	// Increment currentTerm
	rf.currentTerm = rf.currentTerm + 1
	// Vote for self
	rf.votedFor = rf.me
	// Reset election timer
	// Send RequestVote RPCs to all other servers
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	if len(rf.log) > 0 {
		args.LastLogIndex = rf.log[len(rf.log)-1].CommandIndex
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	fmt.Printf("%v begin switch to Candidate with term %v\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	voteChan := make(chan bool, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Candidate {
			break
		}
		if i != rf.me {
			go func(i int) {
				//fmt.Printf("Trying RequestVote %v to %v\n", rf.me, i)
				reply := RequestVoteReply{
					Term:        0,
					VoteGranted: false,
				}
				if ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply); ok {
					if reply.VoteGranted {
						voteChan <- true
					} else {
						voteChan <- false
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							//fmt.Printf("raft %v return follower\n", rf.me)
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					}
				} else {
					voteChan <- false
					//fmt.Printf("Error RequestVote RPC Call %v to %v At term %v\n", rf.me, i, args.Term)
					return
				}
			}(i)
		}
	}
	voteCnt := 1
	voteGrantedCnt := 1
	for voteGranted := range voteChan {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Candidate {
			break
		}
		if voteGranted {
			voteGrantedCnt++
		}
		if voteGrantedCnt > len(rf.peers)/2 {
			// gain over a half votes, switch to leader
			rf.mu.Lock()
			fmt.Printf("Raft %v become Leader With Term %v\n", rf.me, rf.currentTerm)
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log) + rf.snapshot.LastIncludedIndex + 1
				rf.matchIndex[i] = 0
			}
			rf.ElectionTimer = time.Now().Add(ElectionTimeout)
			rf.mu.Unlock()
			go rf.sendHeartBeat()
			break
		}
		voteCnt++
		if voteCnt == len(rf.peers) {
			rf.mu.Lock()
			if rf.state == Candidate {
				rf.currentTerm -= 1
			}
			fmt.Printf("%v Not enough vote return Follower\n", rf.me)
			rf.state = Follower
			rf.votedFor = -1
			ms := 50 + rand.Int63()%300
			rf.ElectionTimer = time.Now().Add(time.Duration(ms) * time.Millisecond)
			rf.mu.Unlock()
			// election completed without getting enough votes, break
			break
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for rf.state == Leader {
		// 立刻广播
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				// Send heartbeat(AppendingEntries RPC)
				go func(i int) {
					rf.mu.Lock()
					if rf.state == Leader {
						//fmt.Printf("[sendHeartBeat]: %v to %v\n", rf.me, i)
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  0,
							Entries:      make([]Log, 0),
							LeaderCommit: rf.commitIndex,
						}
						if args.PrevLogIndex > 0 {
							if args.PrevLogIndex > rf.snapshot.LastIncludedIndex {
								args.PrevLogTerm = rf.log[args.PrevLogIndex-1-rf.snapshot.LastIncludedIndex].Term
							} else if args.PrevLogIndex == rf.snapshot.LastIncludedIndex {
								args.PrevLogTerm = rf.snapshot.LastIncludedTerm
							} else {
								fmt.Printf("[Error]: Raft %v to %v sendHeartBeat %v < %v\n", rf.me, i, args.PrevLogIndex, rf.snapshot.LastIncludedIndex)
								go rf.SendInstallSnapshot(i)
								rf.mu.Unlock()
								return
							}
						}
						rf.mu.Unlock()
						reply := AppendEntriesReply{
							Term:    0,
							Success: false,
						}
						if ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply); ok {
							if reply.Success {
								//fmt.Printf("%v call %v HeartBeat success in Term %v %v\n", rf.me, i, args.Term, args)
							} else {
								if reply.Term > args.Term {
									//fmt.Printf("%v call %v HeartBeat RPC false in Term %v\n", rf.me, i, args.Term)
									rf.mu.Lock()
									rf.currentTerm = reply.Term
									rf.state = Follower
									rf.votedFor = -1
									rf.mu.Unlock()
									return
								} else {
									fmt.Printf("[sendHeartBeat]: %v to %v false %v\n", rf.me, i, reply)
									go rf.Replicate(i)
									// 修改nextIndex 下一次发送heartbeat就会更换
								}
							}
						}
					} else {
						rf.mu.Unlock()
						return
					}
				}(i)

			}
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}

}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		ms := 10 + rand.Int63()%20
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		go rf.ApplyLog()
		if rf.state == Follower && rf.ElectionTimer.Before(time.Now()) {
			rf.ElectionTimer = time.Now().Add(ElectionTimeout)
			go rf.startElection()
		} else if rf.state == Candidate && rf.ElectionTimer.Before(time.Now()) {
			// 选举时间过长，超过了ElectionTimeout，仍未结束（存在Error的RPC调用）
			rf.state = Follower
			rf.currentTerm -= 1
			fmt.Printf("[Ticker]: Raft %v currentTerm -1\n", rf.me)
			rf.votedFor = -1
			ms := 50 + rand.Int63()%300
			rf.ElectionTimer = time.Now().Add(time.Duration(ms) * time.Millisecond)
		} else if rf.state == Leader {
			rf.ElectionTimer = time.Now().Add(ElectionTimeout)
			go rf.checkReplicate()
		}
		rf.mu.Unlock()
		//fmt.Printf("Raft %v state is %v\n", rf.me, rf.state)
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.ElectionTimer = time.Now()
	rf.state = Follower
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
