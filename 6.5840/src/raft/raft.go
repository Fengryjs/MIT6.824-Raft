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
	"log"
	"math/rand"
	"os"
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

	snapshot Snapshot
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

var file = "RaftLog"
var f, _ = os.Create(file)
var logger = log.New(f, "", log.Lmicroseconds)

type ServerState string

// The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
// Such a range only makes sense if the leader sends heartbeats considerably more often
// than once per 150 milliseconds. Because the tester limits you to 10 heartbeats per second,
// you will have to use an election timeout larger than the paper's 150 to 300 milliseconds,
// but not too large, because then you may fail to elect a leader within five
const (
	Leader    = "Leader"
	Candidate = "Candidate"
	Follower  = "Follower"
	//ElectionTimeout = 300 * time.Millisecond
)

func RandomElectionTimeout() time.Duration {
	ms := 150 + rand.Int63()%100
	return time.Duration(ms) * time.Millisecond
}

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
	if e.Encode(rf.currentTerm) == nil && e.Encode(rf.votedFor) == nil &&
		e.Encode(rf.log) == nil && e.Encode(rf.snapshot) == nil {
		raftState := w.Bytes()
		rf.persister.Save(raftState, rf.persister.ReadSnapshot())
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	var snapshot Snapshot
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&snapshot) != nil {
	} else {
		logger.Printf("[ReadPersist]: Raft %v Persist %v %v %v %v", rf.me, currentTerm, votedFor, logs, snapshot)
		//logger.Printf("[ReadPersist]: Raft %v Raft bytes %v", rf.me, rf.persister.ReadRaftState())
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.snapshot = snapshot
		if snapshot.LastIncludedIndex > 0 {
			rf.lastApplied = rf.snapshot.LastIncludedIndex
		}
	}
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
	rf.mu.Lock()
	//logger.Printf("[Snapshot]: Raft %v before %v", rf.me, rf)
	defer rf.mu.Unlock()
	if rf.snapshot.LastIncludedIndex >= index {
		return
	}
	applyMsg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  0,
		SnapshotIndex: index,
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
	rf.lastApplied = max(rf.lastApplied, index) // 当调用snapshot后，crash的server被重启，需要将刷新lastApplied，
	rf.persist()
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
	//logger.Printf("[Snapshot]: %v %v", index, rf)
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
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
		// then the log with the latter term is more up-to-date.
		// If the logs end with the same term,
		// then whichever log is longer is more up-to-date.
		CandidateMoreUpToDate := false
		// 判断日志是否更加新
		if len(rf.log) == 0 {
			if rf.snapshot.LastIncludedIndex < args.LastLogIndex {
				CandidateMoreUpToDate = true
			} else if rf.snapshot.LastIncludedIndex == args.LastLogIndex {
				CandidateMoreUpToDate = args.LastLogTerm >= rf.snapshot.LastIncludedTerm
			} else {
				CandidateMoreUpToDate = false
			}
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
			rf.ElectionTimer = time.Now().Add(RandomElectionTimeout())
		}
	}
	rf.currentTerm = args.Term
	logger.Printf("[RequestVoteRPC] %v Raft %v %v InitialTerm %v VotedFor %v Result %v", args, rf.me, rf.currentTerm, rfInitialTerm, rf.votedFor, reply.VoteGranted)
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
	rf.persist()
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
		rf.ElectionTimer = time.Now().Add(RandomElectionTimeout())
	}
	logger.Printf("[AppendEntries]: Raft %v receive args %v\n", rf.me, args)
	//logger.Printf("[AppendEntries]: Raft %v", rf)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose Term matches prevLogTerm (§5.3)
	// 前面没有log，prevLogIndex = 0，此时Follower接收到RPC时，应该不用返回false，后面的都是需要覆盖的内容
	// prevLogIndex 是否存在
	// 1. 如果说Raft的snapshot不存在
	if args.PrevLogIndex != 0 {
		if rf.snapshot.LastIncludedIndex > args.PrevLogIndex {
			return
		} else if rf.snapshot.LastIncludedIndex == args.PrevLogIndex {
			if rf.snapshot.LastIncludedTerm != args.PrevLogTerm {

			}
		} else {
			// rf.snapshot.LastIncludedIndex < args.PrevLogIndex
			// 那么，就需要从当前的prevLogIndex中寻找是否有PrevLogIndex的内容
			if (args.PrevLogIndex-rf.snapshot.LastIncludedIndex > len(rf.log)) || (rf.log[args.PrevLogIndex-rf.snapshot.LastIncludedIndex-1].Term != args.PrevLogTerm) {
				// 不存在
				reply.Success = false
				if args.PrevLogIndex > len(rf.log)+rf.snapshot.LastIncludedIndex {
					reply.XLen = len(rf.log) + rf.snapshot.LastIncludedIndex
				} else {
					// 所有的Index
					reply.XTerm = rf.log[args.PrevLogIndex-rf.snapshot.LastIncludedIndex-1].Term
					reply.XIndex = rf.log[args.PrevLogIndex-rf.snapshot.LastIncludedIndex-1].CommandIndex
					for reply.XIndex > rf.snapshot.LastIncludedIndex && rf.log[reply.XIndex-rf.snapshot.LastIncludedIndex-1].Term == reply.XTerm {
						reply.XIndex = reply.XIndex - 1
					}
				}
				return
			}
		}
	}
	if len(args.Entries) == 0 {
		if args.LeaderCommit > rf.commitIndex && len(rf.log) > 0 && args.PrevLogIndex > 0 {
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
	for {
		if lastSameIndex == len(rf.log) {
			rf.log = append(rf.log, args.Entries[appendIndex:]...)
		}
		if appendIndex == len(args.Entries) {
			break
		}
		if rf.log[lastSameIndex].Term == args.Entries[appendIndex].Term {
			lastSameIndex++
			appendIndex++
		} else {
			rf.log = rf.log[:lastSameIndex]
			// 4. Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[appendIndex:]...)
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].CommandIndex)
	}
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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if reply.Term > args.Term {
		return
	}
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.ElectionTimer = time.Now().Add(RandomElectionTimeout())
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	if rf.snapshot.LastIncludedIndex < args.LastIncludedIndex {
		rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
		rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
		rf.persister.Save(rf.persister.ReadRaftState(), args.Data)
	} else {
		return
	}
	go rf.ApplySnapshotCommandMsg(args.Data)
	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	index := 0
	for index < len(rf.log) {
		if rf.log[index].CommandIndex == args.LastIncludedIndex && rf.log[index].Term == args.LastIncludedTerm {
			rf.log = rf.log[index+1:]
			return
		}
		index = index + 1
	}
	// 7. Discard the entire log
	rf.log = make([]Log, 0)
	logger.Printf("[InstallSnapshot]: Raft %v discard all log", rf.me)
}
func (rf *Raft) ApplySnapshotCommandMsg(snapshot []byte) {
	data := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(data)
	var lastIncludedIndex int
	var xlog []interface{}
	if decoder.Decode(&lastIncludedIndex) == nil && decoder.Decode(&xlog) == nil {
		//logger.Printf()("[ApplySnapshotCommandMsg]: lastIndex %v xlog %v", lastIncludedIndex, xlog)
		for {
			rf.mu.Lock()
			if rf.lastApplied < lastIncludedIndex {
				rf.lastApplied = rf.lastApplied + 1
				applyMsg := ApplyMsg{
					CommandValid:  true,
					Command:       xlog[rf.lastApplied],
					CommandIndex:  rf.lastApplied,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				logger.Printf("[InstallSnapshot]: Raft %v Apply Msg %v %v", rf.me, applyMsg.CommandIndex, applyMsg.Command)
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
			} else {
				rf.mu.Unlock()
				return
			}
		}
	}
}
func (rf *Raft) SendInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.nextIndex[server] > rf.snapshot.LastIncludedIndex || rf.state != Leader {
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
	logger.Printf("[SendInstallSnapshot]: Raft %v to %v", rf.me, server)
	//logger.Printf("[SendInstallSnapshot]: args %v", installSnapshotArgs)
	rf.mu.Unlock()
	installSnapshotReply := InstallSnapshotReply{
		Term: 0,
	}
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", &installSnapshotArgs, &installSnapshotReply); ok {
		rf.mu.Lock()
		if installSnapshotReply.Term > installSnapshotArgs.Term {
			rf.state = Follower
			//logger.Printf()("[SendInstallSnapshot]: Raft %v smaller term return Follower", rf.me)
		} else {
			rf.nextIndex[server] = rf.snapshot.LastIncludedIndex + 1
		}
		rf.mu.Unlock()
		//logger.Printf()("[Replicate]: %v install snapshot to %v success before replicate", rf.me, server)
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

func (rf *Raft) UpdateCommitIndex() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		} else {
			for i := len(rf.log) + rf.snapshot.LastIncludedIndex; i > max(rf.commitIndex, rf.snapshot.LastIncludedIndex); i-- {
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
		}
		time.Sleep(10 * time.Millisecond)
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
		logger.Printf("[Start]: %v receive %v command %v ", rf.me, index, command)
	}
	rf.mu.Unlock()
	return index, term, isLeader
}
func (rf *Raft) UpdateLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me && rf.nextIndex[server] < len(rf.log)+rf.snapshot.LastIncludedIndex+1 {
				go rf.SendHeartBeat(server)
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// ApplyLog 应该是由Make函数生成后，与该Server同一生命周期的routine
// 如果在ticker中反复生成，那么可能同时有多个applyLog routine 同时向applyCh发送msg
// 可能会导致 apply out of order error 尽管该error产生的可能性很小，152次test出现1次
func (rf *Raft) ApplyLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		//logger.Printf()("[ApplyLog]: Raft %v LastApplied %v CommitIndex %v %v %v", rf.me, rf.lastApplied, rf.commitIndex, rf.snapshotLock, rf.snapshotMsgApply)
		if rf.commitIndex <= rf.lastApplied || rf.lastApplied < rf.snapshot.LastIncludedIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		applyMsg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[rf.lastApplied-rf.snapshot.LastIncludedIndex].Command,
			CommandIndex:  rf.log[rf.lastApplied-rf.snapshot.LastIncludedIndex].CommandIndex,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		logger.Printf("[Ticker]: Raft %v Apply Msg %v %v", rf.me, applyMsg.CommandIndex, applyMsg.Command)
		rf.lastApplied = rf.lastApplied + 1
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
	}
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
	} else {
		args.LastLogIndex = rf.snapshot.LastIncludedIndex
		args.LastLogTerm = rf.snapshot.LastIncludedTerm
	}
	logger.Printf("%v begin switch to Candidate with term %v", rf.me, rf.currentTerm)
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
			logger.Printf("Raft %v become Leader With Term %v", rf.me, rf.currentTerm)
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log) + rf.snapshot.LastIncludedIndex + 1
				rf.matchIndex[i] = 0
			}
			rf.ElectionTimer = time.Now().Add(RandomElectionTimeout())
			rf.mu.Unlock()
			go rf.BroadcastHeartBeat()
			go rf.UpdateCommitIndex()
			go rf.UpdateLog()
			break
		}
		voteCnt++
		if voteCnt == len(rf.peers) {
			rf.mu.Lock()
			if rf.state == Candidate {
				rf.currentTerm -= 1
			}
			rf.state = Follower
			rf.votedFor = -1
			ms := 50 + rand.Int63()%300
			rf.ElectionTimer = time.Now().Add(time.Duration(ms) * time.Millisecond)
			logger.Printf("[RaftElection]: Raft %v Not enough vote return Follower current Term %v", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			// election completed without getting enough votes, break
			break
		}
	}
}
func (rf *Raft) SendHeartBeat(i int) {
	rf.mu.Lock()
	if rf.state == Leader {
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
				logger.Printf("[Error]: Raft %v to %v BroadcastHeartBeat %v < %v", rf.me, i, args.PrevLogIndex, rf.snapshot.LastIncludedIndex)
				go rf.SendInstallSnapshot(i)
				rf.mu.Unlock()
				return
			}
		}
		args.Entries = rf.log[rf.nextIndex[i]-rf.snapshot.LastIncludedIndex-1:]
		//args.Entries
		rf.mu.Unlock()
		//logger.Printf("[SendHeartBeat]: send heartbeat to %v", i)
		reply := AppendEntriesReply{
			Term:    0,
			Success: false,
		}
		if ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply); ok {
			rf.mu.Lock()
			logger.Printf("[SendHeartBeat]: Raft %v return %v", i, reply)
			if reply.Success {
				rf.ElectionTimer = time.Now().Add(RandomElectionTimeout())
				rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
			} else {
				if reply.Term > args.Term {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
				} else {
					if reply.XLen != 0 {
						rf.nextIndex[i] = reply.XLen + 1
					} else {
						index := len(rf.log) - 1
						for index >= 0 {
							if rf.log[index].Term == reply.XTerm {
								rf.nextIndex[i] = rf.log[index].CommandIndex + 1
								break
							}
							index = index - 1
						}
						if index == -1 {
							rf.nextIndex[i] = max(reply.XIndex, 1)
						}
					}
					logger.Printf("[BroadcastHeartBeat]: %v to %v false args %v reply %v", rf.me, i, args, reply)
					go rf.SendHeartBeat(i)
					// 修改nextIndex 下一次发送heartbeat就会更换
				}
			}
			rf.mu.Unlock()
		}
	} else {
		rf.mu.Unlock()
	}
}
func (rf *Raft) BroadcastHeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Leader {
			return
		}
		// 立刻广播
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				//logger.Printf("[SendHeartBeat]: try to heartbeat %v", i)
				// Send heartbeat(AppendingEntries RPC)
				go rf.SendHeartBeat(i)

			}
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		ms := 10 + rand.Int63()%20
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Follower && rf.ElectionTimer.Before(time.Now()) {
			ms := 50 + rand.Int63()%300
			rf.ElectionTimer = time.Now().Add(time.Duration(ms) * time.Millisecond)
			go rf.startElection()
		} else if rf.state == Candidate && rf.ElectionTimer.Before(time.Now()) {
			// 选举时间过长，超过了ElectionTimeout，仍未结束（存在Error的RPC调用）
			rf.state = Follower
			rf.currentTerm -= 1
			rf.votedFor = -1
			rf.ElectionTimer = time.Now().Add(RandomElectionTimeout())
			logger.Printf("[Raft]: Raft %v switch candidate to follower, currentTerm %v", rf.me, rf.currentTerm)
		} else if rf.state == Leader && rf.ElectionTimer.Before(time.Now()) {
			rf.state = Follower
			rf.votedFor = -1
			//rf.ElectionTimer = time.Now().Add(ElectionTimeout)
		}
		rf.mu.Unlock()
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
	rf.snapshot = Snapshot{
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		State:             nil,
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.ApplyLog()

	return rf
}
