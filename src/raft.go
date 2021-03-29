package raft

// API:
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

import "math"
import "sync"
import "sync/atomic"
import "time"
import "../labrpc"

type LogEntry struct {
	Command interface{}
	Term    int
}

type state string

const (
	follower  state = "follower"
	candidate       = "candidate"
	leader          = "leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	timer     *sync.Cond

	// Log state
	commitIndex int
	lastApplied int
	log         []LogEntry
	matchIndex  []int
	nextIndex   []int
	replyCond   *sync.Cond
	replyCount  map[int]int

	// Election state
	currentTerm         int
	electionStartTime   time.Time
	electionState       state
	electionTimeout     time.Duration
	heartbeatReplyCount int
	receivedHeartbeat   bool
	requiredReplies     int
	votedFor            int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.electionState == leader

	return term, isleader
}

//
// Handle updating the current term and electionState (or no change, if not
// necessary), for any RPC request or response. This is a helper function for
// RPCs, and as such, it expects the calling code to hold a lock.
//
func (rf *Raft) setTerm(term int) int {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.electionState = follower
		rf.replyCond.Broadcast()
		rf.votedFor = -1
		rf.persist()
	}
	return rf.currentTerm
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.electionState == leader

	if isLeader {
		index = len(rf.log)
		if index <= 0 {
			index = 1
		}
		term = rf.currentTerm
		newEntry := LogEntry{
			Command: command,
			Term:    term,
		}
		rf.log = append(rf.log, newEntry)
		rf.nextIndex[rf.me] = len(rf.log)
		rf.persist()
		DPrintf("********** CLIENT ********** requested command %d to %d on term %d at index %d", command, rf.me, term, index)
		//DPrintf("%d's log %+v", rf.me, rf.log)
		rf.startLogConsensus(index, term)
	}

	return index, term, isLeader
}

func (rf *Raft) systemTick() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		//DPrintf("tick")
		rf.timer.Broadcast()
		rf.mu.Unlock()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.electionState = follower
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.receivedHeartbeat = false
	rf.replyCond = sync.NewCond(&rf.mu)
	rf.replyCount = make(map[int]int)
	rf.requiredReplies = int((math.Ceil(float64(len(rf.peers)) / 2)))
	rf.timer = sync.NewCond(&rf.mu)
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTimer()
	go rf.systemTick()
	go rf.manageApply()
	go rf.periodicallyCheckElectionState()

	return rf
}
