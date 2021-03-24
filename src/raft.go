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

import "../labrpc"
import "math"
import "math/rand"
import "sync"
import "sync/atomic"
import "time"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

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

	applyCh chan ApplyMsg

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
	electionState       state
	heartbeatReplyCount int
	receivedHeartbeat   bool
	requiredReplies     int
	restartTimer        bool
	votedFor            int
}

//
// Expects calling code to hold a lock
//
func (rf *Raft) apply(i int, entry LogEntry) {
	DPrintf("%d sending apply message: index %d, command %d", rf.me, i, entry.Command)
	msg := ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: i,
	}
	rf.applyCh <- msg
	rf.lastApplied = i
}

func (rf *Raft) appendToFollower(id int, index int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[id] = index
	success := false
	for !success && index >= rf.commitIndex {
		var entries []LogEntry
		if rf.nextIndex[id] <= 0 {
			rf.nextIndex[id] = 1
		}
		for i := rf.nextIndex[id]; i < len(rf.log); i++ {
			entries = append(entries, rf.log[i])
		}

		args := AppendEntriesArgs{
			Entries:      entries,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[id] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[id]-1].Term,
			Term:         term,
		}
		reply := AppendEntriesReply{}

		//DPrintf("%d sending append entries to %d", rf.me, id)

		rf.mu.Unlock()
		rf.sendAppendEntries(id, &args, &reply)
		rf.mu.Lock()

		//DPrintf("%d's reply %+v", id, reply)
		rf.setTerm(reply.Term)
		success = reply.Success
		if success {
			rf.replyCount[index]++
			rf.replyCond.Broadcast()
		} else {
			//rf.nextIndex[id]--
			if reply.NextIndex < rf.nextIndex[id] && reply.Term >= term {
				rf.nextIndex[id] = reply.NextIndex
			}
		}
	}
	// update match and next indices
	if index >= rf.nextIndex[id] {
		rf.nextIndex[id] = index + 1
	}

	rf.replyCond.Broadcast()
}

func (rf *Raft) beginHeartbeats() {
	rf.mu.Lock()
	electionState := rf.electionState
	rf.mu.Unlock()
	rf.initFollowerIndices()
	heartbeatTimer := 300 * time.Millisecond
	for electionState == leader {
		rf.setCommitIndex()
		rf.sendToAllFollowers(rf.heartbeatAppendEntries)
		rf.setCommitIndex()
		time.Sleep(heartbeatTimer)
		rf.mu.Lock()
		electionState = rf.electionState
		//DPrintf("%d replies", rf.heartbeatReplyCount)
		rf.restartTimer = electionState == leader && rf.heartbeatReplyCount >= rf.requiredReplies
		rf.heartbeatReplyCount = 1
		rf.mu.Unlock()
	}
}

func (rf *Raft) checkElectionState() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.receivedHeartbeat = false
		rf.mu.Unlock()

		electionTimeout := time.Duration(rand.Intn(700))*time.Millisecond + 500*time.Millisecond
		time.Sleep(electionTimeout)

		rf.mu.Lock()
		/*DPrintf("%d is checking state on term %d. ReceivedHeartbeat: %t", rf.me, rf.currentTerm, rf.receivedHeartbeat)
		DPrintf(" --- electionState: %+v", rf.electionState)
		DPrintf(" --- votedFor: %+v", rf.votedFor)
		DPrintf(" --- restartTimer: %+v", rf.restartTimer)*/
		electionState := rf.electionState
		if rf.restartTimer {
			rf.restartTimer = false
			rf.mu.Unlock()
		} else if rf.receivedHeartbeat || electionState == leader {
			rf.electionState = follower
			rf.replyCond.Broadcast()
			rf.votedFor = -1
			rf.mu.Unlock()
		} else if rf.votedFor == -1 {
			rf.mu.Unlock()
			go rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

// expects calling code to hold a lock
func (rf *Raft) getNumMatches(index int) int {
	count := 0
	for _, match := range rf.matchIndex {
		if match >= index {
			count++
		}
	}
	return count
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
// Initialize all followers' nextIndex and matchIndex arrays
//
func (rf *Raft) initFollowerIndices() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i, _ := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
}

// Send an empty Entries list to a follower, without waiting for their response
func (rf *Raft) heartbeatAppendEntries(id int, wg *sync.WaitGroup) {
	wg.Done()
	rf.mu.Lock()
	//DPrintf("%d sending heartbeat message to %d\n", rf.me, id)
	sentTerm := rf.currentTerm
	var entries []LogEntry
	args := AppendEntriesArgs{
		Entries:      entries,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[id] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[id]-1].Term,
		Term:         sentTerm,
	}
	reply := AppendEntriesReply{}

	rf.mu.Unlock()
	ok := rf.sendAppendEntries(id, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setTerm(reply.Term)
	if !ok {
		//DPrintf("error sending append entry to %d", x)
		return
	}
	if reply.Term != sentTerm {
		rf.electionState = follower
		rf.replyCond.Broadcast()
		return
	}
	if reply.Success {
		rf.heartbeatReplyCount++
	}
}

func (rf *Raft) manageApply() {
	for !rf.killed() {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.apply(rf.lastApplied, rf.log[rf.lastApplied])
		}
		rf.mu.Unlock()
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// Asynchronously call a function for all followers (without waiting for their
// response).
//
func (rf *Raft) sendToAllFollowers(f func(id int, wg *sync.WaitGroup)) {
	rf.mu.Lock()
	me := rf.me
	nPeers := len(rf.peers)
	var followersWg sync.WaitGroup
	rf.mu.Unlock()
	for i := 0; i < nPeers; i++ {
		if i == me {
			continue
		}
		followersWg.Add(1)
		go f(i, &followersWg)
	}
	followersWg.Wait()
}

func (rf *Raft) setCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i, _ := range rf.log {
		if i > rf.commitIndex && rf.getNumMatches(i) >= rf.requiredReplies && rf.log[i].Term == rf.currentTerm {
			rf.commitIndex = i
		}
	}
}

//
// Send a non-empty Entries list to followers
//
func (rf *Raft) startLogConsensus(index int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.replyCount[index] = 1
	startTerm := rf.currentTerm
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendToFollower(i, index, term)
	}
	for !rf.killed() && rf.electionState == leader && rf.replyCount[index] < rf.requiredReplies {
		rf.replyCond.Wait()
	}

	// consensus reached
	if rf.electionState == leader && rf.currentTerm == startTerm && rf.replyCount[index] >= rf.requiredReplies {
		DPrintf("%d reached consensus %d/%d replies", rf.me, rf.replyCount[index], rf.requiredReplies)
		for i := rf.commitIndex; i < index+1; i++ {
			//rf.apply(i, rf.log[i])
		}
		rf.commitIndex = index //
		for id, _ := range rf.matchIndex {
			if index >= rf.matchIndex[id] {
				rf.matchIndex[id] = index
			}
		}
	}
}

func (rf *Raft) startElection() {
	cond := sync.NewCond(&rf.mu)
	quitElection := false
	electionTimeout := time.Duration(rand.Intn(800))*time.Millisecond + 200*time.Millisecond
	go func() {
		time.Sleep(electionTimeout)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//DPrintf("%d's election timed out", rf.me)
		quitElection = true
		cond.Broadcast()
		if rf.electionState == candidate {
			go rf.startElection()
		}
	}()
	rf.mu.Lock()
	me := rf.me
	nPeers := len(rf.peers)
	term := rf.currentTerm + 1
	rf.electionState = candidate
	rf.replyCond.Broadcast()
	rf.votedFor = me
	replies := make([]RequestVoteReply, nPeers)
	myReply := RequestVoteReply{
		VoteGranted: true,
		Term:        term,
	}
	replies[me] = myReply
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	castCount := 1
	//DPrintf("%d is starting an election on term %d\n", me, term)
	//DPrintf("%d's log: %+v", me, rf.log)
	rf.mu.Unlock()

	for i := 0; i < nPeers; i++ {
		if i == me {
			continue
		}
		go func(x int, term int) {
			args := RequestVoteArgs{
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				Term:         term,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(x, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			replies[x] = reply
			if reply.Term > term {
				rf.currentTerm = reply.Term
				rf.electionState = follower
				rf.replyCond.Broadcast()
				rf.votedFor = -1
				quitElection = true
			}
			if reply.VoteGranted {
				castCount++
				//DPrintf("%d votes out of %d required\n", castCount, rf.requiredReplies)
			}
			cond.Broadcast()
		}(i, term)
	}

	{ // Wait for votes to complete or electionTimeout to expire
		rf.mu.Lock()
		for !rf.killed() && !quitElection && castCount < rf.requiredReplies {
			cond.Wait()
		}
		rf.mu.Unlock()
	}

	{ // count votes
		rf.mu.Lock()
		if quitElection || rf.electionState != candidate {
			rf.mu.Unlock()
			return
		}
		var voteCount int = 0
		for _, reply := range replies {
			if reply.Term > term {
				rf.electionState = follower
				rf.replyCond.Broadcast()
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				voteCount++
			}
		}

		if rf.currentTerm <= term && voteCount >= rf.requiredReplies {
			//DPrintf("%d won election with %d votes on term %d\n", me, voteCount, term)
			rf.electionState = leader
			rf.currentTerm = term
			rf.mu.Unlock()
			go rf.beginHeartbeats()
		} else {
			rf.electionState = follower
			rf.replyCond.Broadcast()
			rf.mu.Unlock()
		}
	}
}

type AppendEntriesArgs struct {
	Entries      []LogEntry
	LeaderId     int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Term         int
}
type AppendEntriesReply struct {
	NextIndex int
	Success   bool
	Term      int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("%d received AE: %+v\n", rf.me, args)
	//DPrintf(" --- %d's existing log: %+v\n", rf.me, rf.log)
	//DPrintf(" --- %d's current term:%d\n", rf.me, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = len(rf.log)
	} else {
		//	} else if args.PrevLogTerm == rf.log[len(rf.log)-1].Term || args.PrevLogTerm >= rf.currentTerm || args.Term > rf.currentTerm {
		rf.receivedHeartbeat = true
		rf.electionState = follower
		rf.replyCond.Broadcast()
		rf.votedFor = -1
		reply.Success = rf.prevMatch(args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = rf.setTerm(args.Term)
		//reply.NextIndex = len(rf.log)

		reply.NextIndex = 1
		for i := len(rf.log) - 1; i >= 1; i-- {
			if rf.log[i].Term == args.PrevLogTerm && i == args.PrevLogIndex {
				reply.NextIndex = i
			}
		}

		if !reply.Success {
			return
		}

		//DPrintf("%d's log before %+v", rf.me, rf.log)
		i := args.PrevLogIndex + 1
		if len(args.Entries) == 0 && args.LeaderCommit == 0 {
			//rf.apply(0, rf.log[0])
		}
		for _, entry := range args.Entries {
			if i > len(rf.log)-1 {
				rf.log = append(rf.log, entry)
				//rf.apply(i, entry)
			} else if rf.log[i] != entry {
				rf.log = rf.log[:i+1]
				rf.log[i] = entry
				//rf.apply(i, entry)
			}
			i++
			//DPrintf("%d's log during %+v", rf.me, rf.log)
		}

		//DPrintf("%d's log after %+v", rf.me, rf.log)
		//prevCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
		}

		//reply.NextIndex = len(rf.log)
		reply.NextIndex = rf.commitIndex + 1
	}
}

//
//
//
func (rf *Raft) logUpToDate(lastLogIndex int, lastLogTerm int) bool {
	myLastEntry := rf.log[len(rf.log)-1]
	if lastLogTerm > myLastEntry.Term {
		return true
	} else if lastLogTerm == myLastEntry.Term {
		return lastLogIndex >= len(rf.log)-1
	} else {
		return false
	}
}

//
// Check if follower contains an entry matching `prevLogIndex` and `prevLogTerm`
// when a leader makes an AppendEntries RPC. This is a helper function for
// AppendEntries, and as such, it expects the calling function to hold a lock.
//
func (rf *Raft) prevMatch(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex > len(rf.log)-1 || prevLogIndex < 0 {
		return false
	} else {
		return rf.log[prevLogIndex].Term == prevLogTerm
	}
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
	}
	return rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type RequestVoteArgs struct {
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	Term         int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("%d received vote request %+v\n", rf.me, args)
	/*DPrintf(" --- %d thinks it's term %d", rf.me, rf.currentTerm)
	DPrintf(" --- votedFor %d", rf.votedFor)*/
	rf.currentTerm = rf.setTerm(args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = rf.logUpToDate(args.LastLogIndex, args.LastLogTerm)
	}

	if reply.VoteGranted {
		//DPrintf("%d is voting for %d", rf.me, args.CandidateId)
		rf.electionState = follower
		rf.replyCond.Broadcast()
		rf.votedFor = args.CandidateId
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
		DPrintf("client requested command %d to %d on term %d at index %d", command, rf.me, term, index)
		//DPrintf("%d's log %+v", rf.me, rf.log)
		go rf.startLogConsensus(index, term)
	}

	return index, term, isLeader
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
	//DPrintf("%d peers", len(peers))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.votedFor = -1
	rf.electionState = follower
	rf.receivedHeartbeat = false
	rf.requiredReplies = int((math.Ceil(float64(len(rf.peers)) / 2)))
	rf.restartTimer = false
	rf.replyCond = sync.NewCond(&rf.mu)
	rf.replyCount = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.checkElectionState()
	go rf.manageApply()

	return rf
}
