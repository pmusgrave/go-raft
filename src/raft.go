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

type logEntry struct {
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

	// Log state
	commitIndex int
	lastApplied int
	log         []logEntry
	matchIndex  []int
	nextIndex   []int

	// Election state
	currentTerm       int
	electionState     state
	receivedHeartbeat bool
	restartTimer      bool
	votedFor          int
}

func (rf *Raft) beginHeartbeats() {
	rf.mu.Lock()
	electionState := rf.electionState
	me := rf.me
	nPeers := len(rf.peers)
	rf.mu.Unlock()

	for electionState == leader {
		var wg sync.WaitGroup
		for i := 0; i < nPeers; i++ {
			if i == me {
				continue
			}
			wg.Add(1)
			go func(x int) {
				wg.Done()
				//DPrintf("%d sending heartbeat message to %d\n", rf.me, x)
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				rf.mu.Lock()
				args.Term = rf.currentTerm
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(x, &args, &reply)
				if !ok {
					//DPrintf("error sending append entry to %d", x)
				}
			}(i)
		}
		wg.Wait()

		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		electionState = rf.electionState
		rf.restartTimer = true
		rf.mu.Unlock()
	}
}

func (rf *Raft) checkElectionState() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.receivedHeartbeat = false
		rf.mu.Unlock()

		electionTimeout := time.Duration(rand.Intn(150))*time.Millisecond + 150*time.Millisecond
		time.Sleep(electionTimeout)

		rf.mu.Lock()
		DPrintf("%d is checking state on term %d. ReceivedHeartbeat: %t", rf.me, rf.currentTerm, rf.receivedHeartbeat)
		electionState := rf.electionState
		if rf.restartTimer {
			rf.restartTimer = false
			rf.mu.Unlock()
		} else if rf.receivedHeartbeat || electionState == leader {
			rf.electionState = follower
			rf.mu.Unlock()
		} else if rf.votedFor == -1 {
			rf.mu.Unlock()
			go rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.electionState == leader
	rf.mu.Unlock()

	return term, isleader
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

func (rf *Raft) startElection() {
	quitElection := false
	electionTimeout := time.Duration(rand.Intn(150))*time.Millisecond + 150*time.Millisecond
	go func() {
		time.Sleep(electionTimeout)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		quitElection = true
	}()

	rf.mu.Lock()
	me := rf.me
	nPeers := len(rf.peers)
	rf.currentTerm++
	rf.electionState = candidate
	rf.votedFor = me
	replies := make([]RequestVoteReply, nPeers)
	myReply := RequestVoteReply{
		VoteSuccess: true,
	}
	replies[me] = myReply
	term := rf.currentTerm
	castCount := 1
	requiredVotes := int((math.Ceil(float64(len(rf.peers)) / 2)))
	cond := sync.NewCond(&rf.mu)
	DPrintf("%d is starting an election on term %d\n", me, rf.currentTerm)
	rf.mu.Unlock()

	for i := 0; i < nPeers; i++ {
		if i == me {
			continue
		}
		go func(x int, term int) {
			args := RequestVoteArgs{
				Requester: me,
				Term:      term,
			}
			reply := RequestVoteReply{
				VoteSuccess: false,
			}
			rf.sendRequestVote(x, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			replies[x] = reply
			if reply.VoteSuccess {
				castCount++
				//DPrintf("%d votes out of %d required\n", castCount, requiredVotes)
			}
			cond.Broadcast()
		}(i, term)
	}

	{ // Wait for votes to complete
		rf.mu.Lock()
		for castCount < requiredVotes {
			if quitElection {
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}
			cond.Wait()
		}
		rf.mu.Unlock()
	}

	{ // count votes
		rf.mu.Lock()
		if term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		var voteCount int = 0
		for _, reply := range replies {
			if reply.VoteSuccess {
				voteCount++
			}
		}

		if voteCount >= requiredVotes {
			DPrintf("%d won with %d votes\n", me, voteCount)
			rf.electionState = leader
			rf.mu.Unlock()
			rf.beginHeartbeats()
		} else {
			rf.electionState = follower
			rf.mu.Unlock()
		}
	}
}

type AppendEntriesArgs struct {
	Term int
}
type AppendEntriesReply struct{}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.electionState = follower
		rf.receivedHeartbeat = true
		rf.currentTerm = args.Term
		rf.votedFor = -1
		// log any commands here
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type RequestVoteArgs struct {
	Requester int
	Term      int
}

type RequestVoteReply struct {
	VoteSuccess bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%d received vote request from %d\n", rf.me, args.Requester)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm || rf.votedFor == -1 {
		DPrintf("%d is voting for %d\n", rf.me, args.Requester)
		rf.votedFor = args.Requester
		rf.currentTerm = args.Term
		reply.VoteSuccess = true
	} else {
		reply.VoteSuccess = false
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.log = make([]logEntry, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.votedFor = -1
	rf.electionState = follower
	rf.receivedHeartbeat = false
	rf.restartTimer = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.checkElectionState()

	return rf
}
