package raft

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

func (rf *Raft) logUpToDate(lastLogIndex int, lastLogTerm int) bool {
	myLastEntry := rf.log[len(rf.log)-1]
	return lastLogTerm > myLastEntry.Term || (lastLogTerm == myLastEntry.Term && lastLogIndex >= len(rf.log)-1)
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = rf.setTerm(args.Term)
	}

	rf.persist()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = rf.logUpToDate(args.LastLogIndex, args.LastLogTerm)
	}

	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		rf.persist()
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
