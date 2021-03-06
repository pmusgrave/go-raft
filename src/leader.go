package raft

func (rf *Raft) attemptAppendEntries(id int, index int, args *AppendEntriesArgs) {
	if rf.killed() || rf.electionState != leader {
		return
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(id, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm || rf.electionState != leader { //
		rf.setTerm(reply.Term)
		return
	}

	if ok {
		DPrintf("req %d->%d: %+v", rf.me, id, args)
		DPrintf("res %d<-%d: %+v", rf.me, id, reply)
	}

	if reply.Success {
		if reply.XLength > rf.nextIndex[id] {
			rf.nextIndex[id] = reply.XLength
		}
		if reply.XLength-1 > rf.matchIndex[id] {
			rf.matchIndex[id] = reply.XLength - 1
		}
		if reply.XLength-1 > rf.matchIndex[rf.me] {
			rf.matchIndex[rf.me] = reply.XLength - 1
		}
	} else {
		isTermInLog, lastValidIndex := rf.termInLog(reply.XTerm)
		if reply.XTerm == -1 && reply.XIndex != -1 {
			if reply.XIndex < rf.nextIndex[id] && reply.Term >= args.Term {
				rf.nextIndex[id] = reply.XIndex
			}
		} else if isTermInLog {
			if reply.XIndex < rf.nextIndex[id] && reply.Term >= args.Term {
				rf.nextIndex[id] = lastValidIndex
			}
		} else {
			rf.nextIndex[id] = 1
		}

		if rf.nextIndex[id] > len(rf.log) {
			rf.nextIndex[id] = len(rf.log) - 1
		}
		if rf.nextIndex[id] < 1 {
			rf.nextIndex[id] = 1
		}

		entries := make([]LogEntry, len(rf.log)-rf.nextIndex[id])
		copy(entries, rf.log[rf.nextIndex[id]:])
		retryArgs := AppendEntriesArgs{
			Entries:      entries,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[id] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[id]-1].Term,
			Term:         rf.currentTerm,
		}
		go rf.attemptAppendEntries(id, len(rf.log)-1, &retryArgs)
	}
	rf.setCommitIndex()
}

func (rf *Raft) getNumMatches(index int) int {
	count := 1
	for i, match := range rf.matchIndex {
		if match >= index && i != rf.me {
			count++
		}
	}
	return count
}

//
// Initialize all followers' nextIndex and matchIndex arrays
//
func (rf *Raft) initFollowerIndices() {
	for i, _ := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) sendHeartbeats() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Entries:      make([]LogEntry, 0),
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: len(rf.log) - 1,
			PrevLogTerm:  rf.log[len(rf.log)-1].Term,
			Term:         rf.currentTerm,
		}
		reply := AppendEntriesReply{}

		go func(id int) {
			rf.sendAppendEntries(id, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.setTerm(reply.Term)
		}(i)
	}
}

func (rf *Raft) setCommitIndex() {
	if rf.electionState != leader {
		return
	}
	for i, _ := range rf.log {
		if i > rf.commitIndex && rf.getNumMatches(i) >= rf.requiredReplies && rf.log[i].Term == rf.currentTerm {
			rf.commitIndex = i
			rf.matchIndex[rf.me] = rf.commitIndex
		}
	}
	rf.applyCond.Broadcast()
}

func (rf *Raft) startLogConsensus(index int, term int) {
	for i, followerIndex := range rf.nextIndex {
		if i == rf.me {
			continue
		}
		if len(rf.log)-1 >= followerIndex {
			entries := make([]LogEntry, len(rf.log)-followerIndex)
			copy(entries, rf.log[followerIndex:])
			args := AppendEntriesArgs{
				Entries:      entries,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: followerIndex - 1,
				PrevLogTerm:  rf.log[followerIndex-1].Term,
				Term:         rf.currentTerm,
			}

			go rf.attemptAppendEntries(i, index, &args)
		}
	}
}

func (rf *Raft) termInLog(term int) (bool, int) {
	inLog := false
	index := 1
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			inLog = true
			index = i
		}
	}
	return inLog, index
}
