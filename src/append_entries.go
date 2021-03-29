package raft

type AppendEntriesArgs struct {
	Entries      []LogEntry
	LeaderId     int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Term         int
}
type AppendEntriesReply struct {
	Success bool
	Term    int
	XIndex  int
	XLength int
	XTerm   int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("          AE received: %d->%d  %+v\n", args.LeaderId, rf.me, args)
	//DPrintf(" --- %d's existing log: %+v\n", rf.me, rf.log)
	//DPrintf(" --- %d's current term:%d\n", rf.me, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XIndex = -1
		reply.XLength = len(rf.log)
		reply.XTerm = -1
		return
	} else {
		//	} else if args.PrevLogTerm == rf.log[len(rf.log)-1].Term || args.PrevLogTerm >= rf.currentTerm || args.Term > rf.currentTerm {
		rf.receivedHeartbeat = true
		rf.electionState = follower
		rf.votedFor = -1
		//rf.currentTerm = args.Term
		rf.currentTerm = rf.setTerm(args.Term)
		reply.Term = rf.currentTerm
		rf.persist()
		reply.Success = rf.prevMatch(args.PrevLogIndex, args.PrevLogTerm)

		{
			reply.XIndex = -1
			reply.XLength = len(rf.log)
			if args.PrevLogIndex < len(rf.log) {
				reply.XTerm = rf.log[args.PrevLogIndex].Term
			} else {
				//reply.XTerm = rf.log[len(rf.log)-1].Term
				reply.XTerm = -1
			}
			for i := len(rf.log) - 1; i > 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = i
				}
			}
		}

		/*reply.NextIndex = 1
		for i := len(rf.log) - 1; i >= 1; i-- {
			if rf.log[i].Term == args.PrevLogTerm && i == args.PrevLogIndex {
				reply.NextIndex = i
			}
		}*/

		if !reply.Success {
			return
		}

		DPrintf("%d's log before AE %+v", rf.me, rf.log)
		i := args.PrevLogIndex + 1
		for _, entry := range args.Entries {
			if i > len(rf.log)-1 {
				rf.log = append(rf.log, entry)
				rf.persist()
			} else if rf.log[i] != entry {
				rf.log = rf.log[:i+1]
				rf.log[i] = entry
				rf.persist()
			}
			i++
			//DPrintf("%d's log during %+v", rf.me, rf.log)
		}
		DPrintf("%d's log after AE %+v", rf.me, rf.log)

		//prevCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			lastIndex := len(rf.log) - 1
			if args.LeaderCommit < lastIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastIndex
			}
			rf.applyCond.Broadcast()
		}

		//reply.NextIndex = len(rf.log)
		reply.XIndex = rf.commitIndex + 1
		reply.XLength = len(rf.log)
		rf.persist()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
