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

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XIndex = -1
		reply.XLength = len(rf.log)
		reply.XTerm = -1
		return
	} else {
		rf.receivedHeartbeat = true
		rf.electionState = follower
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
				reply.XTerm = -1
			}
			for i := len(rf.log) - 1; i > 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = i
				}
			}
		}

		if !reply.Success {
			return
		}

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
		}

		if args.LeaderCommit > rf.commitIndex {
			lastIndex := len(rf.log) - 1
			if args.LeaderCommit < lastIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastIndex
			}
			rf.applyCond.Broadcast()
		}

		reply.XIndex = rf.commitIndex + 1
		reply.XLength = len(rf.log)
		rf.persist()
	}
}

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
