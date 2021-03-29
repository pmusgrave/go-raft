package raft

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

//
// Expects calling code to hold a lock
//
/*func (rf *Raft) apply(start int, stop int) {
	for i := start; i < stop; i++ {
		DPrintf("///////////////////////////////////////////////////////// %d sending apply message: index %d, command %d", rf.me, i, rf.log[i].Command)
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
        }*/

func (rf *Raft) manageApply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			DPrintf("///////////////////////////////////////////////////////// %d sending apply message: index %d, command %d", rf.me, rf.lastApplied, rf.log[rf.lastApplied].Command)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			//rf.lastApplied = i
			//rf.apply(rf.lastApplied, rf.commitIndex+1)
		} else {
			rf.applyCond.Wait()
		}
	}
}
