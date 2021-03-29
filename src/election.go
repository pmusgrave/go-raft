package raft

import "math/rand"
import "time"

func (rf *Raft) checkElectionState() {
	if rf.receivedHeartbeat {
		rf.receivedHeartbeat = false
		rf.electionState = follower
		rf.resetElectionTimer()
		rf.persist()
	} else if rf.electionTimedOut() {
		rf.resetElectionTimer()
		rf.startElection()
	}
}

func (rf *Raft) electionTimedOut() bool {
	t := time.Now()
	elapsed := t.Sub(rf.electionStartTime)
	return elapsed > rf.electionTimeout
}

func (rf *Raft) isNewLeader(term int, replies []RequestVoteReply) (bool, int) {
	for _, reply := range replies {
		if reply.Term > term {
			return true, reply.Term
		}
	}
	return false, -1
}

func (rf *Raft) periodicallyCheckElectionState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		rf.timer.Wait()
		if rf.electionState == leader {
			rf.sendHeartbeats()
		} else {
			rf.checkElectionState()
		}
	}
}

func (rf *Raft) receivedMajorityVote(term int, replies []RequestVoteReply) bool {
	count := 0
	for _, reply := range replies {
		if reply.VoteGranted && reply.Term == term {
			count++
		}
		if count >= rf.requiredReplies {
			return true
		}
	}
	return false
}

func (rf *Raft) resetElectionTimer() {
	rf.electionStartTime = time.Now()
	rf.electionTimeout = time.Duration(rand.Intn(500))*time.Millisecond + 800*time.Millisecond
}

func (rf *Raft) startElection() {
	me := rf.me
	rf.currentTerm++
	rf.electionState = candidate
	rf.votedFor = me
	rf.resetElectionTimer()
	rf.receivedHeartbeat = false
	rf.persist()
	thisElectionStartTime := rf.electionStartTime
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	replies := make([]RequestVoteReply, len(rf.peers))
	replies[me] = RequestVoteReply{
		VoteGranted: true,
		Term:        rf.currentTerm,
	}
	term := rf.currentTerm
	for i, _ := range rf.peers {
		if i == me {
			continue
		}
		go func(id int, replies []RequestVoteReply) {
			args := RequestVoteArgs{
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				Term:         term,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(id, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			replies[id] = reply
		}(i, replies)
	}

	for !rf.receivedMajorityVote(term, replies) && thisElectionStartTime == rf.electionStartTime {
		rf.timer.Wait()
		if rf.electionTimedOut() {
			break
		}
		newLeader, newTerm := rf.isNewLeader(term, replies)
		if newLeader {
			rf.setTerm(newTerm)
		}
	}

	if thisElectionStartTime != rf.electionStartTime {
		rf.persist()
		return
	}

	if term == rf.currentTerm && rf.receivedMajorityVote(term, replies) {
		rf.electionState = leader
		rf.commitIndex = -1
		rf.persist()
		rf.initFollowerIndices()
		rf.sendHeartbeats()
	}
}
