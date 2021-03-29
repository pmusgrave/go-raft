package raft

import "bytes"
import "../labgob"

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	enc := labgob.NewEncoder(buffer)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	readBuffer := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(readBuffer)
	var decodedTerm int
	var decodedVotedFor int
	var decodedLog []LogEntry
	if dec.Decode(&decodedTerm) != nil ||
		dec.Decode(&decodedVotedFor) != nil ||
		dec.Decode(&decodedLog) != nil {
		DPrintf("Error decoding")
	} else {
		rf.currentTerm = decodedTerm
		rf.votedFor = decodedVotedFor
		rf.log = decodedLog
		rf.initFollowerIndices()
	}
}
