package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// empty log struct to be implemented
type logEntry struct {
	Command interface{}
	Term    int
}

// Start
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
	/*
	index := 0
	term := -1
	isLeader := true
	*/

	// Your code here (2B).
	term,isLeader := rf.GetState()
	index := rf.nextIndex[rf.me]
	if ! isLeader{
		return index,term,isLeader
	}
	go rf.Agreement(index,term,command)
	return index, term, isLeader
}

func (rf *Raft) Agreement(index int,term int,command interface{}) {
	rf.log= append(rf.log, logEntry{Command: command,Term: term})
}

func (rf *Raft) BecomeLeader(){
	//rf.commitCount=make(map[int]int)
	rf.Role = "leader"
	for i := range rf.peers{
		rf.nextIndex[i]=len(rf.log)
		rf.matchIndex[i]=0
	}
}

func (rf *Raft) applier()  {

}