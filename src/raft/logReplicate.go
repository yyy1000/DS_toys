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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Role!= "leader"{
		return -1,rf.currenctTerm,false
	}
	// Your code here (2B).
	entry := logEntry{
		Term: rf.currenctTerm,
		Command: command,
	}
	index := rf.log.lastIndex() + 1
	rf.log.append(entry)
	//rf.persist()
	rf.sendAppends(false)
	return index, rf.currenctTerm,true
}

func (rf *Raft) sendAppends(isHeartbeat bool)  {
	for i := range rf.peers{
		if i!=rf.me{
			if rf.log.lastIndex() > rf.nextIndex[i] || isHeartbeat{
				rf.sendAppend(i,isHeartbeat)
			}
		}
	}
}
func (rf *Raft) sendAppend(peer int,isHeartbeat bool){

}
func (rf *Raft) BecomeLeader(){
	//rf.commitCount=make(map[int]int)
	rf.Role = "leader"
	for i := range rf.peers{
		rf.nextIndex[i]=rf.log.lastIndex()+1
		rf.matchIndex[i]=0
	}
}

func (rf *Raft) applier()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastApplied = 0

	if rf.lastApplied + 1 <= rf.log.start(){
		//restart from a snapshot
		rf.lastApplied = rf.log.start()
	}
	for !rf.killed(){
		if rf.waitingSnapshot !=nil{
			DPrintf("%d deliver snapshot\n",rf.me)
			am := ApplyMsg{
				SnapshotValid: true,
				Snapshot: rf.waitingSnapshot,
				SnapshotIndex: rf.waitingIndex,
				SnapshotTerm: rf.waitingTerm,
			}
			rf.waitingSnapshot=nil
			rf.mu.Unlock()
			rf.applyCh<-am
			rf.mu.Lock()
		}else if rf.lastApplied + 1 <= rf.commitIndex &&
			rf.lastApplied +1 <= rf.log.lastIndex() &&
			rf.lastApplied +1 > rf.log.start(){
			rf.lastApplied += 1
			am := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command: rf.log.entrys[rf.lastApplied].Command,
			}
			DPrintf("%v Applier deliver %v\n",rf.me,am.CommandIndex)
			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
		}else{
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) advanceCommit()  {
	if rf.Role != "leader"{

	}
	start := rf.commitIndex + 1
	if start < rf.log.start(){
		start = rf.log.start()
	}
	for index := start; index <= rf.log.lastIndex(); index++{
		if rf.log.entrys[index].Term != rf.currenctTerm{
			continue
		}
		n := 1
		for i:=0;i<len(rf.peers);i++{
			if i!= rf.me && rf.matchIndex[i]>=index{
				n+=1
			}
		}
		if n > len(rf.peers)/2{
			rf.commitIndex = index
		}
	}
	rf.signalApplier()
}

func (rf *Raft) signalApplier()  {
	rf.applyCond.Broadcast()
}

func (rf *Raft) processConflictVaild(peer int,args *AppendEntriesArgs,reply *AppendEntriesReply)  {

}