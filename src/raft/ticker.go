package raft

import "time"

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()
		smallperiod := 50
		time.Sleep(time.Duration(smallperiod)*time.Millisecond)
	}

}

func (rf *Raft) tick()  {
	//
	//
	before := rf.GetLastHeartbeat()
	//DPrintf("%d begin to sleep", rf.me)
	time.Sleep(rf.GetElectionTimeout())
	after := rf.GetLastHeartbeat()
	//DPrintf("%d end sleep, role = %s", rf.me, rf.GetRole())
	if before == after && rf.GetRole() != "leader" {
		rf.mu.Lock()
		rf.Role = "candidate"
		rf.votedFor = rf.me
		rf.tickets = 1
		rf.currenctTerm++
		//DPrintf("%d become candidate, term = %d", rf.me, rf.currenctTerm)
		//DPrintf("%d send Vote to itself", rf.me)
		go rf.Elect(rf.currenctTerm)
		rf.mu.Unlock()
	}
}
