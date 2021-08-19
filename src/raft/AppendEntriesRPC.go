package raft

import (
	"time"
)

const HeartbeatInterval time.Duration = 100 * time.Millisecond

// not implement yet
type AppendEntriesArgs struct {
	Term         int
	LeadId       int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// to be implemented in 2B
	if args.Term >= rf.currenctTerm {
		DPrintf("%d 's timer reset", rf.me)
		rf.Role = "follower"
		rf.UpdateHeartbeat()
		rf.currenctTerm = args.Term
	}
	reply.Term = rf.currenctTerm
	DPrintf("%d term:%d received %d:term= %d's heartbeat", rf.me, rf.currenctTerm, args.LeadId, args.Term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat() {
	term, isleader := rf.GetState()
	for isleader {
		// no heartbeat message
		for i := range rf.peers {
			if i != rf.me {
				go rf.HelpHeartbeat(term, i)
			}
		}
		DPrintf("has send heartbeats once")
		time.Sleep(HeartbeatInterval)
		term, isleader = rf.GetState()
	}
}

func (rf *Raft) HelpHeartbeat(term int, i int) {
	args := AppendEntriesArgs{
		Term:   term,
		LeadId: rf.me,
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(i, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currenctTerm {
		rf.Role = "follower"
		rf.currenctTerm = reply.Term
	}

}
