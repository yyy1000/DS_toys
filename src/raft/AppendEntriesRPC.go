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
	ConflictValid bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// to be implemented in 2B
	if args.Term < rf.currenctTerm {
		reply.Success,reply.Term = false,rf.currenctTerm
		return
	} else if rf.log.lastIndex() < args.PrevLogIndex || rf.log.entrys[args.PrevLogIndex].Term != args.PreLogTerm {
		reply.Success = false
	}else{
		reply.Success = true
		// check appendRPC rule 3
		//DPrintf("%d 's timer reset", rf.me)
		rf.Role = "follower"
		rf.UpdateHeartbeat()
		rf.currenctTerm = args.Term
		// mind here, still problem (not already in the log)
		for i := range args.Entries{
			//DPrintf("now ready to append %d to %d,",args.PrevLogIndex+1+i,rf.me)
			if (rf.log.lastIndex() <= args.PrevLogIndex+i) || (rf.log.entrys[args.PrevLogIndex+1+i].Term != args.Entries[i].Term){
				DPrintf("now ready to append %d to %d,",args.PrevLogIndex+1+i,rf.me)
				//rf.log = append(rf.log,onelog)
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex()+1)
		}
	}
	reply.Term = rf.currenctTerm
	DPrintf("%d term:%d received %d:term= %d's message, loglen=%d, now len=", rf.me, rf.currenctTerm, args.LeadId, args.Term,len(args.Entries))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat() {
	_,isleader := rf.GetState()
	//go rf.ReceiveChan()
	for isleader {
		// no heartbeat message
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendAppends(true)
			}
		}
		//DPrintf("has send heartbeats once")
		time.Sleep(HeartbeatInterval)
		_, isleader = rf.GetState()
	}
}

func (rf *Raft) HelpHeartbeat(term int, i int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         term,
		LeadId:       rf.me,
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex = rf.nextIndex[i] - 1
	args.PreLogTerm = rf.log.entrys[args.PrevLogIndex].Term
	// mind here, not know how the entries
	if rf.commitIndex < rf.log.lastIndex() {
		//args.Entries = rf.log[rf.commitIndex+1:]
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(i, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()


}

func (rf *Raft) processAppendReply(peer int, args *AppendEntriesArgs,reply *AppendEntriesReply){
	if reply.Term > rf.currenctTerm {
		rf.NewTerm(reply.Term)
	}else if reply.Term==rf.currenctTerm{
		rf.processAppendReplyTerm(peer,args,reply)
	}
}
func (rf *Raft) processAppendReplyTerm(peer int,args *AppendEntriesArgs,reply *AppendEntriesReply)  {
	if reply.Success{
		newmatch := args.PrevLogIndex + len(args.Entries)
		newnext := newmatch + 1
		if newnext > rf.nextIndex[peer]{
			rf.nextIndex[peer] = newnext
		}
		if newmatch > rf.matchIndex[peer]{
			rf.matchIndex[peer] = newmatch
		}
	}else if reply.ConflictValid{
		rf.processConflictVaild(peer,args,reply)
	}else if rf.nextIndex[peer] > 1{
		rf.nextIndex[peer] -= 1
		/*
		if rf.nextIndex[peer] < rf.log.start() + 1{
		rf.sendSnapShot(peer)
		}
		*/
	}
	rf.advanceCommit()
}

/*
func (rf *Raft) ReceiveChan()  {
	for msg := range rf.applyCh{
		DPrintf("receive ApplyMsg %v index = %d",msg.Command,msg.CommandIndex)
		if msg.CommandValid {
			rf.commitCount[msg.CommandIndex] ++
		}
		if rf.commitCount[msg.CommandIndex] > len(rf.peers) / 2{
			rf.commitIndex = msg.CommandIndex
			DPrintf("has updated commitIndex = %d",rf.commitIndex)
		}
	}
}
*/

