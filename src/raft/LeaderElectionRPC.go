package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currenctTerm{
		rf.NewTerm(args.Term)
	}
	//rf.restart = true
	var logTerm int
	logLastIndex := len(rf.log) - 1
	logTerm = rf.log[logLastIndex].Term
	uptodate := (args.LastLogTerm > logTerm) || (args.LastLogTerm == logTerm && args.LastLogIndex >= logLastIndex)
	if args.Term < rf.currenctTerm{
		reply.VoteGranted = false
	}else if ((rf.votedFor == -1) || (rf.votedFor == args.CandidateId)) && uptodate{
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		//DPrintf("%d term:%d vote %d's request, candidate.term = %d", rf.me, rf.currenctTerm, args.CandidateId, args.Term)
		rf.UpdateHeartbeat()
		//rf.Role = "follower"
	} else {
		reply.VoteGranted = false
		//DPrintf("%d deny %d's request", rf.me, args.CandidateId)
	}
	reply.Term = rf.currenctTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) NewTerm(term int)  {
	rf.currenctTerm = term
	rf.Role = "follower"
	rf.votedFor = -1
}
func (rf *Raft) Elect(currenctTerm int) {
	for i := range rf.peers {
		if i != rf.me {
			go rf.HelpRequestVote(currenctTerm, i)
		}
	}
}

func (rf *Raft) HelpRequestVote(term int, i int) {
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log)-1,
		LastLogTerm : rf.log[len(rf.log) - 1].Term,
	}
	//DPrintf("%d send RequestVote to %d", rf.me, i)
	replys := RequestVoteReply{}
	ok := rf.sendRequestVote(i, &args, &replys)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if replys.Term > term {
			rf.Role = "follower"
			rf.currenctTerm = max(replys.Term, rf.currenctTerm)
		}
		if replys.VoteGranted && rf.currenctTerm == term {
			rf.tickets++
		}
		if rf.tickets > len(rf.peers)/2 && rf.Role == "candidate" {
			//DPrintf("%d become leader", rf.me)
			rf.BecomeLeader()
			go rf.sendHeartBeat()
		}
	}
}
