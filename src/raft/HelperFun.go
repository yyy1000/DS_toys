package raft

import (
	"math/rand"
	"time"
)

const TimeBound int = 300

func (rf *Raft) GetLastHeartbeat() int64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.LastHeartbeat
}
func (rf *Raft) GetRole() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Role
}
func (rf *Raft) GetElectionTimeout() time.Duration {
	num := rand.Intn(TimeBound)
	return time.Duration(time.Duration(num+TimeBound/2) * time.Millisecond)
}

// mind here, we not lock here for that everytime we reference
// we lock around
func (rf *Raft) UpdateHeartbeat() {
	rf.LastHeartbeat = time.Now().UnixNano() / 1e6
}

func max(lhs int, rhs int) int {
	if lhs > rhs {
		return lhs
	}
	return rhs
}
func min(lhs int, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}
