package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"labgob"
	"labrpc"
)

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

type entry struct {
	EntryTerm  int
	Command    interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  [] int
	Entries      [] entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	FirstConflictIndex int
	Success		bool
	VoteFor		int
}

const (
	leader    = 1
	follower  = 2
	candidate = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	voteFor     int
	log         []entry

	// all server
	commitIndex int

	// leader
	nextIndex  []int
	matchIndex []int
	received   []bool

	status int

	heartBeatRev bool

	lastApplied int

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

	rf.mu.Lock()
	term := rf.currentTerm
	isleader := (rf.status == leader)
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.voteFor) != nil || d.Decode(&rf.log) != nil {
		fmt.Printf("readPersist error !")
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
	fmt.Printf("Peer[%v] requestVote from Peer[%v]\n", args.CandidateId, rf.me)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	if (args.LastLogTerm > rf.log[len(rf.log)-1].EntryTerm) || (args.LastLogTerm == rf.log[len(rf.log)-1].EntryTerm && args.LastLogIndex >= (len(rf.log)-1)) {
		reply.VoteGranted = true
		rf.status = follower
		rf.voteFor = args.CandidateId
		rf.heartBeatRev = true
	} else {
		reply.VoteGranted = false
	}

	rf.persist()

	rf.mu.Unlock()
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

//
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
	// Your code here (2B).

	rf.mu.Lock()

	e := entry{}
	e.EntryTerm = rf.currentTerm
	e.Command = command

	newEntryindex := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.status == leader)

	if isLeader {
		rf.matchIndex[rf.me] = len(rf.log)
		rf.log = append(rf.log, e)
		fmt.Printf("start command at Peer[%v]  log: %v\n", rf.me, rf.log)
	}

	rf.persist()
	rf.mu.Unlock()

	return newEntryindex, term, isLeader
}

func (rf *Raft) AppendEntriesToServer() {

	for rf.killed() == false {

		//fmt.Printf("Peer[%v]: %v\n", rf.me, rf.status)
		rf.mu.Lock()
		if rf.status == leader {
			for len(rf.nextIndex) < len(rf.peers) {
				rf.nextIndex = append(rf.nextIndex, len(rf.log))
			}

			for len(rf.matchIndex) < len(rf.peers) {
				rf.matchIndex = append(rf.matchIndex, 0)
			}

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				rf.received[i] = false
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex

				for j := 0; j < rf.nextIndex[i]; j++ {
					args.PrevLogTerm = append(args.PrevLogTerm, rf.log[j].EntryTerm)
				}

				args.Entries = rf.log[rf.nextIndex[i] : len(rf.log)]

				fmt.Printf("leaderPeer[%v] term: %v sendto Peer[%v] nextIndex: %v matchIndex: %v Entries: %v logLength: %v commitIndex: %v\n", rf.me, rf.currentTerm, i, rf.nextIndex, rf.matchIndex, args.Entries, len(rf.log), rf.commitIndex)

				go func(i int) {
					reply := AppendEntriesReply{}
					succ := rf.SendAppendEntriesToLog(i, &args, &reply)
					if succ {
						rf.mu.Lock()
						if !reply.Success {
							if reply.Term > rf.currentTerm  {
								rf.currentTerm = reply.Term
							}
							rf.status = follower
							rf.voteFor = reply.VoteFor
						}
						rf.mu.Unlock()
						rf.nextIndex[i] = reply.FirstConflictIndex
						rf.matchIndex[i] = reply.FirstConflictIndex - 1
						rf.received[i] = true
					}
				}(i)
			}
		}
		rf.mu.Unlock()
		time.Sleep(70 * time.Millisecond)
	}
}

func (rf *Raft) WaitForCommit() {

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status == leader {
			tmp := make([]int, 0)
			liveNodeCount := 0
			for i := 0; i < len(rf.matchIndex); i++ {
				if rf.received[i] == true {
					liveNodeCount += 1
				}
				tmp = append(tmp, rf.matchIndex[i])
				if i != rf.me {
					rf.received[i] = false
				}
			}

			sort.Ints(tmp)

			if liveNodeCount > len(rf.matchIndex)/2 {
				rf.commitIndex = tmp[len(rf.matchIndex)/2]
			}
		}

		rf.apply()
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

}

func (rf *Raft) SendAppendEntriesToLog(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		t := rand.Intn(700-400) + 400
		time.Sleep(time.Duration(t) * time.Millisecond)
		rf.mu.Lock()
		if rf.status != leader && rf.heartBeatRev == false {
			rf.currentTerm += 1
			rf.status = candidate
			rf.voteFor = rf.me
			rf.LeaderElection()
		}
		rf.heartBeatRev = false
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) LeaderElection() {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log)-1
	args.LastLogTerm = rf.log[len(rf.log)-1].EntryTerm

	voteChan := make(chan bool)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			succ := rf.sendRequestVote(i, &args, &reply)
			voteChan <- (succ && reply.VoteGranted)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = follower
				rf.voteFor = i
			}
			rf.mu.Unlock()
		}(i)
	}

	PeersLength := len(rf.peers)
	voteForNum := 1
	for i := 0; i < PeersLength-1; i++ {
		succ := <-voteChan
		if succ {
			voteForNum += 1
		}
		if voteForNum > PeersLength/2 {
			break
		}
	}

	fmt.Printf("leaderElection Peer[%v]: voteForNum: %v  len(peer): %v\n", rf.me, voteForNum, len(rf.peers))

	if voteForNum > PeersLength/2 && rf.status == candidate {
		rf.status = leader
		rf.nextIndex = make([]int, len(rf.peers))
		// rf.matchIndex = make([]int, len(rf.peers))
		rf.received = make([]bool, len(rf.peers))

		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.received[i] = false
		}

		for len(rf.matchIndex) < len(rf.peers) {
			rf.matchIndex = append(rf.matchIndex, 0)
		}

		rf.received[rf.me] = true
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartBeatRev = false
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.status = follower
	rf.applyCh = applyCh

	e := entry{}
	e.EntryTerm = 0
	rf.log = append(rf.log, e)

	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	go rf.ticker()
	go rf.AppendEntriesToServer()
	go rf.WaitForCommit()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	return rf
}


func (rf *Raft) SendAppendEntriesHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	fmt.Printf("leaderPeer[%v] sendto followerPeer[%v]  %v\n",args.LeaderId, rf.me, rf.log)

	firstConflict := 1

	if len(rf.log) < len(args.PrevLogTerm) {
		firstConflict = len(rf.log)
	}else {
		for i := len(args.PrevLogTerm) - 1; i >= -1; i-- {
			if i == -1 || rf.log[i].EntryTerm == args.PrevLogTerm[i] {
				firstConflict += i
				break
			}
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteFor = rf.voteFor
	reply.FirstConflictIndex = firstConflict

	if (args.Term == rf.currentTerm && rf.voteFor != args.LeaderId) || args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	reply.Success = true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.status = follower
		rf.voteFor = args.LeaderId
		reply.VoteFor = rf.voteFor
	}

	rf.heartBeatRev = true

	if firstConflict < len(args.PrevLogTerm) {
		rf.mu.Unlock()
		return
	}

	reply.FirstConflictIndex += len(args.Entries)
	for i := 0; i < len(args.Entries); i++ {
		if firstConflict + i >= len(rf.log){
			rf.log = append(rf.log, args.Entries[i])
		} else {
			rf.log[firstConflict + i] = args.Entries[i]
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit >= len(rf.log) {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	rf.apply()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) apply() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applymsg := ApplyMsg{}
			applymsg.Command = rf.log[i].Command
			rf.lastApplied = i
			applymsg.CommandIndex = i
			applymsg.CommandValid = true
			rf.applyCh <- applymsg
		}
	}
}



