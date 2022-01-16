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
	EntryIndex int
	Command    interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      entry
	LeaderCommit int
	Full         bool
	HeartBeat    bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

	olderLeaderId int

	status int

	heartBeatRev bool

	lastApplied int

	applyCh chan ApplyMsg

	tmp chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.status == leader)
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

	rf.mu.Lock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	rf.mu.Unlock()
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

	rf.mu.Lock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.voteFor) != nil || d.Decode(&rf.log) != nil {
		fmt.Printf("readPersist error !")
	}
	rf.mu.Unlock()
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
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	}

	if (args.LastLogTerm > rf.log[len(rf.log)-1].EntryTerm) ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].EntryTerm &&
			args.LastLogIndex >= rf.log[len(rf.log)-1].EntryIndex) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.heartBeatRev = true
	} else {
		reply.VoteGranted = false
		rf.voteFor = -1
	}

	rf.mu.Unlock()
	rf.changeTerm(args.Term)
	rf.persist()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()

	e := entry{}
	e.EntryTerm = rf.currentTerm
	e.Command = command

	index = len(rf.log)
	e.EntryIndex = index

	// fmt.Printf("Start ***************************************************!\n")
	term = rf.currentTerm
	isLeader = (rf.status == leader)

	if isLeader {
		rf.log = append(rf.log, e)
		rf.matchIndex[rf.me] = len(rf.log) - 1
	}

	rf.mu.Unlock()
	rf.persist()
	return index, term, isLeader
}

func (rf *Raft) AppendEntriesToServer() {

	for rf.killed() == false {

		rf.mu.Lock()
		if rf.status == leader {
			len1 := len(rf.peers)
			for i := 0; i < len1; i++ {
				if i == rf.me {
					continue
				}

				rf.received[i] = false

				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.HeartBeat = false

				args.PrevLogIndex = rf.log[rf.nextIndex[i]-1].EntryIndex

				args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].EntryTerm

				args.Full = false

				if len(rf.log) > rf.nextIndex[i] {
					args.Entries = rf.log[rf.nextIndex[i]]
				}

				if len(rf.log) == rf.nextIndex[i] {
					args.Full = true
				}

				go func(i int) {
					reply := AppendEntriesReply{}
					succ := rf.SendAppendEntriesToLog(i, &args, &reply)
					rf.changeTerm(reply.Term)
					if succ && reply.Success {
						rf.mu.Lock()
						if len(rf.log) > rf.nextIndex[i] {
							rf.matchIndex[i] = rf.nextIndex[i]
							rf.nextIndex[i] += 1
						}
						rf.received[i] = true
						rf.mu.Unlock()
					} else if succ && !reply.Success {
						rf.mu.Lock()
						if rf.nextIndex[i] < len(rf.log) {
							rf.nextIndex[i] -= 1
						}
						if rf.nextIndex[i] < 1 {
							rf.nextIndex[i] = 1
						}

						rf.mu.Unlock()
					}
				}(i)
			}
		}
		rf.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) WaitForCommit() {

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status == leader {
			tmp := make([]int, 0)
			l := 0
			for i := 0; i < len(rf.matchIndex); i++ {
				if rf.received[i] == true {
					l += 1
				}
				tmp = append(tmp, rf.matchIndex[i])
				if i != rf.me {
					rf.received[i] = false
				}
			}

			sort.Ints(tmp)

			if l > len(rf.matchIndex)/2 {
				rf.commitIndex = tmp[len(rf.matchIndex)/2]
			}
		}

		rf.mu.Unlock()
		go rf.ListenCommitToApply()
		time.Sleep(10 * time.Millisecond)
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

		t := rand.Intn(2000-1200) + 1200
		time.Sleep(time.Duration(t) * time.Millisecond)
		rf.mu.Lock()
		if rf.status != leader && rf.heartBeatRev == false {
			rf.currentTerm += 1
			rf.status = candidate
			rf.voteFor = rf.me
			go rf.LeaderElection(rf.currentTerm)
		}
		rf.heartBeatRev = false
		rf.mu.Unlock()
		rf.persist()
	}
}

func (rf *Raft) LeaderElection(term int) {
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.Term = term
	args.CandidateId = rf.me

	args.LastLogIndex = rf.log[len(rf.log)-1].EntryIndex
	args.LastLogTerm = rf.log[len(rf.log)-1].EntryTerm
	rf.mu.Unlock()

	voteChan := make(chan bool)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			succ := rf.sendRequestVote(i, &args, &reply)
			rf.changeTerm(reply.Term)
			voteChan <- (succ && reply.VoteGranted)
		}(i)
	}

	len1 := len(rf.peers)
	voteForNum := 1
	for i := 0; i < len1-1; i++ {
		succ := <-voteChan
		if succ {
			voteForNum += 1
		}
		if voteForNum > len1/2 {
			break
		}
	}

	rf.mu.Lock()
	if voteForNum > len1/2 && rf.status == candidate && rf.currentTerm <= term {
		rf.status = leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.received = make([]bool, len(rf.peers))

		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.received[i] = false
		}
		rf.received[rf.me] = true
	}
	rf.mu.Unlock()
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

	rf.tmp = make(chan ApplyMsg, 100)

	rf.olderLeaderId = -1

	e := entry{}
	e.EntryIndex = 0
	e.EntryTerm = 0
	rf.log = append(rf.log, e)

	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	go rf.ticker()
	go rf.HeartBeat()
	go rf.AppendEntriesToServer()
	go rf.WaitForCommit()
	go rf.apply()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	return rf
}

func (rf *Raft) apply() {
	for rf.killed() == false {
		go func() {
			rf.applyCh <- <-rf.tmp
		}()
		time.Sleep(180 * time.Millisecond)
	}
}

func (rf *Raft) ListenCommitToApply() {
	rf.mu.Lock()

	if rf.commitIndex > rf.lastApplied {
		applymsg := ApplyMsg{}
		rf.lastApplied += 1
		applymsg.Command = rf.log[rf.lastApplied].Command
		applymsg.CommandIndex = rf.lastApplied
		applymsg.CommandValid = true
		go func(applymsg ApplyMsg) {
			rf.tmp <- applymsg
		}(applymsg)
	}

	rf.mu.Unlock()
}

func (rf *Raft) HeartBeat() {

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status == leader {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				args := AppendEntriesArgs{}
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				args.HeartBeat = true
				go func(i int) {
					reply := AppendEntriesReply{}
					succ := rf.SendAppendEntriesHandler(i, &args, &reply)
					if succ {
						rf.changeTerm(reply.Term)
					}
					rf.mu.Lock()
					rf.mu.Unlock()
				}(i)
			}
		}

		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

}

func (rf *Raft) SendAppendEntriesHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()

	if !args.HeartBeat && rf.olderLeaderId != args.LeaderId && args.PrevLogIndex < len(rf.log)-1 {
		pi := 0
		for i := len(rf.log) - 1; i > 0; i-- {
			if rf.log[i].EntryTerm == args.PrevLogTerm &&
				rf.log[i].EntryIndex == args.PrevLogIndex {
				pi = i
				break
			}
		}
		rf.log = rf.log[0 : pi+1]
		rf.olderLeaderId = args.LeaderId
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	if (!args.HeartBeat) &&
		(rf.log[len(rf.log)-1].EntryTerm == args.Entries.EntryTerm &&
			rf.log[len(rf.log)-1].EntryIndex == args.Entries.EntryIndex) {
		reply.Success = true
		rf.mu.Unlock()
		return
	}

	if (!args.HeartBeat) &&
		(rf.log[len(rf.log)-1].EntryTerm != args.PrevLogTerm ||
			rf.log[len(rf.log)-1].EntryIndex != args.PrevLogIndex) {
		rf.mu.Unlock()
		return
	}

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if !args.HeartBeat {
		reply.Success = true

		if !args.Full {
			rf.log = append(rf.log, args.Entries)
		}

		if args.LeaderCommit > rf.commitIndex {
			if args.Entries.EntryIndex == 0 {
				rf.commitIndex = args.LeaderCommit
			} else if args.LeaderCommit > args.Entries.EntryIndex {
				rf.commitIndex = args.Entries.EntryIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}

	}

	rf.heartBeatRev = true

	rf.mu.Unlock()

	go rf.ListenCommitToApply()

	rf.persist()

	rf.changeTerm(args.Term)
}

func (rf *Raft) changeTerm(term int) {
	rf.mu.Lock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.status = follower
		rf.voteFor = -1
	}
	rf.mu.Unlock()

	rf.persist()
}
