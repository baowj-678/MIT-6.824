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
	"log"
	"math/rand"
	"os"
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// ApplyMsg
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

//
// AppendEntries
// invoked by leader to replicate log entries,
// also used as heartbeat.
type AppendEntries struct {
	Term         int   // leader's term.
	LeaderId     int   // so followers can redirect clients.
	PrevLogIndex int64 // index of log entry immediately preceding new ones.
	PrevLogTerm  int   // term of PrevLogIndex entry.
	Entries      []int // log entries to store(empty for heartbeat, may send more than one for efficiency).
	LeaderCommit int   // leader's commitIndex.
}

//
// AppendEntriesReply
// reply after leader sent AppendEntries.
type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself, -2 represent dead
	Success int // true if follower contained entry matching.
	// PrevLogIndex and PrevLogTerm.
}

//
// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	timeout time.Duration // rand timeout: (from 800 to 1000ms)
	//electionTimeInterval  time.Duration
	heartBeatInterval time.Duration // default: (150ms)
	electionTimer     int64
	verbose           int
	isPeersLive       []int64

	currentTermMutex sync.Mutex
	currentTerm      int // current term.

	lastElectionTimeMutex sync.Mutex
	lastElectionTime      int64

	votedForMutex sync.Mutex
	votedFor      int // which peer I vote for, -1 for no one.

	currentStateMutex sync.Mutex
	currentState      State

	electionTimerMutex sync.Mutex
	isPeersLiveMutex   sync.Mutex
}

//
// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.currentTermMutex.Lock()
	rf.currentStateMutex.Lock()

	term = rf.currentTerm
	isleader = rf.currentState == Leader
	rf.currentStateMutex.Unlock()
	rf.currentTermMutex.Unlock()

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
}

//
// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//
// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term.
	CandidateId  int // candidate requesting vote.
	LastLogIndex int // index of candidate's last log entry.
	LastLogTerm  int // term of candidate's last log entry.
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for condidate to update itself.
	VoteGranted bool // true means condidate received vote.
}

//
// RequestVote
// example RequestVote RPC handler.
// candidate response
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// currentTerm < term
	if rf.currentTermMutex.Lock(); rf.currentTerm < args.Term {
		rf.Log(2, "RequestVote("+strconv.Itoa(rf.me)+"): currentTerm("+strconv.Itoa(rf.currentTerm)+") < term("+strconv.Itoa(args.Term)+") from peer("+strconv.Itoa(args.CandidateId)+")")
		rf.currentStateMutex.Lock()
		rf.votedForMutex.Lock()
		// set term
		rf.currentTerm = args.Term
		// change state
		rf.currentState = Follower
		// vote for
		rf.votedFor = args.CandidateId
		rf.votedForMutex.Unlock()
		rf.currentStateMutex.Unlock()
		rf.currentTermMutex.Unlock()
		// reset heartbeat timer
		rf.electionTimerMutex.Lock()
		rf.electionTimer = time.Now().UnixMilli()
		rf.electionTimerMutex.Unlock()
		reply.Term = args.Term
		// vote for this peer
		reply.VoteGranted = true
	} else if rf.currentTerm == args.Term {
		rf.Log(2, "RequestVote("+strconv.Itoa(rf.me)+"): currentTerm("+strconv.Itoa(rf.currentTerm)+") = term("+strconv.Itoa(args.Term)+") from peer("+strconv.Itoa(args.CandidateId)+")")
		rf.currentStateMutex.Lock()
		if rf.currentState == Follower {
			// vote for this peer
			rf.votedForMutex.Lock()
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				reply.VoteGranted = true
				// vote for
				rf.votedFor = args.CandidateId
				rf.votedForMutex.Unlock()
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
				// reset election timer
				rf.electionTimerMutex.Lock()
				rf.electionTimer = time.Now().UnixMilli()
				rf.electionTimerMutex.Unlock()
			} else {
				reply.VoteGranted = false
				rf.votedForMutex.Unlock()
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
			}
		} else {
			rf.currentStateMutex.Unlock()
			rf.currentTermMutex.Unlock()
		}
		reply.Term = args.Term
	} else {
		// currentTerm > term
		rf.Log(1, "RequestVote("+strconv.Itoa(rf.me)+"): currentTerm("+
			strconv.Itoa(rf.currentTerm)+") > term("+strconv.Itoa(args.Term)+") from peer("+strconv.Itoa(args.CandidateId)+")")
		// currentTerm > term
		reply.Term = rf.currentTerm
		rf.currentTermMutex.Unlock()
		reply.VoteGranted = false
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// Kill
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
	rf.Log(1, "Killed("+strconv.Itoa(rf.me)+")")
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	// log
	rf.Log(1, "ticker("+strconv.Itoa(rf.me)+"): start")
	for rf.killed() == false {
		// log
		rf.Log(1, "ticker("+strconv.Itoa(rf.me)+"): not killed loop")
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.electionTimerMutex.Lock()
		if time.Now().UnixMilli()-rf.electionTimer > int64(rf.timeout) {
			// timeout
			rf.electionTimerMutex.Unlock()
			// log
			rf.currentTermMutex.Lock()
			rf.currentStateMutex.Lock()
			if rf.currentState == Follower || rf.currentState == Candidate {
				rf.Log(1, "ticker("+strconv.Itoa(rf.me)+"): timeout, state: "+string(rf.currentState)+"; term: "+strconv.Itoa(rf.currentTerm))
				// change state
				rf.currentState = Candidate
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
				// start election
				go rf.election()
			} else {
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
			}
		} else {
			rf.electionTimerMutex.Unlock()
		}
		// sleep a rand timeout
		time.Sleep(rf.timeout / 2 * time.Millisecond)
	}
	time.Sleep(time.Second)
}

// The heartBeat go routine will send heartbeat periodically
func (rf *Raft) sendAppendEntries(term int) {
	rf.Log(1, "sendAppendEntries("+strconv.Itoa(rf.me)+"): Start")
	// start send heartbeat
	request := AppendEntries{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: 0,       // TODO
		PrevLogTerm:  0,       // TODO
		Entries:      []int{}, // TODO
		LeaderCommit: 0,       // TODO
	}
	reply := AppendEntriesReply{}
	for i, peer := range rf.peers {
		if i == rf.me {
			// do not need to send Heartbeat to self.
			continue
		}
		ok := peer.Call("Raft.AppendEntries", &request, &reply)
		if ok {
			// currentTerm < term
			if rf.currentTermMutex.Lock(); rf.currentTerm < reply.Term {
				rf.currentStateMutex.Lock()
				rf.votedForMutex.Lock()
				rf.currentTerm = reply.Term
				rf.currentState = Follower
				rf.votedFor = i
				rf.votedForMutex.Unlock()
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
				// set election timer
				rf.electionTimerMutex.Lock()
				rf.electionTimer = time.Now().UnixMilli()
				rf.electionTimerMutex.Unlock()
				// log
				rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(i)+"), term-over: find big term peer.")
				return
			} else {
				rf.currentTermMutex.Unlock()
			}

			// success
			if reply.Success == 1 {
				// log
				rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(i)+"), success")
				// do something
				// fail
			} else {
				// log
				rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(i)+"), fail")
			}
		} else {
			rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(i)+"), no-response")
		}
	}
}

func (rf *Raft) HeartBeat() {
	// log
	rf.Log(1, "HeartBeat("+strconv.Itoa(rf.me)+"): Start")
	for !rf.killed() {
		rf.Log(3, "HeartBeat("+strconv.Itoa(rf.me)+"): Loop")
		rf.currentTermMutex.Lock()
		rf.currentStateMutex.Lock()
		if rf.currentState == Leader {
			term := rf.currentTerm
			rf.currentStateMutex.Unlock()
			rf.currentTermMutex.Unlock()
			// send AppendEntries
			go rf.sendAppendEntries(term)
		} else {
			rf.currentStateMutex.Unlock()
			rf.currentTermMutex.Unlock()
			break
		}
		time.Sleep(rf.heartBeatInterval * time.Millisecond)
	}
}

//
// election
// result: true for success, false for fail or not a candidate
func (rf *Raft) election() bool {
	rf.currentTermMutex.Lock()
	rf.currentStateMutex.Lock()
	if rf.currentState != Candidate {
		// term changed
		rf.currentStateMutex.Unlock()
		rf.currentTermMutex.Unlock()
		return true
	}
	rf.votedForMutex.Lock()
	// increment term & get current term
	rf.currentTerm += 1
	term := rf.currentTerm
	// state
	state := rf.currentState
	// vote for myself
	rf.votedFor = rf.me
	rf.votedForMutex.Unlock()
	rf.currentStateMutex.Unlock()
	rf.currentTermMutex.Unlock()
	// reset election timer
	rf.electionTimerMutex.Lock()
	rf.electionTimer = time.Now().UnixMilli()
	rf.electionTimerMutex.Unlock()
	rf.Log(1, "election("+strconv.Itoa(rf.me)+"): start; state: "+string(state)+"; term: "+strconv.Itoa(term))

	request := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: 0, // TODO
		LastLogTerm:  0, // TODO
	}
	reply := RequestVoteReply{}

	currentVotesCount := 1

	// ask for others' vote.
	for id, _ := range rf.peers {
		if id == rf.me {
			// don't need to vote for myself.
			continue
		}
		// request for vote
		rf.Log(2, "election("+strconv.Itoa(rf.me)+"): request("+strconv.Itoa(id)+") for vote")
		ok := rf.sendRequestVote(id, &request, &reply)
		if ok {
			rf.Log(2, "election("+strconv.Itoa(rf.me)+"): get("+strconv.Itoa(id)+")'s vote response")
			if rf.currentTermMutex.Lock(); reply.Term > rf.currentTerm {
				rf.Log(2, "election("+strconv.Itoa(rf.me)+"): follow to("+strconv.Itoa(id)+")")
				// currentTerm < term
				rf.currentStateMutex.Lock()
				rf.votedForMutex.Lock()
				// be follower
				rf.currentTerm = reply.Term
				// change state to Follower
				rf.currentState = Follower
				// change votedFor
				rf.votedFor = id
				rf.votedForMutex.Unlock()
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
				// change heartbeat time
				rf.electionTimerMutex.Lock()
				rf.electionTimer = time.Now().UnixMilli()
				rf.electionTimerMutex.Unlock()
				return true
			} else {
				rf.currentTermMutex.Unlock()
			}

			if reply.VoteGranted {
				// get vote
				currentVotesCount += 1
				rf.Log(2, "election("+strconv.Itoa(rf.me)+"): get-vote-success("+strconv.Itoa(id)+"); total votes("+strconv.Itoa(currentVotesCount)+")")
			} else {
				// not get vote
				rf.Log(2, "election("+strconv.Itoa(rf.me)+"): get-vote-fail("+strconv.Itoa(id)+"), total votes("+strconv.Itoa(currentVotesCount)+")")
			}
		} else {
			rf.Log(2, "election("+strconv.Itoa(rf.me)+"): get-no("+strconv.Itoa(id)+")'s vote response")
		}
	}
	if currentVotesCount > len(rf.peers)/2 {
		// log
		rf.Log(1, "election("+strconv.Itoa(rf.me)+"): be leader; term("+strconv.Itoa(term)+")")
		// be leader
		// change state
		rf.currentStateMutex.Lock()
		rf.currentState = Leader
		rf.currentStateMutex.Unlock()

		// start heartbeat go routine
		go rf.HeartBeat()
		return true
	} else {
		return false
	}
}

// AppendEntries
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	if rf.currentTermMutex.Lock(); rf.currentTerm < args.Term {
		// if currentTerm < term
		rf.currentStateMutex.Lock()
		rf.votedForMutex.Lock()
		// change currentTerm
		rf.currentTerm = args.Term
		// change state to Follower
		rf.currentState = Follower
		// change votedFor
		rf.votedFor = args.LeaderId
		rf.votedForMutex.Unlock()
		rf.currentStateMutex.Unlock()
		rf.currentTermMutex.Unlock()

		// reset election timer
		rf.electionTimerMutex.Lock()
		rf.electionTimer = time.Now().UnixMilli()
		rf.electionTimerMutex.Unlock()
		// reply
		reply.Term = args.Term
	} else if rf.currentTerm == args.Term {
		rf.currentStateMutex.Lock()
		switch rf.currentState {
		case Follower:
			// do something
			rf.votedForMutex.Lock()
			if rf.votedFor == args.LeaderId {
				rf.votedForMutex.Unlock()
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
				// reset heartbeat time
				rf.electionTimerMutex.Lock()
				rf.electionTimer = time.Now().UnixMilli()
				rf.electionTimerMutex.Unlock()
			} else {
				rf.votedForMutex.Unlock()
				rf.currentStateMutex.Unlock()
				rf.currentTermMutex.Unlock()
			}
		case Candidate:
			rf.votedForMutex.Lock()
			// change to Follower
			rf.currentState = Follower
			// change votedFor
			rf.votedFor = args.LeaderId
			rf.votedForMutex.Unlock()
			rf.currentStateMutex.Unlock()
			rf.currentTermMutex.Unlock()

			// reset heartbeat time
			rf.electionTimerMutex.Lock()
			rf.electionTimer = time.Now().UnixMilli()
			rf.electionTimerMutex.Unlock()
		default:
			rf.currentStateMutex.Unlock()
			rf.currentTermMutex.Unlock()
		}
		reply.Term = args.Term
	} else {
		reply.Success = 0
		reply.Term = rf.currentTerm
		rf.currentTermMutex.Unlock()
	}
}

//
// Make
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.heartBeatInterval = 200
	rf.timeout = time.Duration(800 + rand.Int()%200)
	rf.verbose = getVerbosity()
	rf.currentState = Follower
	rf.votedFor = -1
	rf.electionTimer = 0
	rf.isPeersLive = make([]int64, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// log
	rf.Log(1, "Make("+strconv.Itoa(me)+"): timeout: ("+strconv.Itoa(int(rf.timeout))+")")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) Log(verbose int, args string) {
	if rf.verbose >= verbose {
		log.Println(args)
	}
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

// Lock Order
//	1. rf.currentTermMutex.Lock()
//	2. rf.currentStateMutex.Lock()
//	3. rf.votedForMutex.Lock()
