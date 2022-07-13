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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"os"
	"sort"
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
	Term         int        // leader's term.
	LeaderId     int        // so followers can redirect clients.
	PrevLogIndex int        // index of log entry immediately preceding new ones.
	PrevLogTerm  int        // term of PrevLogIndex entry.
	Entries      []LogEntry // log entries to store(empty for heartbeat, may send more than one for efficiency).
	LeaderCommit int        // leader's commitIndex.
}

//
// AppendEntriesReply
// reply after leader sent AppendEntries.
type AppendEntriesReply struct {
	Term          int // currentTerm, for leader to update itself, -2 represent dead
	Success       int // 1 ifs follower contained entry matching; 0 if not matching; -1 if not Follower to this Leader.
	ConflictIndex int // the first index it stores for that term.
	ConflictTerm  int // the term of the conflicting entry.
}

// LogEntry
// contains command
// for state machine, and term when entry
// was received by leader (first index is 1)
type LogEntry struct {
	Command      interface{} // command for state machine
	CommandTerm  int         // term when entry was received by leader
	CommandIndex int
}

//
// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	debugLogFile *os.File
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	applyChan    chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimeout   time.Duration // rand electionTimeout: (from 800 to 1000ms)
	heartBeatInterval time.Duration // default: (150ms)
	electionTimer     int64         //
	verbose           int           // for log

	// Persistent
	currentTerm int // current term.
	votedFor    int // which peer I vote for, -1 for no one.
	log         []LogEntry

	// Volatile on All servers.
	commitIndex int // Index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile on Leaders TODO:nextIndex和matchIndex区别是什么？
	nextIndex  []int // for each server, index of the next log entry	to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Other data
	currentState State
}

//
// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.currentState == Leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage. [No Lock]
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.Log(1, "persist("+strconv.Itoa(rf.me)+"): term("+strconv.Itoa(rf.currentTerm)+"); votedFor("+strconv.Itoa(rf.votedFor)+"); logLen("+strconv.Itoa(len(rf.log))+")")
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		rf.Log(1, "readPersist("+strconv.Itoa(rf.me)+"): term("+strconv.Itoa(rf.currentTerm)+"); votedFor("+strconv.Itoa(rf.votedFor)+"); logLen("+strconv.Itoa(len(rf.log))+")")
		rf.mu.Unlock()
	}
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
	reply.Term = args.Term
	reply.VoteGranted = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.Log(2, "RequestVote("+strconv.Itoa(rf.me)+"): currentTerm("+strconv.Itoa(rf.currentTerm)+") < term("+strconv.Itoa(args.Term)+") from peer("+strconv.Itoa(args.CandidateId)+")")
		rf.convertToFollower(args.Term)
		rf.persist()
		// reset heartbeat timer TODO 是否要reset timer
		//rf.electionTimer = time.Now().UnixMilli()
		reply.Term = args.Term
		// vote for this peer
		reply.VoteGranted = false
	}
	if rf.currentTerm == args.Term {
		rf.Log(2, "RequestVote("+strconv.Itoa(rf.me)+"): currentTerm("+strconv.Itoa(rf.currentTerm)+") = term("+strconv.Itoa(args.Term)+") from peer("+strconv.Itoa(args.CandidateId)+")")
		if rf.currentState == Follower {
			// vote for this peer
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				// if: candidate’s log is at least as up-to-date as receiver’s log.
				rf.Log(2, "RequestVote("+strconv.Itoa(rf.me)+"): currentTerm("+strconv.Itoa(rf.currentTerm)+"), commandTerm("+strconv.Itoa(rf.log[len(rf.log)-1].CommandTerm)+"), logLen("+strconv.Itoa(len(rf.log)-1)+"); peer("+strconv.Itoa(args.CandidateId)+"), Term("+strconv.Itoa(args.Term)+"), lastLogTerm("+strconv.Itoa(args.LastLogTerm)+"), logLen("+strconv.Itoa(args.LastLogIndex)+")")
				if rf.log[len(rf.log)-1].CommandTerm < args.LastLogTerm ||
					(rf.log[len(rf.log)-1].CommandTerm == args.LastLogTerm && len(rf.log) <= args.LastLogIndex+1) {
					// vote for candidate
					reply.VoteGranted = true
					rf.votedFor = args.CandidateId
					rf.persist()
					// reset election timer if and only if grant vote
					rf.electionTimer = time.Now().UnixMilli()
				}
			} else {
				reply.VoteGranted = false
			}
		}
		reply.Term = args.Term
	} else {
		// currentTerm > term
		rf.Log(1, "RequestVote("+strconv.Itoa(rf.me)+"): currentTerm("+strconv.Itoa(rf.currentTerm)+") > term("+strconv.Itoa(args.Term)+") from peer("+strconv.Itoa(args.CandidateId)+")")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

//
// PreRequestVote
// example PreRequestVote RPC handler.
// candidate response
func (rf *Raft) PreRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		// pre vote for this peer
		reply.VoteGranted = true
	} else if rf.currentTerm == args.Term {
		if rf.currentState == Follower {
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				// if: candidate’s log is at least as up-to-date as receiver’s log
				if rf.log[len(rf.log)-1].CommandTerm <= args.Term && rf.log[len(rf.log)-1].CommandIndex <= args.LastLogIndex {
					// vote for candidate
					reply.VoteGranted = true
				} else {
					reply.VoteGranted = false
				}
			} else {
				reply.VoteGranted = false
			}
		}
	} else {
		// currentTerm > term
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
// within a electionTimeout interval, Call() returns true; otherwise
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan int) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		rf.mu.Lock()
		rf.Log(2, "sendRequestVote("+strconv.Itoa(rf.me)+"): get("+strconv.Itoa(server)+")'s vote response")
		if reply.Term > rf.currentTerm {
			rf.Log(2, "sendRequestVote("+strconv.Itoa(rf.me)+"): follow to("+strconv.Itoa(server)+")")
			// currentTerm < term be follower
			rf.convertToFollower(reply.Term)
			rf.persist()
			// change heartbeat time
			rf.electionTimer = time.Now().UnixMilli()
			rf.mu.Unlock()
			// not get vote
			ch <- 0
		} else {
			rf.mu.Unlock()
			if reply.VoteGranted {
				// get vote
				ch <- 1
				rf.Log(2, "sendRequestVote("+strconv.Itoa(rf.me)+"): get-vote-success("+strconv.Itoa(server)+")")
			} else {
				// not get vote
				ch <- 0
				rf.Log(2, "sendRequestVote("+strconv.Itoa(rf.me)+"): get-vote-fail("+strconv.Itoa(server)+")")
			}
		}
	} else {
		rf.Log(2, "sendRequestVote("+strconv.Itoa(rf.me)+"): get-no("+strconv.Itoa(server)+")'s vote response")
		ch <- 0
	}
	return
}

// PreRequestVote
//func (rf *Raft) sendPreRequestVote(server int, args *RequestVoteArgs, ch chan int) {
//	reply := RequestVoteReply{}
//	ok := rf.peers[server].Call("Raft.PreRequestVote", args, &reply)
//	if ok {
//		if reply.VoteGranted {
//			// get vote
//			ch <- 1
//		} else {
//			// not get vote
//			ch <- 0
//		}
//	} else {
//		ch <- 0
//	}
//	return
//}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.Log(1, "Start("+strconv.Itoa(rf.me)+")"+": Command("+strconv.Itoa(command.(int))+"); State("+string(rf.currentState)+"); Term("+strconv.Itoa(rf.currentTerm)+")")
	index := -1
	term := rf.currentTerm
	isLeader := rf.currentState == Leader
	if !isLeader {
		return index, term, isLeader
	} else {
		newLogEntry := LogEntry{
			Command:      command,
			CommandTerm:  term,
			CommandIndex: len(rf.log),
		}
		index = len(rf.log)
		rf.log = append(rf.log, newLogEntry)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.persist()
		return index, term, isLeader
	}
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
		// check if a leader election should be started and to randomize sleeping time
		rf.mu.Lock()
		if time.Now().UnixMilli()-rf.electionTimer > int64(rf.electionTimeout) {
			// electionTimeout, reset electionTimeout
			rf.resetElectionTimeout()
			if rf.currentState == Follower || rf.currentState == Candidate {
				rf.Log(1, "ticker("+strconv.Itoa(rf.me)+"): electionTimeout, state: "+string(rf.currentState)+"; term: "+strconv.Itoa(rf.currentTerm))
				// change state
				rf.currentState = Candidate
				rf.mu.Unlock()
				// start election
				go rf.election()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
		// sleep a rand electionTimeout TODO 时间间隔选择:(固定时间间隔还是)
		//time.Sleep(10 * time.Millisecond)
		time.Sleep(rf.electionTimeout / 2 * time.Millisecond)
	}
	time.Sleep(time.Second)
}

func (rf *Raft) sendAppendEntries(server int, term int) {
	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	rf.mu.Lock()
	if rf.currentState != Leader || rf.currentTerm != term {
		// is not leader
		rf.mu.Unlock()
		return
	}
	prevLogIndex := len(rf.log) - 1
	prevLogTerm := rf.log[prevLogIndex].CommandTerm
	var entries []LogEntry
	leaderCommit := rf.commitIndex
	lastLogIndex := len(rf.log) - 1
	if len(rf.log) >= rf.nextIndex[server] {
		prevLogIndex = rf.nextIndex[server] - 1
		prevLogTerm = rf.log[prevLogIndex].CommandTerm
		entries = make([]LogEntry, len(rf.log)-rf.nextIndex[server])
		copy(entries, rf.log[rf.nextIndex[server]:])
	}
	request := AppendEntries{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(server)+"); term("+strconv.Itoa(rf.currentTerm)+");  lastLogIndex("+strconv.Itoa(request.PrevLogIndex)+"); entriesLen("+strconv.Itoa(len(request.Entries))+")")
	rf.mu.Unlock()

	// Sent RPC
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &request, &reply)
	// Get RPC Reply
	rf.mu.Lock()
	if ok {
		// currentTerm < term
		if rf.currentTerm < reply.Term {
			rf.convertToFollower(reply.Term)
			rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(server)+"); currentTerm("+strconv.Itoa(rf.currentTerm)+"); requestTerm("+strconv.Itoa(request.Term)+"); term-over: find bigger term peer.")
			rf.persist()
			// set election timer
			rf.electionTimer = time.Now().UnixMilli()
			// log
			rf.mu.Unlock()
			return
		}

		if rf.currentState != Leader || rf.currentTerm != request.Term {
			rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(server)+"); currentTerm("+strconv.Itoa(rf.currentTerm)+"); requestTerm("+strconv.Itoa(request.Term)+"); term changed || not Leader after reply.")
			rf.mu.Unlock()
			return
		}

		if reply.Success == 1 {
			// success: update nextIndex and matchIndex for follower
			rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(server)+"); currentTerm("+strconv.Itoa(rf.currentTerm)+"); requestTerm("+strconv.Itoa(request.Term)+"); nextIndex("+strconv.Itoa(lastLogIndex+1)+"); matchIndex("+strconv.Itoa(lastLogIndex)+")")
			rf.nextIndex[server] = lastLogIndex + 1
			rf.matchIndex[server] = lastLogIndex
		} else if reply.Success == 0 {
			//fail because of log inconsistency
			rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(server)+"), currentTerm("+strconv.Itoa(rf.currentTerm)+"); requestTerm("+strconv.Itoa(request.Term)+"); fail for log inconsistency")
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			if reply.ConflictTerm == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.decrementNextIndex(server, reply.ConflictTerm, request.PrevLogIndex)
			}
			rf.mu.Unlock()
			go rf.sendAppendEntries(server, term)
			rf.mu.Lock()
		} else {
			rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(server)+"), currentTerm("+strconv.Itoa(rf.currentTerm)+"); requestTerm("+strconv.Itoa(request.Term)+"); fail not for log inconsistency")
		}
	} else {
		// wrong
		rf.nextIndex[server] = request.PrevLogIndex + 1
		rf.Log(2, "sendAppendEntries("+strconv.Itoa(rf.me)+"): peer("+strconv.Itoa(server)+"), currentTerm("+strconv.Itoa(rf.currentTerm)+"); requestTerm("+strconv.Itoa(request.Term)+"); no-response")
	}
	rf.mu.Unlock()
}

// decrementNextIndex. [No Lock]
// find the first log'Index of conflictTerm.
func (rf *Raft) decrementNextIndex(server int, conflictTerm int, prevLogIndex int) {
	rf.nextIndex[server] = 1
	for i := prevLogIndex - 1; i >= 0; i-- {
		if rf.log[i].CommandTerm < conflictTerm {
			rf.nextIndex[server] = i + 1
			break
		}
	}
}

// The heartBeat go routine will send heartbeat periodically
func (rf *Raft) sendAllAppendEntries(term int) {
	rf.Log(1, "sendAllAppendEntries("+strconv.Itoa(rf.me)+"): Start")
	// start send heartbeat
	for i, _ := range rf.peers {
		if i == rf.me {
			// do not need to send Heartbeat to self.
			continue
		}
		go rf.sendAppendEntries(i, term)
	}
}

func (rf *Raft) HeartBeat() {
	// log
	rf.Log(1, "HeartBeat("+strconv.Itoa(rf.me)+"): Start")
	for !rf.killed() {
		rf.Log(3, "HeartBeat("+strconv.Itoa(rf.me)+"): Loop")
		rf.mu.Lock()
		if rf.currentState == Leader {
			term := rf.currentTerm
			rf.mu.Unlock()
			// send AppendEntries
			go rf.sendAllAppendEntries(term)
			// commit
			go rf.commit()
		} else {
			rf.mu.Unlock()
			break
		}
		time.Sleep(rf.heartBeatInterval * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	rf.Log(1, "commit("+strconv.Itoa(rf.me)+"): Enter")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex) // by increase order
	majorityMatchIndex := 0
	if len(matchIndex)%2 == 0 {
		majorityMatchIndex = matchIndex[(len(matchIndex)-1)/2]
	} else {
		majorityMatchIndex = matchIndex[len(matchIndex)/2]
	}
	if majorityMatchIndex > rf.commitIndex {
		if len(rf.log) >= majorityMatchIndex {
			if rf.log[majorityMatchIndex].CommandTerm == rf.currentTerm {
				// send ApplyMsg
				rf.sendApplyMsg(rf.commitIndex+1, majorityMatchIndex)
				rf.Log(2, "commit("+strconv.Itoa(rf.me)+"): from("+strconv.Itoa(rf.commitIndex)+") to("+strconv.Itoa(majorityMatchIndex)+")")
				rf.commitIndex = majorityMatchIndex
			}
		}
	}
}

func (rf *Raft) sendApplyMsg(from int, to int) {
	// send ApplyMsg
	//rf.Log(1, "sendApplyMsg("+strconv.Itoa(rf.me)+"): command("+strconv.Itoa(rf.log[from].Command.(int))+"); from("+strconv.Itoa(from)+"); to("+strconv.Itoa(to)+")")
	for i := from; i <= to; i++ {
		newCommitApplyMsg := ApplyMsg{
			Command:      rf.log[i].Command,
			CommandIndex: i,
			CommandValid: true,
		}
		rf.applyChan <- newCommitApplyMsg
	}
}

//func (rf *Raft) preVote(request *RequestVoteArgs) bool {
//	ch := make(chan int)
//	// start sendPreRequestVote goRoutine
//	for id, _ := range rf.peers {
//		if id == rf.me {
//			// don't need to vote for myself.
//			continue
//		}
//		// pre request for vote
//		go rf.sendPreRequestVote(id, request, ch)
//	}
//	currentVotesCount := 1
//	finishThreadCount := 0
//	// wait for answer
//	for ok := range ch {
//		finishThreadCount += 1
//		if ok == 1 {
//			currentVotesCount += 1
//		}
//		if currentVotesCount > len(rf.peers)/2 {
//			return true
//		}
//		if finishThreadCount == len(rf.peers)-1 {
//			break
//		}
//	}
//	return false
//}

//
// election
// result: true for success, false for fail or not a candidate
func (rf *Raft) election() {
	// Request Vote
	rf.mu.Lock()
	if rf.currentState != Candidate {
		// term changed
		rf.mu.Unlock()
		return
	}
	// increment term & get current term
	rf.currentTerm += 1
	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].CommandTerm,
	}
	state := rf.currentState
	rf.Log(1, "election("+strconv.Itoa(rf.me)+"): start; state: "+string(state)+"; term: "+strconv.Itoa(request.Term))
	// reset election timer
	rf.electionTimer = time.Now().UnixMilli()
	ch := make(chan int)
	rf.mu.Unlock()

	// start sendRequestVote goRoutine
	for id, _ := range rf.peers {
		if id == rf.me {
			// don't need to vote for myself.
			continue
		}
		// request for vote
		rf.Log(2, "election("+strconv.Itoa(rf.me)+"): request("+strconv.Itoa(id)+") for vote")
		go rf.sendRequestVote(id, &request, ch)
	}

	currentVotesCount := 1
	finishThreadCount := 0
	// wait for reply
	for ok := range ch {
		finishThreadCount += 1
		if ok == 1 {
			currentVotesCount += 1
			rf.Log(1, "election("+strconv.Itoa(rf.me)+"): total-vote("+strconv.Itoa(currentVotesCount)+")")
		}
		if currentVotesCount > len(rf.peers)/2 {
			// change state
			rf.mu.Lock()
			if rf.currentTerm == request.Term {
				// if term not changed, be leader
				rf.Log(1, "election("+strconv.Itoa(rf.me)+"): be leader; term("+strconv.Itoa(rf.currentTerm)+")")
				rf.currentState = Leader
				// init
				rf.initLeader()
				rf.mu.Unlock()
				// start heartbeat go routine
				go rf.HeartBeat()
			} else {
				rf.mu.Unlock()
			}
			break
		}
		if finishThreadCount == len(rf.peers)-1 {
			break
		}
	}

}

// initLeader. [No Lock]
func (rf *Raft) initLeader() {
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
}

// AppendEntries
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	// check term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		// if currentTerm < term
		rf.Log(2, "AppendEntries("+strconv.Itoa(rf.me)+"): currentTerm("+strconv.Itoa(rf.currentTerm)+") < term("+strconv.Itoa(args.Term)+") from peer("+strconv.Itoa(args.LeaderId)+")")
		rf.convertToFollower(args.Term)
		rf.persist()
		// reset election timer, TODO time,要不要重置
		//rf.electionTimer = time.Now().UnixMilli()
		// reply
		reply.Term = args.Term
	} else if rf.currentTerm > args.Term {
		// reply false
		reply.Success = -1
		reply.Term = rf.currentTerm
		return
	}

	// check log
	if rf.currentTerm == args.Term {
		if rf.currentState != Follower {
			rf.currentState = Follower
		}
		if rf.votedFor != args.LeaderId {
			rf.votedFor = args.LeaderId
		}
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].CommandTerm != args.PrevLogTerm {
			reply.Success = 0
			if len(rf.log) < args.PrevLogIndex+1 {
				reply.ConflictIndex = len(rf.log)
			} else if rf.log[args.PrevLogIndex].CommandTerm != args.PrevLogTerm {
				reply.ConflictTerm = rf.log[args.PrevLogIndex].CommandTerm
				reply.ConflictIndex = -1
				// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.persist()
			}
		} else {
			rf.Log(2, "AppendEntries("+strconv.Itoa(rf.me)+"): term("+strconv.Itoa(rf.currentTerm)+"); append entries")
			// do append entries
			rf.doAppendEntries(&args.Entries, args.PrevLogIndex)
			reply.Success = 1
			// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				oldCommitIndex := rf.commitIndex
				if len(rf.log) < args.LeaderCommit {
					rf.Log(2, "AppendEntries("+strconv.Itoa(rf.me)+"): term("+strconv.Itoa(rf.currentTerm)+"); commitIndex from("+strconv.Itoa(rf.commitIndex)+") to("+strconv.Itoa(len(rf.log))+")")
					rf.commitIndex = len(rf.log)
				} else {
					rf.Log(2, "AppendEntries("+strconv.Itoa(rf.me)+"): term("+strconv.Itoa(rf.currentTerm)+"); commitIndex from("+strconv.Itoa(rf.commitIndex)+") to("+strconv.Itoa(args.LeaderCommit)+")")
					rf.commitIndex = args.LeaderCommit
				}
				// send ApplyMsg
				rf.sendApplyMsg(oldCommitIndex+1, rf.commitIndex)
			}
			rf.persist()
			// reset heartbeat time
			rf.electionTimer = time.Now().UnixMilli()
		}
	} else {
		reply.Success = -1
	}
}

// convertToFollower. [No Lock]
func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.currentState = Follower
}

// doAppendEntries. [No Lock]
// Append new Entries to this server's Log
func (rf *Raft) doAppendEntries(entries *[]LogEntry, prevLogIndex int) {
	if len(*entries) == 0 {
		return
	}
	oldLogLen := len(rf.log)
	if rf.log[len(rf.log)-1].CommandTerm < (*entries)[len(*entries)-1].CommandTerm {
		// Follower's lastCommandTerm < Leader's lastCommandTerm
		rf.log = append(rf.log[:prevLogIndex+1], *entries...)
	} else {
		if len(rf.log) == prevLogIndex+1 {
			// just append to the tail.
			rf.log = append(rf.log, *entries...)
		} else if len(rf.log) > prevLogIndex+1 {
			if len(rf.log) < prevLogIndex+len(*entries)+1 {
				// log > prevLogIndex + entries TODO 需要检查吗
				rf.log = append(rf.log[:prevLogIndex+1], *entries...)
			} else {
				for i := 0; i < len(*entries); i++ {
					// log >= prevLogIndex + entries
					if rf.log[prevLogIndex+i+1] != (*entries)[i] {
						rf.log[prevLogIndex+i+1] = (*entries)[i]
					}
				}
			}
		}
	}
	rf.Log(2, "doAppendEntries("+strconv.Itoa(rf.me)+"): log len from("+strconv.Itoa(oldLogLen)+") to("+strconv.Itoa(len(rf.log))+")")
}
func (rf *Raft) resetElectionTimeout() time.Duration {
	rf.electionTimeout = time.Duration(800 + rand.Int()%200)
	rf.Log(1, "resetElectionTimeout("+strconv.Itoa(rf.me)+"): "+strconv.Itoa(int(rf.electionTimeout)))
	return rf.electionTimeout
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
	rf.electionTimeout = time.Duration(800 + rand.Int()%200)
	rf.verbose = getVerbosity()
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = 0
	rf.applyChan = applyCh
	rf.log = []LogEntry{
		{
			Command:      nil,
			CommandTerm:  0,
			CommandIndex: 0,
		},
	}
	// Volatile
	rf.commitIndex = 0
	rf.lastApplied = 0
	// Leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// debug
	//f, err := os.OpenFile("out.txt", os.O_APPEND, os.ModePerm)
	//if err == nil {
	//	rf.debugLogFile = f
	//} else {
	//	f.Close()
	//}
	//log.SetOutput(f)
	// log
	rf.Log(1, "Make("+strconv.Itoa(me)+"): electionTimeout: ("+strconv.Itoa(int(rf.electionTimeout))+")")
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
	//v := os.Getenv("VERBOSE")
	//level := 0
	//if v != "" {
	//	var err error
	//	level, err = strconv.Atoi(v)
	//	if err != nil {
	//		log.Fatalf("Invalid verbosity %v", v)
	//	}
	//}
	return 0
}

// Lock Order
//	1. rf.currentTermMutex.Lock()
//	2. rf.currentStateMutex.Lock()
//	3. rf.votedForMutex.Lock()
// 	4. rf.logMutex.Lock()
//	5. rf.nextIndexMutex.Lock()
//	5. rf.matchIndexMutex.Lock()
//	6. rf.commitIndexMutex.Lock()
