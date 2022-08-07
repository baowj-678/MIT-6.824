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

type InstallSnapshot struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file [not implement]
	Data              []byte // raw bytes of the snapshot chunk, starting at	offset int
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

//
// AppendEntriesReply
// reply after leader sent AppendEntries.
type AppendEntriesReply struct {
	Term          int // currentTerm, for leader to update itself, -2 represent dead
	Success       int // 1 if follower contained entry matching;\n 0 if not matching;\n -1 if not Follower to this Leader;\n -2 if need installSnapshot.
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
	applyMsgInterval  time.Duration // default: (20ms)
	electionTimer     int64         //
	verbose           int           // for log

	// Persistent
	currentTerm int // current term.
	votedFor    int // which peer I vote for, -1 for no one.
	log         []LogEntry
	snapshot    []byte

	// Volatile on All servers.
	commitIndex       int // Index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied       int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastIncludedIndex int // snapshot last include index
	lastIncludedTerm  int // snapshot the term of lastIncludeIndex

	// Volatile on Leaders
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
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.Log(1, "persist(%v): term(%v); votedFor(%v); logLen(%v)", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(state []byte, snapshot []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(state)
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

	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	// snapshot
	r = bytes.NewBuffer(snapshot)
	d = labgob.NewDecoder(r)
	var lastIncludeIndex int
	if d.Decode(&lastIncludeIndex) != nil {
	} else {
		rf.mu.Lock()
		rf.lastIncludedIndex = lastIncludeIndex
		rf.snapshot = snapshot
		rf.Log(1, "readPersist("+strconv.Itoa(rf.me)+"): lastIncludeIndex("+strconv.Itoa(lastIncludeIndex)+"); lastIncludeTerm("+strconv.Itoa(lastIncludeIndex)+"); ")
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
func (rf *Raft) Snapshot(newLastIncludedIndex int, snapshot []byte) {
	rf.Log(1, "Snapshot("+strconv.Itoa(rf.me)+"): Enter; lastIncludeIndex("+strconv.Itoa(newLastIncludedIndex)+"); ")
	// Your code here (2D).
	rf.mu.Lock()
	if newLastIncludedIndex > rf.lastIncludedIndex {
		// snapshot
		index := newLastIncludedIndex - rf.lastIncludedIndex - 1
		lastIncludeTerm := rf.log[index].CommandTerm
		rf.Log(2, "Snapshot(%v): oldLastIncludeIndex(%v); oldLastIncludeTerm(%v), newLastIncludeIndex(%v), newLastIncludeTerm(%v), logLen(%v)\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, newLastIncludedIndex, lastIncludeTerm, len(rf.log))
		rf.lastIncludedIndex = newLastIncludedIndex
		rf.lastIncludedTerm = lastIncludeTerm
		if rf.log[index].CommandIndex != newLastIncludedIndex {
			log.Fatalf("Snapshot(%v): lastIncludeIndex(%v); lastIncludeTerm(%v), newLastIncludedIndex(%v) \n log:(%v)", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, newLastIncludedIndex, rf.log)
		}
		rf.snapshot = snapshot
		// remove log
		rf.log = rf.log[index+1:]
		// persist
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshot, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.Log(2, "InstallSnapshot(%v): currentTerm(%v) < term(%v) from peer(%v)", rf.me, rf.currentTerm, args.Term, args.LeaderId)
		rf.convertToFollower(args.Term)
	}
	rf.Log(2, "InstallSnapshot(%v): this.lastIncludedIndex(%v), leader.lastIncludedIndex(%v)\n", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
	if rf.currentTerm == args.Term {
		if rf.currentState == Follower {
			if rf.lastIncludedIndex < args.LastIncludedIndex {
				// remove some log
				if len(rf.log) > 0 {
					latestLogIndex := rf.log[len(rf.log)-1].CommandIndex
					if latestLogIndex > args.LastIncludedIndex {
						rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
						if rf.log[0].CommandIndex != args.LastIncludedIndex+1 {
							panic("index is not continuous")
						}
					} else {
						rf.log = []LogEntry{}
					}
				}
				// install
				rf.snapshot = args.Data
				rf.lastIncludedIndex = args.LastIncludedIndex
				rf.lastIncludedTerm = args.LastIncludedTerm
				rf.persist()
			}
		}
	}
	reply.Term = rf.currentTerm
}

// sendInstallSnapshot
func (rf *Raft) sendInstallSnapshot(server int) {
	rf.Log(2, "sendInstallSnapshot(%v): peer(%v)", rf.me, server)
	rf.mu.Lock()
	request := InstallSnapshot{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	// send RPC
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &request, &reply)
	if ok {
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.Log(2, "sendInstallSnapshot(%v): currentTerm(%v) < term(%v) from peer(%v)", rf.me, rf.currentTerm, reply.Term, server)
			rf.convertToFollower(reply.Term)
		} else {
			// update nextIndex
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			rf.matchIndex[server] = rf.lastIncludedIndex
		}
		rf.mu.Unlock()
	}
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
		rf.Log(2, "RequestVote(%v): currentTerm(%v) < term(%v) from peer(%v)", rf.me, rf.currentTerm, args.Term, args.CandidateId)
		rf.convertToFollower(args.Term)
		reply.Term = args.Term
		reply.VoteGranted = false
	}
	if rf.currentTerm == args.Term {
		rf.Log(2, "RequestVote(%v): currentTerm(%v) = term(%v) from peer(%v)", rf.me, rf.currentTerm, args.Term, args.CandidateId)
		if rf.currentState == Follower {
			// vote for this peer
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				// if: candidate’s log is at least as up-to-date as receiver’s log.
				latestLogIndex, latestLogTerm := rf.getLatestLogIndex()
				rf.Log(2, "RequestVote(%v): currentTerm(%v), commandTerm(%v), logLen(%v); peer(%v), Term(%v), lastLogTerm(%v), logLen(%v)", rf.me, rf.currentTerm, latestLogTerm, latestLogIndex, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex)
				if latestLogTerm < args.LastLogTerm || (latestLogTerm == args.LastLogTerm && latestLogIndex <= args.LastLogIndex) {
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
		rf.Log(1, "RequestVote(%v): currentTerm(%v) > term(%v) from peer(%v)", rf.me, rf.currentTerm, args.Term, args.CandidateId)
		reply.Term = rf.currentTerm
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
		rf.Log(2, "sendRequestVote(%v): get(%v)'s vote response", rf.me, server)
		if reply.Term > rf.currentTerm {
			rf.Log(2, "sendRequestVote(%v): follow to(%v)", rf.me, server)
			// currentTerm < term be follower
			rf.convertToFollower(reply.Term)
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
				rf.Log(2, "sendRequestVote(%v): get-vote-success(%v)", rf.me, server)
			} else {
				// not get vote
				ch <- 0
				rf.Log(2, "sendRequestVote(%v): get-vote-fail(%v)", rf.me, server)
			}
		}
	} else {
		rf.Log(2, "sendRequestVote(%v): get-no(%v)'s vote response", rf.me, server)
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
	term := rf.currentTerm
	isLeader := rf.currentState == Leader
	if !isLeader {
		return -1, term, isLeader
	} else {
		index, _ := rf.getLatestLogIndex()
		index += 1
		newLogEntry := LogEntry{
			Command:      command,
			CommandTerm:  term,
			CommandIndex: index,
		}
		rf.log = append(rf.log, newLogEntry)
		rf.matchIndex[rf.me] = index
		rf.Log(1, "Start(%v): Command(%v); CommandIndex(%v); State(%v); Term(%v)\n", rf.me, command, index, rf.currentState, rf.currentTerm)
		go rf.sendAllAppendEntries(term)
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
	rf.Log(1, "Killed(%v)", rf.me)
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
	rf.Log(1, "ticker(%v): start", rf.me)
	for rf.killed() == false {
		// log
		rf.Log(1, "ticker(%v): not killed loop", rf.me)
		// check if a leader election should be started and to randomize sleeping time
		rf.mu.Lock()
		if time.Now().UnixMilli()-rf.electionTimer > int64(rf.electionTimeout) {
			// electionTimeout, reset electionTimeout
			if rf.currentState == Follower || rf.currentState == Candidate {
				rf.resetElectionTimeout()
				rf.Log(1, "ticker(%v): electionTimeout, state(%v); term(%v)", rf.me, rf.currentState, rf.currentTerm)
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
		// sleep a rand electionTimeout
		time.Sleep(rf.electionTimeout / 2 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, term int, ch chan int) {
	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	rf.mu.Lock()
	if rf.currentState != Leader || rf.currentTerm != term {
		// is not leader
		rf.mu.Unlock()
		ch <- 0
		return
	}
	// get entries
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	var entries []LogEntry
	if rf.lastIncludedIndex < prevLogIndex {
		index := prevLogIndex - rf.lastIncludedIndex - 1
		prevLogTerm = rf.log[index].CommandTerm
		entries = make([]LogEntry, len(rf.log)-index-1)
		if len(rf.log)-index-1 > 0 {
			copy(entries, rf.log[index+1:])
		}
	} else if rf.lastIncludedIndex == prevLogIndex {
		prevLogTerm = rf.lastIncludedTerm
		entries = make([]LogEntry, len(rf.log))
		copy(entries, rf.log)
	} else {
		// install snapshot
		rf.mu.Unlock()
		go rf.sendInstallSnapshot(server)
		ch <- 0
		return
	}

	request := AppendEntries{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	index, _ := rf.getLatestLogIndex()
	rf.Log(2, "sendAppendEntries(%v): peer(%v); term(%v);  prevLogIndex(%v); entriesLen(%v), commitIndex(%v), lastIncludedIndex(%v), lastIndex(%v)\n", rf.me, server, rf.currentTerm, request.PrevLogIndex, len(request.Entries), rf.commitIndex, rf.lastIncludedIndex, index)
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
			rf.Log(2, "sendAppendEntries(%v): peer(%v); currentTerm(%v); requestTerm(%v); term-over: find bigger term peer\n", rf.me, server, rf.currentTerm, request.Term)
			// set election timer
			rf.electionTimer = time.Now().UnixMilli() // TODO 重新设置timer对性能有什么影响
			// log
			rf.mu.Unlock()
			ch <- 0
			return
		}

		if rf.currentState != Leader || rf.currentTerm != request.Term {
			rf.Log(2, "sendAppendEntries(%v): peer(%v); currentTerm(%v); requestTerm(%v); Leader(%v); term changed || not Leader after reply.\n", rf.me, server, rf.currentTerm, request.Term, rf.currentState)
			rf.mu.Unlock()
			ch <- 0
			return
		}

		if reply.Success == 1 {
			// success: update nextIndex and matchIndex for follower
			newNextIndex := rf.nextIndex[server]
			if len(request.Entries) > 0 {
				newNextIndex = request.Entries[len(request.Entries)-1].CommandIndex + 1
			}
			// next index & match index must increase
			if newNextIndex > rf.nextIndex[server] {
				rf.nextIndex[server] = newNextIndex
				rf.matchIndex[server] = newNextIndex - 1
			}
			// send channel for starting commit
			ch <- 1
			rf.Log(2, "sendAppendEntries(%v): peer(%v); currentTerm(%v); requestTerm(%v); nextIndex(%v); matchIndex(%v)\n", rf.me, server, rf.currentTerm, request.Term, newNextIndex, newNextIndex-1)
		} else if reply.Success == 0 {
			// fail because of log inconsistency, decrement nextIndex and retry
			rf.Log(2, "sendAppendEntries(%v): peer(%v), currentTerm(%v); requestTerm(%v); conflictIndex(%v); conflictTerm(%v); fail for log inconsistency\n", rf.me, server, rf.currentTerm, request.Term, reply.ConflictIndex, reply.ConflictTerm)
			if reply.ConflictTerm == 0 {
				if reply.ConflictIndex < rf.nextIndex[server] {
					rf.nextIndex[server] = reply.ConflictIndex
					go rf.sendAppendEntries(server, term, ch)
				} else {
					ch <- 0
				}
			} else {
				rf.decrementNextIndex(server, reply.ConflictTerm, request.PrevLogIndex)
				go rf.sendAppendEntries(server, term, ch)
			}
		} else if reply.Success == -1 {
			ch <- 0
			rf.Log(2, "sendAppendEntries(%v): peer(%v), currentTerm(%v); requestTerm(%v); fail not for log inconsistency\n", rf.me, server, rf.currentTerm, request.Term)
		} else if reply.Success == -2 {
			ch <- 0
			// install
			rf.Log(2, "sendAppendEntries(%v): peer(%v), currentTerm(%v); requestTerm(%v); Install Snapshot\n", rf.me, server, rf.currentTerm, request.Term)
			go rf.sendInstallSnapshot(server)
		}
	} else {
		ch <- 0
		// wrong TODO 网络不好的情况下怎么办？
		rf.nextIndex[server] = request.PrevLogIndex + 1
		rf.Log(2, "sendAppendEntries(%v): peer(%v), currentTerm(%v); requestTerm(%v); no-response\n", rf.me, server, rf.currentTerm, request.Term)
	}
	rf.mu.Unlock()
}

// decrementNextIndex. [No Lock]
// find the first log'Index of conflictTerm.
func (rf *Raft) decrementNextIndex(server int, conflictTerm int, prevLogIndex int) {
	rf.nextIndex[server] = rf.lastIncludedIndex
	nowLogIndex := prevLogIndex - rf.lastIncludedIndex - 1
	for i := nowLogIndex - 1; i >= 0; i-- {
		if rf.log[i].CommandTerm == conflictTerm-1 {
			rf.nextIndex[server] = rf.log[i].CommandIndex + 1
			break
		}
	}
}

// The heartBeat go routine will send heartbeat periodically
func (rf *Raft) sendAllAppendEntries(term int) {
	rf.Log(1, "sendAllAppendEntries(%v): Start\n", rf.me)
	// start send heartbeat
	ch := make(chan int)
	for i, _ := range rf.peers {
		if i == rf.me {
			// not need to send Heartbeat to self.
			continue
		}
		go rf.sendAppendEntries(i, term, ch)
	}
	finish := 1
	success := 1
	for i := range ch {
		finish += 1
		if i == 1 {
			success += 1
		}
		if success > len(rf.peers)/2 {
			go rf.commit()
		}
		if finish == len(rf.peers) {
			close(ch)
			break
		}
	}
}

func (rf *Raft) HeartBeat() {
	// log
	rf.Log(1, "HeartBeat(%v): Start", rf.me)
	for !rf.killed() {
		rf.Log(3, "HeartBeat(%v): Loop", rf.me)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// only leader can commit
	if rf.currentState != Leader {
		return
	}
	// commit
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex) // by increase order
	majorityMatchIndex := 0
	if len(matchIndex)%2 == 0 {
		majorityMatchIndex = matchIndex[(len(matchIndex)-1)/2]
	} else {
		majorityMatchIndex = matchIndex[len(matchIndex)/2]
	}
	rf.Log(2, "commit(%v): commitIndex(%v), majorityMatchIndex(%v)\n", rf.me, rf.commitIndex, majorityMatchIndex)
	// newCommitIndex > oldCommitIndex
	if majorityMatchIndex > rf.commitIndex {
		index := majorityMatchIndex - rf.lastIncludedIndex
		commandTerm := 0
		if index > 0 {
			if rf.log[index-1].CommandIndex != majorityMatchIndex {
				log.Fatalf("commit(%v): commandIndex(%v) != majorityMatchIndex(%v), lastIncludedIndex(%v)\nlog:(%v)\n", rf.me, rf.log[index-1].CommandIndex, majorityMatchIndex, rf.lastIncludedIndex, rf.log)
				panic("index is not continuous")
			}
			commandTerm = rf.log[index-1].CommandTerm
		} else {
			commandTerm = rf.currentTerm
		}
		// to commit entry's term must equals to currentTerm
		if commandTerm == rf.currentTerm {
			rf.commitIndex = majorityMatchIndex
			rf.Log(2, "commit(%v): commitIndex(%v), majorityMatchIndex(%v)\n", rf.me, rf.commitIndex, majorityMatchIndex)
		}
	}
}

// ApplyMsg
func (rf *Raft) ApplyMsg() {
	rf.Log(1, "ApplyMsg(%v): start", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.Log(2, "ApplyMsg(%v): lastApplied(%v); commitIndex(%v); lastIncludedIndex(%v); logLen(%v)\n", rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex, len(rf.log))
			rf.persist()
			// apply
			if rf.lastApplied < rf.lastIncludedIndex {
				// apply snapshot
				snapshot := make([]byte, len(rf.snapshot))
				copy(snapshot, rf.snapshot)
				go rf.sendApplyMsgSnapshot(snapshot, rf.lastIncludedIndex, rf.lastIncludedTerm)
				rf.lastApplied = rf.lastIncludedIndex
			} else {
				// apply log
				lastAppliedIndex := rf.lastApplied - rf.lastIncludedIndex - 1
				commitIndexIndex := rf.commitIndex - rf.lastIncludedIndex - 1
				entries := make([]LogEntry, commitIndexIndex-lastAppliedIndex)
				copy(entries, rf.log[lastAppliedIndex+1:commitIndexIndex+1])
				go rf.sendApplyMsgLog(entries)
				rf.lastApplied = rf.commitIndex // TODO
			}
		}
		rf.mu.Unlock()
		time.Sleep(rf.applyMsgInterval * time.Millisecond)
	}
}

func (rf *Raft) sendApplyMsgSnapshot(snapshot []byte, lastIncludedIndex int, lastIncludedTerm int) {
	rf.Log(1, "sendApplyMsgSnapshot(%v): lastIncludedIndex(%v); lastIncludedTerm(%v)", rf.me, lastIncludedIndex, lastIncludedTerm)
	newCommitApplyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		SnapshotIndex: lastIncludedIndex,
		SnapshotTerm:  lastIncludedTerm,
		Snapshot:      snapshot,
	}
	rf.applyChan <- newCommitApplyMsg
}

// sendApplyMsg.
func (rf *Raft) sendApplyMsgLog(entries []LogEntry) {
	// send log
	for _, entry := range entries {
		rf.Log(1, "sendApplyMsgLog(%v): start, Index(%v); Command(%v)\n", rf.me, entry.CommandIndex, entry.Command)
		newCommitApplyMsg := ApplyMsg{
			Command:      entry.Command,
			CommandIndex: entry.CommandIndex,
			CommandValid: true,
			// Snapshot false
			SnapshotValid: false,
		}
		rf.applyChan <- newCommitApplyMsg
		rf.Log(1, "sendApplyMsgLog(%v): finish, Index(%v); Command(%v)\n", rf.me, entry.CommandIndex, entry.Command)
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
	latestLogIndex, latestLogTerm := rf.getLatestLogIndex()
	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: latestLogIndex,
		LastLogTerm:  latestLogTerm,
	}
	state := rf.currentState
	// voteFor myself
	rf.votedFor = rf.me
	rf.Log(1, "election(%v): start; state(%v); term(%v)", rf.me, state, request.Term)
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
		rf.Log(2, "election(%v): request(%v) for vote", rf.me, id)
		go rf.sendRequestVote(id, &request, ch)
	}

	currentVotesCount := 1
	finishThreadCount := 0
	// wait for reply
	for ok := range ch {
		finishThreadCount += 1
		if ok == 1 {
			currentVotesCount += 1
			rf.Log(1, "election(%v): total-vote(%v)", rf.me, currentVotesCount)
		}
		if currentVotesCount > len(rf.peers)/2 {
			// change state
			rf.mu.Lock()
			if rf.currentTerm == request.Term {
				// if term not changed, be leader
				rf.Log(1, "election(%v): be leader; term(%v)", rf.me, rf.currentTerm)
				// init
				rf.initLeader()
				rf.mu.Unlock()
				rf.Start("new-leader")
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

func (rf *Raft) getLatestLogIndex() (lastLogIndex int, lastLogTerm int) {
	lastLogIndex = rf.lastIncludedIndex
	lastLogTerm = rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].CommandIndex
		lastLogTerm = rf.log[len(rf.log)-1].CommandTerm
	}
	return lastLogIndex, lastLogTerm
}

// initLeader. [No Lock]
func (rf *Raft) initLeader() {
	rf.currentState = Leader
	index, _ := rf.getLatestLogIndex()
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = index + 1
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
	// start heartbeat go routine
	go rf.HeartBeat()
	//fmt.Println("new-leader")
}

// AppendEntries
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	// check term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		// if currentTerm < term
		rf.Log(1, "AppendEntries(%v): currentTerm(%v) < term(%v) from peer(%v)", rf.me, rf.currentTerm, args.Term, args.LeaderId)
		rf.convertToFollower(args.Term)
		reply.Term = args.Term
	} else if rf.currentTerm > args.Term {
		// reply false
		reply.Success = -1
		reply.Term = rf.currentTerm
		return
	}

	// check log
	// reset heartbeat time
	rf.electionTimer = time.Now().UnixMilli()

	if rf.currentState != Follower {
		rf.currentState = Follower
	}
	if rf.votedFor != args.LeaderId {
		rf.votedFor = args.LeaderId
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	reply.Success = 0
	latestIndex, latestTerm := rf.getLatestLogIndex()
	rf.Log(2, "AppendEntries(%v): lastIncludeIndex(%v); latestIndex(%v); lastIncludeTerm(%v); latestTerm(%v); prevLogIndex(%v); prevLogTerm(%v) logLen(%v), entriesLen(%v), commitIndex(%v)\n", rf.me, rf.lastIncludedIndex, latestIndex, rf.lastIncludedTerm, latestTerm, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), len(args.Entries), rf.commitIndex)
	if rf.lastIncludedIndex > args.PrevLogIndex {
		// lastIncludedIndex > prevLogIndex
		if len(args.Entries) > 0 {
			if args.Entries[len(args.Entries)-1].CommandIndex > rf.lastIncludedIndex {
				// lastEntriesIndex >= lastIncludedIndex > prevLogIndex
				index := rf.lastIncludedIndex - args.PrevLogIndex
				if rf.lastIncludedIndex != args.Entries[index-1].CommandIndex {
					panic("index is not continuous")
				}
				// append [index:] to log
				rf.doAppendEntries(args.Entries[index:], 0)
			}
		}
		reply.Success = 1
	} else if rf.lastIncludedIndex < args.PrevLogIndex {
		// lastIncludedIndex < prevLogIndex
		if len(rf.log) > 0 {
			if args.PrevLogIndex <= rf.log[len(rf.log)-1].CommandIndex {
				// lastIncludedIndex < prevLogIndex <= lastLogIndex
				index := args.PrevLogIndex - rf.lastIncludedIndex - 1
				if rf.log[index].CommandIndex != args.PrevLogIndex {
					panic("index is not continuous")
				}
				if rf.log[index].CommandTerm == args.PrevLogTerm {
					rf.doAppendEntries(args.Entries, index+1)
					reply.Success = 1
				} else {
					// log not match
					reply.ConflictTerm = rf.log[index].CommandTerm
					reply.ConflictIndex = 0
					rf.log = rf.log[:index]
					rf.persist()
				}
			} else {
				// lastIncludedIndex < prevLogIndex >= lastLogIndex
				// set conflict index
				reply.ConflictIndex = rf.log[len(rf.log)-1].CommandIndex + 1
				reply.ConflictTerm = 0
			}
		} else {
			// set conflict index
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			reply.ConflictTerm = 0
		}
	} else {
		// lastIncludedIndex == prevLogIndex
		// append [0:] to log
		rf.doAppendEntries(args.Entries, 0)
		reply.Success = 1
	}
	// commit
	if reply.Success == 1 {
		if args.LeaderCommit > rf.commitIndex {
			oldCommitIndex := rf.commitIndex
			index, _ := rf.getLatestLogIndex()
			if index < args.LeaderCommit {
				rf.Log(2, "AppendEntries(%v): term(%v); commitIndex from(%v) to(%v)\n", rf.me, rf.currentTerm, oldCommitIndex, index)
				rf.commitIndex = index
			} else {
				rf.Log(2, "AppendEntries(%v): term(%v); commitIndex from(%v) to(%v)\n", rf.me, rf.currentTerm, oldCommitIndex, args.LeaderCommit)
				rf.commitIndex = args.LeaderCommit
			}
		}
	}
}

// convertToFollower. [No Lock]
func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.currentState = Follower
	rf.persist()
}

// doAppendEntries. [No Lock, No Persist]
// Append new Entries to this server's Log(from logIndex)
func (rf *Raft) doAppendEntries(entries []LogEntry, logIndex int) {
	oldLogLen := len(rf.log)
	if len(rf.log) == logIndex {
		rf.log = append(rf.log, entries...)
	} else if len(rf.log) > logIndex {
		i := 0
		for ; i < len(entries) && logIndex < len(rf.log); i++ {
			if rf.log[logIndex] != (entries)[i] {
				// not match -> remove all log after logIndex and append new entries
				rf.log = append(rf.log[:logIndex], (entries)[i:]...)
				return
			}
			logIndex += 1
		}
		if logIndex == len(rf.log) && i < len(entries) {
			// len(entries) > len(log) -> append new entries
			rf.log = append(rf.log, (entries)[i:]...)
		}
	} else {
		panic("doAppendEntries: len(rf.log) < logIndex")
	}
	rf.persist()
	rf.Log(2, "doAppendEntries(%v): log len from(%v) to(%v)", rf.me, oldLogLen, len(rf.log))
}

func (rf *Raft) resetElectionTimeout() time.Duration {
	rf.electionTimeout = time.Duration(800 + rand.Int()%200)
	rf.Log(1, "resetElectionTimeout(%v): %v", rf.me, rf.electionTimeout)
	return rf.electionTimeout
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
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
	rf.Log(1, "Make(%v)", rf.me)
	// Your initialization code here (2A, 2B, 2C).

	// Time
	rf.heartBeatInterval = 120
	rf.applyMsgInterval = 20
	rf.electionTimeout = time.Duration(400 + rand.Int()%200)
	//rf.resetElectionTimeout()
	rf.electionTimer = time.Now().UnixMilli()

	rf.currentState = Follower
	rf.applyChan = applyCh
	// Persist
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = []LogEntry{}
	// init snapshot
	rf.snapshot = nil

	// Volatile
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0
	// Leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// log
	rf.verbose = getVerbosity()
	// debug
	//f, err := os.OpenFile("out.txt", os.O_APPEND, os.ModePerm)
	//if err == nil {
	//	rf.debugLogFile = f
	//} else {
	//	f.Close()
	//}
	//log.SetOutput(f)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyMsg()
	return rf
}

func (rf *Raft) Log(verbose int, args string, v ...interface{}) {
	if rf.verbose >= verbose {
		if len(v) > 0 {
			log.Printf(args, v...)
		} else {
			log.Println(args)
		}
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
	return -1
}
