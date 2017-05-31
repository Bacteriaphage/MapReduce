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

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "fmt"

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Log struct{
    index   int
    content []byte
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
   currentTerm int               // last term server has seen 
   votedFor    int               // candidateId that received vote in current term
   // Volatile state on all servers:
   commitIndex int               // index of highest log entry known to be committed
   lastApplied int               // index of highest log entry applied to state machine
   // Volatile state on leaders:
   nextIndex   []int             // for each server, index of the next log entry to send
   matchIndex  []int             // for each server, index of highest log entry known to be
                                  // replicated on server
}
// {{{
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
   term = rf.currentTerm
   isleader = (rf.votedFor==rf.me)
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}
// }}}

//
// AppendEntrues RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct{
   Term           int               // leader`s term
   LeaderId       int               // follower can redirect clients
   PrevLogIndex   int               // index of log entry immediately preceding new ones
   PrevLogTerm    int               // term of prevLogIndex
   // Entries        *Persister        // log entries to store
   LeaderCommit   int               // leader`s commitIndex
}

//
// AppendEntries RPC reply structure
// fiesld names must start with capital letters!
//
type AppendEntriesReply struct{
   Term           int               // currentTerm, for leader to update itself
   Success        bool              // true if follower contained entry matching 
                                    // prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
   Term           int               // candidate`s term
   CandidateId    int               // candidate requesting vote
   LastLogIndex   int               // index of candidate`s last log entry
   LastLogTerm    int               // term of candidate`s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
   Term            int             // currentTerm, for candidate to update itself
   VoteGranted     bool            // true means candidate received vote
}

// AppendEnteries RPC handler

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
//   fmt.Printf("remote call server %d\n", rf.me)
   if args.Term < rf.currentTerm{
      reply.Success = false
      reply.Term = rf.currentTerm
      fmt.Printf("Term of server %d not match\n", rf.me)
//   } else if args.PrevLogIndex != rf.commitIndex{
//      reply.Success = false
//      reply.Term = args.Term
//      fmt.Printf("Log Index of server %d not match\n", rf.me)
   } else{
      reply.Success = true
      reply.Term = args.Term
      rf.commitIndex = args.LeaderCommit
//      fmt.Printf("server%d update log, new log index is %d\n", rf.me, rf.commitIndex)
   }
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
   // this raft node`s term is greater than the candidate
   if args.Term <= rf.currentTerm{
      reply.Term = rf.currentTerm
      reply.VoteGranted = false
   } else if rf.currentTerm < args.Term /*&& rf.commitIndex == args.LastLogIndex*/{
      fmt.Printf("server %d voted for server %d\n", rf.me, args.CandidateId)
      rf.votedFor = args.CandidateId
      rf.currentTerm = args.Term
      reply.Term = args.Term
      reply.VoteGranted = true
   } else{
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply){
   lastTerm := rf.currentTerm
   //totalMember := len(rf.peers)
   rand.Seed(int64(server))
   for ;; {
      // vote progress
      // set random range from 200ms-350ms
      // some confuse:
      // paper chose 150ms-300ms for a up to 150ms heartbeat, 
      // test allow more slow heartbeat from 100ms to another value;
      voteSpan := rand.Intn(150) + 200
      hasLeader := false
      fmt.Printf("server %d becomes candidate\n", rf.me)
      for i:= 0; i < voteSpan; i+=1{
         time.Sleep(time.Millisecond)
         if lastTerm != rf.currentTerm{
            hasLeader = true
            break;
         }
      }
      fmt.Printf("server %d has leader %v\n", rf.me, hasLeader)
      if !hasLeader && lastTerm == rf.currentTerm && rf.votedFor != rf.me{
         fmt.Printf("server %d apply for leader\n", rf.me)
         rf.currentTerm += 1
         args.Term = rf.currentTerm
         args.CandidateId = rf.me
         numberOfVote := 0
         availableServer := 0
         for i := 0; i < len(rf.peers); i++{
            if i == server{
               continue
            }
            if rf.peers[i].Call("Raft.RequestVote", args, reply){
               availableServer++
            }
            if reply.VoteGranted == false{
               // other raft`s term larger than this, this cannot be voted
               if reply.Term > rf.currentTerm{
                  rf.votedFor = -1
                  break
               }
            } else{
               numberOfVote++
            }
         }
         if numberOfVote >= availableServer/2 && availableServer != 0{
            rf.votedFor = rf.me
            fmt.Printf("server %d becomes the leader\n", rf.me)
         }
      }
      lastTerm = rf.currentTerm
      // append progress:
      lastCommit := rf.commitIndex
      fmt.Printf("server %d, preCommitIndex is %d\n", rf.me, lastCommit)
      if rf.votedFor != rf.me{
         for ;; {
            span := rand.Intn(150) + 200
            valid := false
            for i := 0; i < span; i+=1{
               time.Sleep(1 * time.Millisecond)
               if lastCommit != rf.commitIndex{
                  valid = true
                  break;
               }
            }
            if !valid{
               fmt.Printf("server%d no heartbeat!, preCommitIndex is %d\n", rf.me, lastCommit)
               break;
            }
            lastCommit = rf.commitIndex
         }
      } else{
         prevLogIndex := rf.commitIndex
         rf.commitIndex += 1
         prevLogTerm := rf.currentTerm-1
         appendArgs := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.commitIndex}
         appendReply := AppendEntriesReply{0, false}
         for ;;{
            time.Sleep(150 * time.Millisecond)
            for i:= 0; i < len(rf.peers); i++{
               if i == rf.me{
                  continue
               }
               rf.peers[i].Call("Raft.AppendEntries", &appendArgs, &appendReply)
               if rf.peers[rf.me] == nil || !appendReply.Success{
                  rf.votedFor = -1
                  fmt.Printf("invalid leader %d\n", rf.me)
                  break
               }
               appendReply.Success = false
            }
            if rf.votedFor == -1{
               break;
            }
            prevLogTerm = rf.currentTerm
            prevLogIndex = rf.commitIndex
            appendArgs.PrevLogIndex = rf.commitIndex
            rf.commitIndex += 1
            appendArgs.PrevLogTerm = prevLogTerm
            appendArgs.LeaderCommit = rf.commitIndex

         }
      }
   }
}

// {{{
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
//// }}}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
   rf.currentTerm = 0
   rf.votedFor = -1
   rf.commitIndex = 0
   rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
   // each term of a raft node has different time spans, 
   // that mean we need count random time down in func sendVoteRequest

   // initialize first requestVoteArgs and requestVoteReply structure
   args := &RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
   reply := &RequestVoteReply{rf.currentTerm, false}
   go rf.sendRequestVote(rf.me, args, reply)

	return rf
}

//After system has a leader, every server`s commitIndex should be same in a valid 
//timeout span. Otherwise, the first timeout server will start a new leader election.
//
//First we call a function with two state, one is leader election, the other is
//headtbeat(AppendEntries), after we have a leader, the leader will fill a shared mem
//with heartbeat signal periodically, other followers should listen to their location.
//Once there is no signal within timeout, the follower became a candidate and start
//a new election.
//
//psudocode:
//for ;;{
//    vote progress:
//    ...
//    append process:
//    if rf.votedFor != rf.me{
//        for ;;{ 
//            time.Sleep(timeout)
//            if shared_mem[rf.me] != true{
//                break
//        }
//    } else{
//        for ;;{
//            time.Sleep(fixed time){
//                for i:=0; i < len(rf.peers); i++{
//                    write signal into shared_mem
//                }
//            }
//        }
//    }
