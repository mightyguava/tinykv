// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomized election timeout within [electionTimeout, 2*electionTimeout), reset every time the
	// node becomes a follower or candidate.
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	time int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	var peers []uint64
	if len(confState.Nodes) > 0 {
		peers = confState.Nodes
		if len(c.peers) > 0 {
			panic("config.peers cannot be set when restarting raft with existing state")
		}
	} else {
		peers = c.peers
	}
	prs := make(map[uint64]*Progress)
	for _, p := range peers {
		prs[p] = &Progress{}
	}
	r := &Raft{
		id:               c.ID,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
	}
	r.reset(0)
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	r.RaftLog.committed = hardState.Commit
	return r
}

// bcastAppend sends an append RPC to each peer
func (r *Raft) bcastAppend() {
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}
		r.sendAppend(pr)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	ents, err := r.RaftLog.entriesFrom(pr.Next)
	if err != nil {
		log.Panicf("failed to get entries to send: %v", err)
	}
	// LogTerm and Index sent are those of the entry immediately preceding this batch
	term := r.RaftLog.mustTerm(pr.Next - 1)
	r.send(pb.Message{To: to, Term: r.Term, LogTerm: term, Index: pr.Next - 1, Commit: r.RaftLog.committed, Entries: entriesToPtr(ents), MsgType: pb.MessageType_MsgAppend})
	return false
}

func (r *Raft) bcastHeartbeat() {
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}
		r.sendHeartbeat(pr)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	committed := min(r.RaftLog.committed, r.Prs[to].Match)
	r.send(pb.Message{To: to, Term: r.Term, Commit: committed, MsgType: pb.MessageType_MsgHeartbeat})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if r.Lead == r.id {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	log.Infof("%x becoming follower, new leader %x vote %x term %d", r.id, lead, r.Vote, term)
	r.reset(term)
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	log.Infof("%x becoming candidate", r.id)
	r.reset(r.Term)
	r.State = StateCandidate
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	log.Infof("%x becoming leader", r.id)
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	// Leader creates a noop entry when starting a new term
	r.leaderAppendEntries(&pb.Entry{})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term == 0 {
		// local message, proceed
	} else if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend {
			// Heartbeat or Append received from a leader with a higher term, so we should fall back to a follower.
			r.becomeFollower(m.Term, m.From)
		} else {
			// Otherwise, become a follower.
			r.becomeFollower(m.Term, None)
		}
	} else if m.Term < r.Term {
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			// Heartbeat received from leader with a lower term. Reply with a message to "disrupt" the leader to
			// become a follower.
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse})
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.send(pb.Message{To: m.From, Term: r.Term, Reject: true, MsgType: pb.MessageType_MsgRequestVoteResponse})
		}
		// Ignore this stale message
		return nil
	}
	var err error
	switch r.State {
	case StateFollower:
		err = r.stepFollower(m)
	case StateCandidate:
		err = r.stepCandidate(m)
	case StateLeader:
		err = r.stepLeader(m)
	}
	return err
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.Lead = m.From // Could be the first message after leader is elected
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.Lead = m.From // Could be the first message after leader is elected
		r.Term = m.Term
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.vote(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Reject {
			log.Infof("%x rejected vote", m.From)
		}
		r.votes[m.From] = !m.Reject
		if r.tallyVotes() == voteAccepted {
			r.becomeLeader()
			r.bcastAppend()
		} else if r.tallyVotes() == voteRejected {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgRequestVote:
		r.vote(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	pr := r.Prs[m.From]
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppendResponse:
		pr.Match = m.Index
		pr.Next = m.Index + 1
		if r.maybeCommit() {
			r.bcastAppend()
		} else if pr.Next < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgRequestVote:
		r.vote(m)
	case pb.MessageType_MsgPropose:
		r.leaderAppendEntries(m.Entries...)
		r.bcastAppend()
	}
	return nil
}

func (r *Raft) leaderAppendEntries(entries ...*pb.Entry) {
	index := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = index + uint64(i) + 1
	}
	r.RaftLog.append(entriesToValue(entries)...)
	r.Prs[r.id].Match = entries[len(entries)-1].Index
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.maybeCommit()
}

func (r *Raft) broadcast(msg pb.Message) {
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}
		msg.To = pr
		r.send(msg)
	}
}

func (r *Raft) send(msg pb.Message) {
	if msg.From == 0 {
		msg.From = r.id
	}
	if msg.Term == 0 {
		msg.Term = r.Term
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) startElection() {
	r.reset(r.Term)
	r.becomeCandidate()
	r.votes[r.id] = true
	r.Vote = r.id
	log.Infof("%x starting campaign for term %d", r.id, r.Term)
	logTerm := r.RaftLog.mustTerm(r.RaftLog.LastIndex())
	r.broadcast(pb.Message{Index: r.RaftLog.LastIndex(), LogTerm: logTerm, MsgType: pb.MessageType_MsgRequestVote})
	if r.tallyVotes() == voteAccepted {
		r.becomeLeader()
	}
}

func (r *Raft) vote(m pb.Message) {
	reject := false
	if m.Term < r.Term || // Reject if the message has a lower term ...
		m.Term == r.Term && (r.Vote != m.From && r.Vote != None || r.Lead != None) || // ... or if we already voted for a different candidate or already think there is a leader ...
		!candidateIsUpToDate(r.RaftLog, m.LogTerm, m.Index) { // ... or the candidate's log is not up to date
		reject = true
	} else {
		r.Term = m.Term
		r.Vote = m.From
	}
	r.send(pb.Message{To: m.From, Reject: reject, MsgType: pb.MessageType_MsgRequestVoteResponse})
}

func candidateIsUpToDate(l *RaftLog, term, index uint64) bool {
	lastTerm, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("error getting last term: %v", err)
	}
	return term > lastTerm || (term == lastTerm && index >= l.LastIndex())
}

func (r *Raft) reset(term uint64) {
	r.Lead = None
	r.Term = term
	r.State = StateFollower
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	for k := range r.votes {
		delete(r.votes, k)
	}
	r.Vote = None
	for pr, prg := range r.Prs {
		prg.Match = 0
		prg.Next = r.RaftLog.LastIndex() + 1
		if pr == r.id {
			prg.Match = r.RaftLog.LastIndex()
		}
	}
}

const (
	voteWaiting int = iota
	voteAccepted
	voteRejected
)

func (r *Raft) tallyVotes() int {
	required := len(r.Prs)/2 + 1
	count := 0
	for pr := range r.Prs {
		if r.votes[pr] {
			count++
		}
	}
	if count >= required {
		return voteAccepted
	}
	// If number of rejections exceeds required votes, reject
	if (len(r.votes) - count) >= required {
		return voteRejected
	}
	return voteWaiting
}

func (r *Raft) maybeCommit() bool {
	// Find the smallest index that has been replicated to a quorum of nodes.
	minReplicated := uint64(0)
	quorum := len(r.Prs)/2 + 1
	index := make([]uint64, 0, len(r.Prs)-1)
	for _, prg := range r.Prs {
		index = append(index, prg.Match)
	}
	sort.Slice(index, func(i, j int) bool {
		return index[i] < index[j]
	})
	minReplicated = index[len(index)-quorum]
	term, err := r.RaftLog.Term(minReplicated)
	if err != nil {
		return false
	}
	if term < r.Term {
		// Leader does not try to commit entries from previous terms. Once an entry from the current term is committed,
		// entries from previous terms are indirectly committed. §5.4.2
		return false
	}
	if minReplicated > r.RaftLog.committed {
		r.RaftLog.commit(minReplicated)
		return true
	}
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	r.electionElapsed = 0
	last, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, entriesToValue(m.Entries)...)
	if !ok {
		r.send(pb.Message{To: m.From, Reject: true, MsgType: pb.MessageType_MsgAppendResponse})
	} else {
		r.send(pb.Message{To: m.From, Index: last, MsgType: pb.MessageType_MsgAppendResponse})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.electionElapsed = 0
	r.RaftLog.commit(m.Commit)
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func entriesToPtr(r []pb.Entry) []*pb.Entry {
	ptr := make([]*pb.Entry, len(r))
	for i := range r {
		ptr[i] = &r[i]
	}
	return ptr
}

func entriesToValue(ptr []*pb.Entry) []pb.Entry {
	r := make([]pb.Entry, len(ptr))
	for i, e := range ptr {
		r[i] = *e
	}
	return r
}
