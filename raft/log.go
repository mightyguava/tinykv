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

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact. This includes both stable and
	// unstable entries.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// the first index after the snapshot
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	ents, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:    storage,
		firstIndex: firstIndex,
		applied:    firstIndex - 1,
		committed:  firstIndex - 1,
		stabled:    lastIndex,
		entries:    ents,
	}
}

// append entries into the log, skipping entries that already exist (term and index match). If there is a term mismatch,
// all existing entries after the mismatch, inclusive, are discarded, and replaced with the new entries.
func (l *RaftLog) append(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) maybeAppend(prevLogIndex, prevLogTerm, leaderCommit uint64, entries ...pb.Entry) (lastIndex uint64, accepted bool) {
	// Reject if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if term, err := l.Term(prevLogIndex); err != nil || term != prevLogTerm {
		return 0, false
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)

	// Find index of the first conflict, or if there are no conflicts
	conflictingIndex := l.findFirstConflict(entries...)
	if conflictingIndex == 0 {
		// all entries are already in the log, do nothing
	} else if conflictingIndex <= l.committed {
		log.Panicf("entry %d conflict with committed entry [committed(%d)]", conflictingIndex, l.committed)
	} else {
		var err error
		if conflictingIndex <= l.LastIndex() {
			// there is a conflict, truncate
			l.entries, err = l.slice(l.firstIndex, conflictingIndex)
			l.stabled = conflictingIndex - 1
		}
		if err != nil {
			log.Panicf("error getting entries: %v", err)
		}
		// append the new entries
		l.append(entries[conflictingIndex-entries[0].Index:]...)
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if l.committed < leaderCommit {
		lastNewIndex := prevLogIndex + uint64(len(entries))
		l.commit(min(leaderCommit, lastNewIndex))
	}
	return l.LastIndex(), true
}

func (l *RaftLog) findFirstConflict(entries ...pb.Entry) uint64 {
	for i := 0; i < len(entries); i++ {
		term, err := l.Term(entries[i].Index)
		if err != nil || term != entries[i].Term {
			if entries[i].Index <= l.LastIndex() {
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]", entries[i].Index, term, entries[i].Term)
			}
			return entries[i].Index
		}
	}
	return 0
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	numUnstable := l.LastIndex() - l.stabled
	total := len(l.entries)
	ents := l.entries[total-int(numUnstable) : total]
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	ents, err := l.slice(max(l.applied+1, l.firstIndex), l.committed+1)
	if err != nil {
		log.Panicf("error fetching next entries, firstIndex %d applied %d committed %d: %v",
			l.firstIndex, l.applied, l.committed, err)
	}
	return ents
}

func (l *RaftLog) getEntry(idx uint64) (pb.Entry, error) {
	ents, err := l.slice(idx, idx+1)
	if len(ents) == 0 {
		return pb.Entry{}, errors.New("entry not found")
	}
	return ents[0], err
}

func (l *RaftLog) entriesFrom(lo uint64) ([]pb.Entry, error) {
	li := l.LastIndex()
	if lo > li {
		return nil, nil
	}
	return l.slice(lo, li+1)
}

// slice returns a slice of the array reading from both stable and unstable logs, from lo to hi-1, inclusive
func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if hi > l.LastIndex()+1 || lo < l.firstIndex {
		return nil, errors.New("out of bounds")
	}
	if lo == hi {
		return nil, nil
	}
	return l.entries[lo-l.entries[0].Index : hi-l.entries[0].Index], nil
}

func (l *RaftLog) commit(index uint64) {
	// committed cannot decrease
	if l.committed < index {
		l.committed = index
	}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	idx, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return idx
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i < l.firstIndex-1 {
		return 0, errors.New("invalid index for term")
	}
	if i == l.firstIndex-1 {
		return 0, nil
	}
	ent, err := l.slice(i, i+1)
	if err != nil {
		return 0, err
	}
	return ent[0].Term, nil
}

func (l *RaftLog) mustTerm(i uint64) uint64 {
	term, err := l.Term(i)
	if err != nil {
		log.Panicf("error getting term for index %d: %v", i, err)
	}
	return term
}
