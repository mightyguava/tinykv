package raftstore

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
	regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
	metaPeer := util.FindPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

type proposal struct {
	// index + term for unique identification
	index uint64
	term  uint64
	cb    *message.Callback
}

type peer struct {
	// The ticker of the peer, used to trigger
	// * raft tick
	// * raft log gc
	// * region heartbeat
	// * split check
	ticker *ticker
	// Instance of the Raft module
	RaftGroup *raft.RawNode
	// The peer storage for the Raft module
	peerStorage *PeerStorage

	// Record the meta information of the peer
	Meta     *metapb.Peer
	regionId uint64
	// Tag which is useful for printing log
	Tag string

	// Record the callback of the proposals
	// (Used in 2B)
	proposals []*proposal

	// Index of last scheduled compacted raft log.
	// (Used in 2C)
	LastCompactedIdx uint64

	// Cache the peers information from other stores
	// when sending raft messages to other peers, it's used to get the store id of target peer
	// (Used in 3B conf change)
	peerCache map[uint64]*metapb.Peer
	// Record the instants of peers being added into the configuration.
	// Remove them after they are not pending any more.
	// (Used in 3B conf change)
	PeersStartPendingTime map[uint64]time.Time
	// Mark the peer as stopped, set when peer is destroyed
	// (Used in 3B conf change)
	stopped bool

	// An inaccurate difference in region size since last reset.
	// split checker is triggered when it exceeds the threshold, it makes split checker not scan the data very often
	// (Used in 3B split)
	SizeDiffHint uint64
	// Approximate size of the region.
	// It's updated everytime the split checker scan the data
	// (Used in 3B split)
	ApproximateSize *uint64
}

func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
	meta *metapb.Peer) (*peer, error) {
	if meta.GetId() == util.InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())

	ps, err := NewPeerStorage(engines, region, regionSched, tag)
	if err != nil {
		return nil, err
	}

	appliedIndex := ps.AppliedIndex()

	raftCfg := &raft.Config{
		ID:            meta.GetId(),
		ElectionTick:  cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		Applied:       appliedIndex,
		Storage:       ps,
	}

	raftGroup, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	p := &peer{
		Meta:                  meta,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		peerCache:             make(map[uint64]*metapb.Peer),
		PeersStartPendingTime: make(map[uint64]time.Time),
		Tag:                   tag,
		ticker:                newTicker(region.GetId(), cfg),
	}

	// If this region has only one peer and I am the one, campaign directly.
	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err = p.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (p *peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

func (p *peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerID]; ok {
		return peer
	}
	for _, peer := range p.peerStorage.Region().GetPeers() {
		if peer.GetId() == peerID {
			p.insertPeerCache(peer)
			return peer
		}
	}
	return nil
}

func (p *peer) findProposal(term, index uint64) *proposal {
	for ; len(p.proposals) > 0; {
		prop := p.proposals[0]
		// Break if the proposal found is for a future index
		if prop.term > term || prop.term == term && prop.index > index {
			return nil
		}
		p.proposals = p.proposals[1:]
		if prop.term == term {
			if prop.index == index {
				return prop
			} else {
				// prop.index < index
				log.Panicf("%s unexpected proposal at term %d, found index %d, expected %d", p.Tag, prop.term, prop.index, index)
			}
		} else {
			log.Warnf("%s skipping stale proposal at term %d index %d", p.Tag, prop.term, prop.index)
			prop.cb.Done(ErrRespStaleCommand(term))
		}
	}
	return nil
}

func (p *peer) apply(ents []eraftpb.Entry) {
	for _, ent := range ents {
		if len(ent.Data) == 0 {
			// noop entry, skip
			continue
		}
		p.applyEntry(ent)
	}
}

// apply a single entry. All writes go through a single WriteBatch to ensure the apply is atomic.
func (p *peer) applyEntry(ent eraftpb.Entry) {
	proposal := p.findProposal(ent.Term, ent.Index)
	var cb *message.Callback
	if proposal != nil {
		// Proposal can be nil if this node was a follower when the proposal was made, or the node restarted.
		cb = proposal.cb
	}
	cmdReq := &raft_cmdpb.RaftCmdRequest{}
	if err := proto.Unmarshal(ent.Data, cmdReq); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	resp := newCmdResp()

	if len(cmdReq.Requests) == 0 {
		cb.Done(resp)
		return
	}

	kvWB := new(engine_util.WriteBatch)
	switch cmdReq.Requests[0].CmdType {
	case raft_cmdpb.CmdType_Snap:
		if len(cmdReq.Requests) > 1 {
			cb.Done(ErrResp(errors.New("expected snap request by itself")))
			log.Panic("expected snap request by itself")
			return
		}
		if cb != nil {
			cb.Txn = p.peerStorage.Engines.Kv.NewTransaction(false)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: p.Region()},
			})
		}
	case raft_cmdpb.CmdType_Get:
		for _, req := range cmdReq.Requests {
			if req.CmdType != raft_cmdpb.CmdType_Get {
				cb.Done(ErrResp(errors.New("only Get commands expected in read batch")))
				log.Panic("only Get commands expected in read batch")
				break
			}
			rr, err := p.handleRequest(req, nil)
			if err != nil {
				cb.Done(ErrResp(err))
				break
			}
			resp.Responses = append(resp.Responses, rr)
		}
	case raft_cmdpb.CmdType_Put, raft_cmdpb.CmdType_Delete:
		for _, req := range cmdReq.Requests {
			if req.CmdType != raft_cmdpb.CmdType_Put && req.CmdType != raft_cmdpb.CmdType_Delete {
				cb.Done(ErrResp(errors.New("only Put/Delete commands expected in write batch")))
				log.Panic("only Put/Delete commands expected in write batch")
				break
			}
			rr, err := p.handleRequest(req, kvWB)
			if err != nil {
				cb.Done(ErrResp(err))
				break
			}
			resp.Responses = append(resp.Responses, rr)
		}
	}

	p.peerStorage.SaveApplyResults(ent.Index, kvWB)

	cb.Done(resp)
}

func (p *peer) handleRequest(req *raft_cmdpb.Request, wb *engine_util.WriteBatch) (*raft_cmdpb.Response, error) {
	resp := &raft_cmdpb.Response{CmdType: req.CmdType}
	kv := p.peerStorage.Engines.Kv
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		v, err := engine_util.GetCF(kv, req.Get.Cf, req.Get.Key)
		if err != nil {
			return nil, err
		}
		resp.Get = &raft_cmdpb.GetResponse{Value: v}
	case raft_cmdpb.CmdType_Put:
		op := req.Put
		wb.SetCF(op.Cf, op.Key, op.Value)
		resp.Put = &raft_cmdpb.PutResponse{}
	case raft_cmdpb.CmdType_Delete:
		op := req.Delete
		wb.DeleteCF(op.Cf, op.Key)
		resp.Delete = &raft_cmdpb.DeleteResponse{}
	}
	return resp, nil
}

func (p *peer) nextProposalIndex() uint64 {
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

/// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
func (p *peer) MaybeDestroy() bool {
	if p.stopped {
		log.Infof("%v is being destroyed, skip", p.Tag)
		return false
	}
	return true
}

/// Does the real destroy worker.Task which includes:
/// 1. Set the region to tombstone;
/// 2. Clear data;
/// 3. Notify all pending requests.
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
	start := time.Now()
	region := p.Region()
	log.Infof("%v begin to destroy", p.Tag)

	// Set Tombstone state explicitly
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	if err := p.peerStorage.clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone)
	// write kv rocksdb first in case of restart happen between two write
	if err := kvWB.WriteToDB(engine.Kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(engine.Raft); err != nil {
		return err
	}

	if p.peerStorage.isInitialized() && !keepData {
		// If we meet panic when deleting data and raft log, the dirty data
		// will be cleared by a newer snapshot applying or restart.
		p.peerStorage.ClearData()
	}

	for _, proposal := range p.proposals {
		NotifyReqRegionRemoved(region.Id, proposal.cb)
	}
	p.proposals = nil

	log.Infof("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start))
	return nil
}

func (p *peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

func (p *peer) storeID() uint64 {
	return p.Meta.StoreId
}

func (p *peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

/// Set the region of a peer.
///
/// This will update the region of the peer, caller must ensure the region
/// has been preserved in a durable device.
func (p *peer) SetRegion(region *metapb.Region) {
	p.peerStorage.SetRegion(region)
}

func (p *peer) PeerId() uint64 {
	return p.Meta.GetId()
}

func (p *peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

func (p *peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
	for _, msg := range msgs {
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			log.Debugf("%v send message err: %v", p.Tag, err)
		}
	}
}

/// Collects all pending peers and update `peers_start_pending_time`.
func (p *peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	truncatedIdx := p.peerStorage.truncatedIndex()
	for id, progress := range p.RaftGroup.GetProgress() {
		if id == p.Meta.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.getPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				if _, ok := p.PeersStartPendingTime[id]; !ok {
					now := time.Now()
					p.PeersStartPendingTime[id] = now
					log.Debugf("%v peer %v start pending at %v", p.Tag, id, now)
				}
			}
		}
	}
	return pendingPeers
}

func (p *peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

/// Returns `true` if any new peer catches up with the leader in replicating logs.
/// And updates `PeersStartPendingTime` if needed.
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.peerStorage.truncatedIndex()
		progress, ok := p.RaftGroup.Raft.Prs[peerId]
		if ok {
			if progress.Match >= truncatedIdx {
				delete(p.PeersStartPendingTime, peerId)
				elapsed := time.Since(startPendingTime)
				log.Debugf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed)
				return true
			}
		}
	}
	return false
}

func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
	// The peer campaigned when it was created, no need to do it again.
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// If last peer is the leader of the region before split, it's intuitional for
	// it to become the leader of new split region.
	p.RaftGroup.Campaign()
	return true
}

func (p *peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(p.Region(), clonedRegion)
	if err != nil {
		return
	}
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}

func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Meta
	toPeer := p.getPeerFromCache(msg.To)
	if toPeer == nil {
		return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
	}
	log.Debugf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer, toPeer)

	sendMsg.FromPeer = &fromPeer
	sendMsg.ToPeer = toPeer

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or region split, but it's not
	//    created yet
	// For both cases the region start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending region split to perform
	// later.
	if p.peerStorage.isInitialized() && util.IsInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg
	return trans.Send(sendMsg)
}
