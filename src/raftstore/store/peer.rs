// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;
use std::default::Default;

use rocksdb::{DB, WriteBatch, Writable};
use rocksdb::rocksdb::Snapshot;
use protobuf::{self, Message};
use uuid::Uuid;

use kvproto::metapb;
use kvproto::raftpb::{self, ConfChangeType};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, ChangePeerRequest, CmdType,
                          AdminCmdType, Request, Response, AdminRequest, AdminResponse,
                          TransferLeaderRequest, TransferLeaderResponse};
use kvproto::raft_serverpb::{RaftMessage, RaftTruncatedState};
use raft::{self, RawNode, StateRole};
use raftstore::{Result, Error};
use raftstore::coprocessor::CoprocessorHost;
use raftstore::coprocessor::split_observer::SplitObserver;
use util::{escape, HandyRwLock};
use pd::PdClient;
use super::store::Store;
use super::peer_storage::{self, PeerStorage, RaftStorage};
use super::util;
use super::msg::Callback;
use super::cmd_resp;
use super::transport::Transport;
use super::keys;
use super::engine::{Peekable, Iterable, Mutable};

pub struct PendingCmd {
    pub uuid: Uuid,
    pub cb: Callback,
}

#[derive(Debug)]
pub enum ExecResult {
    ChangePeer {
        change_type: ConfChangeType,
        peer: metapb::Peer,
        region: metapb::Region,
    },
    CompactLog {
        state: RaftTruncatedState,
    },
    SplitRegion {
        left: metapb::Region,
        right: metapb::Region,
    },
}

// When we apply commands in handing ready, we should also need a way to
// let outer store do something after handing ready over.
// We can save these intermediate results in ready result.
// We only need to care administration commands now.
pub struct ReadyResult {
    // We can execute multi commands like 1, conf change, 2 split region, ...
    // in one ready, and outer store should handle these results sequentially too.
    pub exec_results: Vec<ExecResult>,
    // snap_applied_region should be set when we apply snapshot
    pub snap_applied_region: Option<metapb::Region>,
}

#[derive(Default)]
struct PendingCmdQueue {
    normals: VecDeque<PendingCmd>,
    conf_change: Option<PendingCmd>,
    uuids: HashSet<Uuid>,
}

impl PendingCmdQueue {
    pub fn contains(&self, uuid: &Uuid) -> bool {
        self.uuids.contains(uuid)
    }

    fn remove(&mut self, cmd: &Option<PendingCmd>) {
        if let Some(ref cmd) = *cmd {
            self.uuids.remove(&cmd.uuid);
        }
    }

    fn pop_normal(&mut self) -> Option<PendingCmd> {
        let cmd = self.normals.pop_front();
        self.remove(&cmd);
        cmd
    }

    fn append_normal(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd> {
        let cmd = self.conf_change.take();
        self.remove(&cmd);
        cmd
    }

    fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.conf_change = Some(cmd);
    }
}

pub struct Peer {
    engine: Arc<DB>,
    pub peer: metapb::Peer,
    region_id: u64,
    pub raft_group: RawNode<RaftStorage>,
    pub storage: Arc<RaftStorage>,
    pending_cmds: PendingCmdQueue,
    peer_cache: Arc<RwLock<HashMap<u64, metapb::Peer>>>,
    coprocessor_host: CoprocessorHost,
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: u64,
    // if we remove ourself in ChangePeer remove, we should set this flag, then
    // any following committed logs in same Ready should be applied failed.
    penging_remove: bool,
}

impl Peer {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create<T: Transport, C: PdClient>(store: &mut Store<T, C>,
                                             region: &metapb::Region)
                                             -> Result<Peer> {
        let store_id = store.store_id();
        let peer_id = match util::find_peer(region, store_id) {
            None => {
                return Err(box_err!("find no peer for store {} in region {:?}", store_id, region))
            }
            Some(peer) => peer.get_id(),
        };

        Peer::new(store, region, peer_id)
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after appling snapshot.
    pub fn replicate<T: Transport, C: PdClient>(store: &mut Store<T, C>,
                                                region_id: u64,
                                                from_epoch: &metapb::RegionEpoch,
                                                peer_id: u64)
                                                -> Result<Peer> {
        let tombstone_key = &keys::region_tombstone_key(region_id);
        if let Some(region) = try!(store.engine().get_msg::<metapb::Region>(tombstone_key)) {
            let region_epoch = region.get_region_epoch();
            // The region in this peer already destroyed
            if !(from_epoch.get_version() >= region_epoch.get_version() &&
                 from_epoch.get_conf_ver() > region_epoch.get_conf_ver()) {
                error!("from epoch {:?}, tombstone epoch {:?}",
                       from_epoch,
                       region_epoch);
                // We receive a stale message and we can't re-create the peer with the peer id.
                return Err(box_err!("peer {} already destroyed", peer_id));
            }
        }

        // We will remove tombstone key when apply snapshot
        info!("replicate peer, peer id {}, region_id {}",
              peer_id,
              region_id);

        let mut region = metapb::Region::new();
        region.set_id(region_id);
        Peer::new(store, &region, peer_id)
    }

    fn new<T: Transport, C: PdClient>(store: &mut Store<T, C>,
                                      region: &metapb::Region,
                                      peer_id: u64)
                                      -> Result<Peer> {
        if peer_id == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let cfg = store.config();

        let store_id = store.store_id();
        let ps = try!(PeerStorage::new(store.engine(), &region, cfg.snap_dir.clone()));
        let applied_index = ps.applied_index();
        let storage = Arc::new(RaftStorage::new(ps));

        let raft_cfg = raft::Config {
            id: peer_id,
            peers: vec![],
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            tag: format!("[region {}]", region.get_id()),
        };

        let raft_group = try!(RawNode::new(&raft_cfg, storage.clone(), &[]));

        let mut peer = Peer {
            engine: store.engine(),
            peer: util::new_peer(store_id, peer_id),
            region_id: region.get_id(),
            storage: storage,
            raft_group: raft_group,
            pending_cmds: Default::default(),
            peer_cache: store.peer_cache(),
            coprocessor_host: CoprocessorHost::new(),
            size_diff_hint: 0,
            penging_remove: false,
        };

        peer.load_all_coprocessors();

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            try!(peer.raft_group.campaign());
        }

        Ok(peer)
    }

    pub fn destroy(&mut self) -> Result<()> {
        // TODO maybe very slow
        // Delete all data in this peer.
        let wb = WriteBatch::new();
        try!(self.storage.wl().scan_region(self.engine.as_ref(),
                                           &mut |key, _| {
                                               try!(wb.delete(key));
                                               Ok(true)
                                           }));

        try!(wb.put_msg(&keys::region_tombstone_key(self.region_id), &self.region()));
        try!(self.engine.write(wb));

        self.coprocessor_host.shutdown();

        Ok(())
    }

    pub fn load_all_coprocessors(&mut self) {
        // TODO load coprocessors from configuation
        self.coprocessor_host.registry.register_observer(100, box SplitObserver);
    }

    pub fn update_region(&mut self, region: &metapb::Region) -> Result<()> {
        if self.region_id != region.get_id() {
            return Err(box_err!("invalid region id {} != {}",
                                region.get_id(),
                                self.region_id));
        }

        self.storage.wl().set_region(region);
        Ok(())
    }

    pub fn region(&self) -> metapb::Region {
        self.storage.rl().get_region().clone()
    }

    pub fn peer_id(&self) -> u64 {
        self.peer.get_id()
    }

    pub fn get_raft_status(&self) -> raft::Status {
        self.raft_group.status()
    }

    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    pub fn handle_raft_ready<T: Transport>(&mut self,
                                           trans: &Arc<RwLock<T>>)
                                           -> Result<Option<ReadyResult>> {
        if !self.raft_group.has_ready() {
            return Ok(None);
        }

        debug!("handle raft ready: peer {:?}, region {}",
               self.peer,
               self.region_id);

        let mut ready = self.raft_group.ready();

        let applied_region = try!(self.storage.wl().handle_raft_ready(&ready));

        for msg in &ready.messages {
            try!(self.send_raft_message(&msg, trans));
        }

        let allow_apply = self.storage.rl().snap_state.allow_apply_committed();
        let exec_results = if allow_apply {
            try!(self.handle_raft_commit_entries(&ready.committed_entries))
        } else {
            if let Some(ref mut hs) = ready.hs {
                // Don't apply here.
                hs.set_commit(0);
            }
            vec![]
        };

        self.raft_group.advance(ready);
        Ok(Some(ReadyResult {
            snap_applied_region: applied_region,
            exec_results: exec_results,
        }))
    }

    pub fn propose(&mut self,
                   cmd: PendingCmd,
                   req: RaftCmdRequest,
                   mut err_resp: RaftCmdResponse)
                   -> Result<()> {
        if self.pending_cmds.contains(&cmd.uuid) {
            cmd_resp::bind_error(&mut err_resp, box_err!("duplicated uuid {:?}", cmd.uuid));
            return cmd.cb.call_box((err_resp,));
        }

        debug!("propose command with uuid {:?}", cmd.uuid);
        metric_incr!("raftstore.propose");

        if let Err(e) = self.check_epoch(&req) {
            cmd_resp::bind_error(&mut err_resp, e);
            return cmd.cb.call_box((err_resp,));
        }

        if get_transfer_leader_cmd(&req).is_some() {
            let resp = self.transfer_leader(req);

            // transfer leader command doesn't need to replicate log and apply, so we
            // return immediately. Note that this command may fail, we can view it just as an advice
            return cmd.cb.call_box((resp,));
        } else if get_change_peer_cmd(&req).is_some() {
            if self.raft_group.raft.pending_conf {
                return Err(box_err!("there is a pending conf change, try later."));
            }
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                // if it loses leader ship before confchange is replicated, there may be
                // a stale pending conf change before next conf change is applied. If it
                // becomes leader again with the stale pending conf change, will enter
                // this block, so we notify leadership may have changed.
                self.notify_not_leader(cmd);
            }

            if let Err(e) = self.propose_conf_change(req) {
                cmd_resp::bind_error(&mut err_resp, e);
                return cmd.cb.call_box((err_resp,));
            }

            self.pending_cmds.set_conf_change(cmd);
        } else if let Err(e) = self.propose_normal(req) {
            cmd_resp::bind_error(&mut err_resp, e);
            return cmd.cb.call_box((err_resp,));
        } else {
            self.pending_cmds.append_normal(cmd);
        }

        metric_incr!("raftstore.propose.success");
        Ok(())
    }

    /// Call the callback of `cmd` that leadership may have been changed.
    ///
    /// Please note that, `NotLeader` here doesn't mean that currently this
    /// peer is not leader.
    fn notify_not_leader(&self, cmd: PendingCmd) {
        let leader = self.get_peer_from_cache(self.leader_id());
        let not_leader = Error::NotLeader(self.region_id, leader);
        let resp = cmd_resp::err_resp(not_leader, cmd.uuid, self.term());
        if let Err(e) = cmd.cb.call_box((resp,)) {
            error!("failed to clean stale callback of {}: {:?}", cmd.uuid, e);
        }
    }

    fn propose_normal(&mut self, mut cmd: RaftCmdRequest) -> Result<()> {
        // TODO: validate request for unexpected changes.
        try!(self.coprocessor_host.pre_propose(&self.storage.rl(), &mut cmd));
        let data = try!(cmd.write_to_bytes());
        try!(self.raft_group.propose(data));
        Ok(())
    }

    fn transfer_leader(&mut self, cmd: RaftCmdRequest) -> RaftCmdResponse {
        let transfer_leader = get_transfer_leader_cmd(&cmd).unwrap();
        let peer = transfer_leader.get_peer();

        metric_incr!("raftstore.transfer_leader");

        info!("transfer leader from {:?} to {:?} at region {}",
              self.peer,
              peer,
              self.region_id,
        );

        self.raft_group.transfer_leader(peer.get_id());

        let mut response = AdminResponse::new();
        response.set_cmd_type(AdminCmdType::TransferLeader);
        response.set_transfer_leader(TransferLeaderResponse::new());
        let mut resp = RaftCmdResponse::new();
        resp.set_admin_response(response);
        resp
    }

    fn propose_conf_change(&mut self, cmd: RaftCmdRequest) -> Result<()> {
        metric_incr!("raftstore.propose.conf_change");
        let data = try!(cmd.write_to_bytes());
        let change_peer = get_change_peer_cmd(&cmd).unwrap();

        let mut cc = raftpb::ConfChange::new();
        cc.set_change_type(change_peer.get_change_type());
        cc.set_node_id(change_peer.get_peer().get_id());
        cc.set_context(data);

        info!("propose conf change {:?} peer {:?} at region {}",
              cc.get_change_type(),
              cc.get_node_id(),
              self.region_id);

        self.raft_group.propose_conf_change(cc).map_err(From::from)
    }

    fn check_epoch(&self, req: &RaftCmdRequest) -> Result<()> {
        let (mut check_ver, mut check_conf_ver) = (false, false);
        if req.has_admin_request() {
            match req.get_admin_request().get_cmd_type() {
                AdminCmdType::CompactLog |
                AdminCmdType::InvalidAdmin => {}
                AdminCmdType::Split => check_ver = true,
                AdminCmdType::ChangePeer => check_conf_ver = true,
                AdminCmdType::TransferLeader => {
                    check_ver = true;
                    check_conf_ver = true;
                }
            };
        } else {
            // for get/set/seek/delete, we don't care conf_version.
            check_ver = true;
        }

        if !check_ver && !check_conf_ver {
            return Ok(());
        }

        if !req.get_header().has_region_epoch() {
            return Err(box_err!("missing epoch!"));
        }

        let from_epoch = req.get_header().get_region_epoch();
        let latest_region = self.region();
        let latest_epoch = latest_region.get_region_epoch();

        // should we use not equal here?
        if (check_conf_ver && from_epoch.get_conf_ver() < latest_epoch.get_conf_ver()) ||
           (check_ver && from_epoch.get_version() < latest_epoch.get_version()) {
            debug!("received stale epoch {:?}, mime: {:?}",
                   from_epoch,
                   latest_epoch);
            return Err(Error::StaleEpoch(format!("latest_epoch of region {} is {:?}, but you \
                                                  sent {:?}",
                                                 self.region_id,
                                                 latest_epoch,
                                                 from_epoch)));
        }

        Ok(())
    }

    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        if let Some(peer) = self.peer_cache.rl().get(&peer_id).cloned() {
            return Some(peer);
        }

        // Try to find in region, if found, set in cache.
        for peer in self.storage.rl().get_region().get_peers() {
            if peer.get_id() == peer_id {
                self.peer_cache.wl().insert(peer_id, peer.clone());
                return Some(peer.clone());
            }
        }

        None
    }

    fn send_raft_message<T: Transport>(&mut self,
                                       msg: &raftpb::Message,
                                       trans: &Arc<RwLock<T>>)
                                       -> Result<()> {
        metric_incr!("raftstore.send_raft_message");
        let mut send_msg = RaftMessage::new();
        send_msg.set_region_id(self.region_id);
        send_msg.set_message(msg.clone());
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
        let mut unreachable = false;

        let from_peer = match self.get_peer_from_cache(msg.get_from()) {
            Some(p) => p,
            None => {
                return Err(box_err!("failed to lookup sender peer {} in region {}",
                                    msg.get_from(),
                                    self.region_id))
            }
        };

        let to_peer = match self.get_peer_from_cache(msg.get_to()) {
            Some(p) => p,
            None => {
                return Err(box_err!("failed to look up recipient peer {} in region {}",
                                    msg.get_to(),
                                    self.region_id))
            }
        };

        let to_peer_id = to_peer.get_id();
        let to_store_id = to_peer.get_store_id();

        debug!("send raft msg {:?}[size: {}] from {} to {}",
               msg.get_msg_type(),
               msg.compute_size(),
               from_peer.get_id(),
               to_peer_id);

        send_msg.set_from_peer(from_peer);
        send_msg.set_to_peer(to_peer);

        if let Err(e) = trans.rl().send(send_msg) {
            warn!("region {} with peer {:?} failed to send msg to {} in store {}, err: {:?}",
                  self.region_id,
                  self.peer,
                  to_peer_id,
                  to_store_id,
                  e);

            unreachable = true;
        }

        if unreachable {
            self.raft_group.report_unreachable(to_peer_id);
        }

        Ok(())
    }

    fn handle_raft_commit_entries(&mut self,
                                  committed_entries: &[raftpb::Entry])
                                  -> Result<Vec<ExecResult>> {
        metric_incr!("raftstore.handle_raft_commit_entries");
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let mut results = vec![];
        for entry in committed_entries {
            let res = try!(match entry.get_entry_type() {
                raftpb::EntryType::EntryNormal => self.handle_raft_entry_normal(entry),
                raftpb::EntryType::EntryConfChange => self.handle_raft_entry_conf_change(entry),
            });

            if let Some(res) = res {
                results.push(res);
            }
        }

        Ok(results)
    }

    fn handle_raft_entry_normal(&mut self, entry: &raftpb::Entry) -> Result<Option<ExecResult>> {
        let index = entry.get_index();
        let data = entry.get_data();

        if data.is_empty() {
            // when a peer become leader, it will send an empty entry.
            return Ok(None);
        }

        let cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(data));
        // no need to return error here.
        self.process_raft_cmd(index, cmd).or_else(|e| {
            error!("process raft command at index {} err: {:?}", index, e);
            Ok(None)
        })
    }

    fn handle_raft_entry_conf_change(&mut self,
                                     entry: &raftpb::Entry)
                                     -> Result<Option<ExecResult>> {

        let index = entry.get_index();
        let mut conf_change =
            try!(protobuf::parse_from_bytes::<raftpb::ConfChange>(entry.get_data()));
        let cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(conf_change.get_context()));
        let res = match self.process_raft_cmd(index, cmd) {
            a @ Ok(Some(_)) => a,
            e => {
                error!("process raft command at index {} err: {:?}", index, e);
                // If failed, tell raft that the config change was aborted.
                conf_change = raftpb::ConfChange::new();
                Ok(None)
            }
        };

        self.raft_group.apply_conf_change(conf_change);
        metric_incr!("raftstore.handle_raft_entry_conf_change");

        res
    }

    fn find_cb(&mut self, uuid: Uuid, cmd: &RaftCmdRequest) -> Option<Callback> {
        if get_change_peer_cmd(cmd).is_some() {
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                if cmd.uuid == uuid {
                    return Some(cmd.cb);
                } else {
                    self.notify_not_leader(cmd);
                }
            }
            return None;
        }
        while let Some(head) = self.pending_cmds.pop_normal() {
            if head.uuid == uuid {
                return Some(head.cb);
            }
            // because of the lack of original RaftCmdRequest, we skip calling
            // coprocessor here.
            // TODO: call coprocessor with uuid instead.
            self.notify_not_leader(head);
        }
        None
    }

    fn process_raft_cmd(&mut self, index: u64, cmd: RaftCmdRequest) -> Result<Option<ExecResult>> {
        if index == 0 {
            return Err(box_err!("processing raft command needs a none zero index"));
        }

        let uuid = util::get_uuid_from_req(&cmd).unwrap();

        let cb = self.find_cb(uuid, &cmd);

        let (mut resp, exec_result) = self.apply_raft_cmd(index, &cmd).unwrap_or_else(|e| {
            error!("apply raft command err {:?}", e);
            (cmd_resp::new_error(e), None)
        });

        debug!("command with uuid {:?} is applied", uuid);

        if cb.is_none() {
            return Ok(exec_result);
        }

        let cb = cb.unwrap();
        self.coprocessor_host.post_apply(&self.storage.rl(), &cmd, &mut resp);
        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        // Bind uuid here.
        cmd_resp::bind_uuid(&mut resp, uuid);
        cmd_resp::bind_term(&mut resp, self.term());
        if let Err(e) = cb.call_box((resp,)) {
            error!("callback err {:?}", e);
        }

        Ok(exec_result)
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    fn apply_raft_cmd(&mut self,
                      index: u64,
                      req: &RaftCmdRequest)
                      -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        if self.penging_remove {
            return Err(box_err!("handle removing peer {} before, ignore apply",
                                self.peer.get_id()));
        }

        let last_applied_index = self.storage.rl().applied_index();
        if last_applied_index >= index {
            return Err(box_err!("applied index moved backwards, {} >= {}",
                                last_applied_index,
                                index));
        }

        let wb = WriteBatch::new();
        let (mut resp, exec_result) = {
            let engine = self.engine.clone();
            let ctx = ExecContext {
                snap: engine.snapshot(),
                wb: &wb,
                req: req,
            };

            self.exec_raft_cmd(&ctx).unwrap_or_else(|e| {
                error!("execute raft command err: {:?}", e);
                (cmd_resp::new_error(e), None)
            })
        };

        peer_storage::save_applied_index(&wb, self.region_id, index)
            .expect("save applied index must not fail");

        // Commit write and change storage fields atomically.
        // Lock here to guarantee generating snapshot sees a consistent view data.
        let mut storage = self.storage.wl();
        match self.engine
            .write(wb) {
            Ok(_) => {
                storage.set_applied_index(index);

                if let Some(ref exec_result) = exec_result {
                    match *exec_result {
                        ExecResult::ChangePeer { ref region, .. } => {
                            storage.set_region(region);
                        }
                        ExecResult::CompactLog { ref state } => {
                            storage.set_truncated_state(state);
                            // TODO: we can set exec_result to None, because outer store
                            // doesn't need it.
                        }
                        ExecResult::SplitRegion { ref left, .. } => {
                            storage.set_region(left);
                        }
                    }
                };
            }
            Err(e) => {
                error!("commit batch failed err {:?}", e);
                resp = cmd_resp::message_error(e);
            }
        };

        Ok((resp, exec_result))
    }
}

fn get_transfer_leader_cmd(msg: &RaftCmdRequest) -> Option<&TransferLeaderRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_transfer_leader() {
        return None;
    }

    Some(req.get_transfer_leader())
}

fn get_change_peer_cmd(msg: &RaftCmdRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}

struct ExecContext<'a> {
    pub snap: Snapshot<'a>,
    pub wb: &'a WriteBatch,
    pub req: &'a RaftCmdRequest,
}

// Here we implement all commands.
impl Peer {
    fn exec_raft_cmd(&mut self,
                     ctx: &ExecContext)
                     -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        try!(self.check_epoch(&ctx.req));
        if ctx.req.has_admin_request() {
            self.exec_admin_cmd(ctx)
        } else {
            // Now we don't care write command outer, so use None.
            self.exec_write_cmd(ctx).and_then(|v| Ok((v, None)))
        }
    }

    fn exec_admin_cmd(&mut self,
                      ctx: &ExecContext)
                      -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        let request = ctx.req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        info!("execute admin command {:?} at region {:?}",
              request,
              self.region());

        let (mut response, exec_result) = try!(match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::CompactLog => self.exec_compact_log(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            AdminCmdType::InvalidAdmin => Err(box_err!("unsupported admin command type")),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_change_peer(&mut self,
                        ctx: &ExecContext,
                        request: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut region = self.region();

        warn!("my peer id {}, {}, {:?}, epoch: {:?}",
              self.peer_id(),
              peer.get_id(),
              util::conf_change_type_str(&change_type),
              region.get_region_epoch());

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let exists = util::find_peer(&region, store_id).is_some();
        let conf_ver = region.get_region_epoch().get_conf_ver() + 1;

        region.mut_region_epoch().set_conf_ver(conf_ver);

        match change_type {
            raftpb::ConfChangeType::AddNode => {
                metric_incr!("raftstore.add_peer");
                if exists {
                    error!("my peer id {}, can't add duplicated peer {:?} to store {}, region \
                            {:?}",
                           self.peer_id(),
                           peer,
                           store_id,
                           region);
                    return Err(box_err!("can't add duplicated peer {:?} to store {}",
                                        peer,
                                        store_id));
                }
                // TODO: Do we allow adding peer in same node?

                // Add this peer to cache.
                self.peer_cache.wl().insert(peer.get_id(), peer.clone());
                region.mut_peers().push(peer.clone());

                metric_incr!("raftstore.add_peer.success");

                warn!("my peer id {}, add peer {:?}, region {:?}",
                      self.peer_id(),
                      peer,
                      self.region());
            }
            raftpb::ConfChangeType::RemoveNode => {
                metric_incr!("raftstore.remove_peer");
                if !exists {
                    error!("remove missing peer {:?} from store {}", peer, store_id);
                    return Err(box_err!("remove missing peer {:?} from store {}", peer, store_id));
                }

                if self.peer_id() == peer.get_id() {
                    // Remove ourself, we will destroy all region data later.
                    // So we need not to apply following logs.
                    self.penging_remove = true;
                }

                // Remove this peer from cache.
                self.peer_cache.wl().remove(&peer.get_id());
                util::remove_peer(&mut region, store_id).unwrap();

                metric_incr!("raftstore.remove_peer.success");
                warn!("my peer_id {}, remove {}, region:{:?}",
                      self.peer_id(),
                      peer.get_id(),
                      self.region());
            }
        }

        try!(ctx.wb.put_msg(&keys::region_info_key(region.get_id()), &region));

        let mut resp = AdminResponse::new();
        resp.mut_change_peer().set_region(region.clone());

        Ok((resp,
            Some(ExecResult::ChangePeer {
            change_type: change_type,
            peer: peer.clone(),
            region: region,
        })))
    }

    fn exec_split(&mut self,
                  ctx: &ExecContext,
                  req: &AdminRequest)
                  -> Result<(AdminResponse, Option<ExecResult>)> {
        metric_incr!("raftstore.split");
        let split_req = req.get_split();
        if !split_req.has_split_key() {
            return Err(box_err!("missing split key"));
        }

        let split_key = split_req.get_split_key();
        let mut region = self.region();
        if split_key <= region.get_start_key() {
            return Err(box_err!("invalid split request: {:?}", split_req));
        }

        try!(util::check_key_in_region(split_key, &region));

        info!("split at key: {}, region: {:?}", escape(split_key), region);

        // TODO: check new region id validation.
        let new_region_id = split_req.get_new_region_id();

        // After split, the origin region key range is [start_key, split_key),
        // the new split region is [split_key, end).
        let mut new_region = region.clone();
        region.set_end_key(split_key.to_vec());

        new_region.set_start_key(split_key.to_vec());
        new_region.set_id(new_region_id);

        // Update new region peer ids.
        let new_peer_ids = split_req.get_new_peer_ids();
        if new_peer_ids.len() != new_region.get_peers().len() {
            return Err(box_err!("invalid new peer id count, need {}, but got {}",
                                new_region.get_peers().len(),
                                new_peer_ids.len()));
        }

        for (index, peer) in new_region.mut_peers().iter_mut().enumerate() {
            let peer_id = new_peer_ids[index];
            peer.set_id(peer_id);

            // Add this peer to cache.
            self.peer_cache.wl().insert(peer_id, peer.clone());
        }

        // update region version
        let region_ver = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_ver);
        new_region.mut_region_epoch().set_version(region_ver);
        try!(ctx.wb.put_msg(&keys::region_info_key(region.get_id()), &region));
        try!(ctx.wb.put_msg(&keys::region_info_key(new_region.get_id()), &new_region));

        let mut resp = AdminResponse::new();
        resp.mut_split().set_left(region.clone());
        resp.mut_split().set_right(new_region.clone());

        self.size_diff_hint = 0;

        Ok((resp,
            Some(ExecResult::SplitRegion {
            left: region,
            right: new_region,
        })))
    }

    fn exec_compact_log(&mut self,
                        ctx: &ExecContext,
                        req: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        metric_incr!("raftstore.compact");
        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::new();

        let first_index = self.storage.rl().first_index();
        if compact_index <= first_index {
            debug!("compact index {} <= first index {}, no need to compact",
                   compact_index,
                   first_index);
            return Ok((resp, None));
        }

        let state = try!(self.storage.rl().compact(ctx.wb, compact_index));
        Ok((resp, Some(ExecResult::CompactLog { state: state })))
    }

    fn exec_write_cmd(&mut self, ctx: &ExecContext) -> Result<RaftCmdResponse> {
        let requests = ctx.req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = try!(match cmd_type {
                CmdType::Get => self.do_get(ctx, req),
                CmdType::Seek => self.do_seek(ctx, req),
                CmdType::Put => self.do_put(ctx, req),
                CmdType::Delete => self.do_delete(ctx, req),
                CmdType::Snap => self.do_snap(ctx, req),
                CmdType::Invalid => Err(box_err!("invalid cmd type, message maybe currupted.")),
            });

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        Ok(resp)
    }

    fn check_data_key(&self, key: &[u8]) -> Result<()> {
        // region key range has no data prefix, so we must use origin key to check.
        try!(util::check_key_in_region(key, self.storage.rl().get_region()));

        Ok(())
    }

    fn do_get(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        // TODO: the get_get looks wried, maybe we should figure out a better name later.
        let key = req.get_get().get_key();
        try!(self.check_data_key(key));

        let mut resp = Response::new();
        let res = try!(ctx.snap.get_value(&keys::data_key(key)));
        if let Some(res) = res {
            resp.mut_get().set_value(res.to_vec());
        }

        Ok(resp)
    }

    fn do_seek(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let key = req.get_seek().get_key();
        try!(self.check_data_key(key));

        let mut resp = Response::new();
        let res = try!(ctx.snap.seek(&keys::data_key(key)));
        if let Some((k, v)) = res {
            resp.mut_seek().set_key(keys::origin_key(&k).to_vec());
            resp.mut_seek().set_value(v);
        }

        Ok(resp)
    }

    fn do_put(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        try!(self.check_data_key(key));

        let resp = Response::new();
        let key = keys::data_key(key);
        self.size_diff_hint += key.len() as u64;
        self.size_diff_hint += value.len() as u64;
        try!(ctx.wb.put(&key, value));

        Ok(resp)
    }

    fn do_delete(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        try!(self.check_data_key(key));

        let key = keys::data_key(key);
        // since size_diff_hint is not accurate, so we just skip calculate the value size.
        let klen = key.len() as u64;
        if self.size_diff_hint > klen {
            self.size_diff_hint -= klen;
        } else {
            self.size_diff_hint = 0;
        }
        let resp = Response::new();
        try!(ctx.wb.delete(&key));

        Ok(resp)
    }

    fn do_snap(&mut self, _: &ExecContext, _: &Request) -> Result<Response> {
        let mut resp = Response::new();
        resp.mut_snap().set_region(self.storage.rl().get_region().clone());
        Ok(resp)
    }
}
