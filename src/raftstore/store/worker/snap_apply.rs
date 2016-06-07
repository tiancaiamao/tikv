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

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::time::Instant;

use rocksdb::{WriteBatch, Writable};
use protobuf::Message;

use kvproto::raftpb::{Snapshot, HardState};
use kvproto::raft_serverpb::{RaftSnapshotData, RaftTruncatedState};
use raftstore::store::{RaftStorage, keys};
use raftstore::store::engine::{Mutable, Peekable};
use raftstore::store::peer_storage::{SnapApplyState, ApplySnapResult, save_last_index,
                                     save_truncated_state};
use util::worker::Runnable;

pub struct Task {
    snapshot: Snapshot,
    storage: Arc<RaftStorage>,
}

impl Task {
    pub fn new(snap: Snapshot, storage: Arc<RaftStorage>) -> Task {
        Task {
            snapshot: snap,
            storage: storage,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "Snapshot Apply Task for {}",
               self.storage.rl().get_region_id())
    }
}

pub struct Runner;

impl Runnable<Task> for Runner {
    // TODO read data from file
    fn run(&mut self, task: Task) {
        print!("run in snap apply worker\n");
        let w = WriteBatch::new();
        let snap = task.snapshot;
        let db = task.storage.rl().get_engine();
        let region_id = task.storage.rl().get_region_id();

        let mut snap_data = RaftSnapshotData::new();
        snap_data.merge_from_bytes(snap.get_data()).unwrap();

        // Apply snapshot should not overwrite current hard state which
        // records the previous vote.
        // TODO: maybe exclude hard state when do snapshot.
        let hard_state_key = keys::raft_hard_state_key(region_id);
        let hard_state: Option<HardState> = db.get_msg(&hard_state_key).unwrap();

        let region = snap_data.get_region();
        if region.get_id() != region_id {
            panic!("mismatch region id {} != {}", region_id, region.get_id());
        }
        let mut timer = Instant::now();
        print!("delete everything in the region for this peer\n");
        // Delete everything in the region for this peer.
        {
            let peer_storage = task.storage.wl();
            peer_storage.scan_region(db.as_ref(),
                             &mut |key, _| {
                                 w.delete(key).unwrap();
                                 Ok(true)
                             })
                .unwrap();
        }
        info!("clean old data takes {:?}", timer.elapsed());
        timer = Instant::now();
        // Write the snapshot into the region.
        for kv in snap_data.get_data() {
            w.put(kv.get_key(), kv.get_value()).unwrap();
        }
        info!("apply new data takes {:?}", timer.elapsed());
        // Restore the hard state
        match hard_state {
            None => {
                w.delete(&hard_state_key).unwrap();
            }
            Some(state) => {
                w.put_msg(&hard_state_key, &state).unwrap();
            }
        }

        db.write(w).unwrap();
        print!("apply snapshot ok for region {}\n", region_id);

        // If we apply snapshot ok, we should update some infos like applied index too.
        let last_index = snap.get_metadata().get_index();
        task.storage.wl().set_applied_index(last_index);
        task.storage.wl().set_region(&region);
        task.storage.wl().snap_applying = SnapApplyState::Success(region.clone());
        print!("not dead lock!!!\n");
    }
}
