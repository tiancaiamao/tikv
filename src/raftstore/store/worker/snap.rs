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

use raftstore::store::{self, RaftStorage, SnapState, PeerStorage};
use raftstore::store::engine::{Peekable, Iterable, Mutable};
use rocksdb::{DB, WriteBatch, Writable};
use kvproto::raftpb::Snapshot;
use kvproto::raft_serverpb::{RaftSnapshotData, KeyValue, RaftTruncatedState};
use kvproto::metapb;
use protobuf::{self, Message};

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::error;
use std::time::Instant;

use util::worker::Runnable;

pub enum Task {
    Generate {
        storage: Arc<RaftStorage>,
    },
    Apply {
        db: Arc<DB>,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
        snapshot: Snapshot,
    },
}

impl Task {
    pub fn new(storage: Arc<RaftStorage>) -> Task {
        Task::Generate { storage: storage }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Generate { ref storage } => {
                write!(f, "Generate Snapshot for {}", storage.rl().get_region_id())
            }
            Task::Apply { .. } => write!(f, "Apply snapshot"),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub struct Runner;

impl Runner {
    fn generate_snap(&self, storage: Arc<RaftStorage>) -> Result<(), Error> {
        // do we need to check leader here?
        let db = storage.rl().get_engine();
        let raw_snap;
        let region_id;
        let ranges;
        let applied_idx;
        let term;

        {
            let storage = storage.rl();
            raw_snap = db.snapshot();
            region_id = storage.get_region_id();
            ranges = storage.region_key_ranges();
            applied_idx = box_try!(storage.load_applied_index(&raw_snap));
            term = box_try!(storage.term(applied_idx));
        }

        let snap = box_try!(store::do_snapshot(&raw_snap, region_id, ranges, applied_idx, term));
        storage.wl().snap_state = SnapState::Snap(snap);
        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Generate { storage } => {
                metric_incr!("raftstore.generate_snap");
                let ts = Instant::now();
                if let Err(e) = self.generate_snap(storage.clone()) {
                    error!("failed to generate snap: {:?}!!!", e);
                    storage.wl().snap_state = SnapState::Failed;
                    return;
                }
                metric_incr!("raftstore.generate_snap.success");
                metric_time!("raftstore.generate_snap.cost", ts.elapsed());
            }
            Task::Apply { db, ranges, snapshot } => {
                let mut snap_data = RaftSnapshotData::new();
                snap_data.merge_from_bytes(snapshot.get_data());

                let w = WriteBatch::new();
                let mut timer = Instant::now();
                // Delete everything in the region for this peer.
                for r in ranges {
                    db.scan(&r.0,
                            &r.1,
                            &mut |key, _| {
                                w.delete(key);
                                Ok(true)
                            });
                }
                info!("clean old data takes {:?}", timer.elapsed());
                timer = Instant::now();
                // Write the snapshot into the region.
                for kv in snap_data.get_data() {
                    w.put(kv.get_key(), kv.get_value());
                }
                info!("apply new data takes {:?}", timer.elapsed());
            }
        }
    }
}
