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

use raftstore::store::{self, RaftStorage, SnapState};

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::error;
use std::io::{Seek, SeekFrom, Error as IoError};
use std::fs::File;
use std::time::Instant;
use std::path::PathBuf;

use rocksdb::{WriteBatch, Writable};
use kvproto::raftpb::Snapshot;
use raftstore::store::keys;
use raftstore::store::peer_storage::{SNAP_REV_PREFIX, SnapFile};

use util::worker::Runnable;
use util::codec::bytes::CompactBytesDecoder;
use util::codec::Error as CodecError;
use util::codec::number::NumberDecoder;

/// Snapshot generating task.
pub enum Task {
    Gen(Arc<RaftStorage>),
    Apply(Arc<RaftStorage>, Snapshot),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Gen(ref s) => write!(f, "Gen snap for {}", s.rl().get_region_id()),
            Task::Apply(ref s, _) => write!(f, "Apply snap for {}", s.rl().get_region_id()),
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
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: CodecError) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

pub struct Runner {
    snap_dir: PathBuf,
}

impl Runner {
    pub fn new<T: Into<PathBuf>>(snap_dir: T) -> Runner {
        Runner { snap_dir: snap_dir.into() }
    }

    fn generate_snap(&self, storage: &RaftStorage) -> Result<(), Error> {
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

        let snap = box_try!(store::do_snapshot(self.snap_dir.as_path(),
                                               &raw_snap,
                                               region_id,
                                               ranges,
                                               applied_idx,
                                               term));
        storage.wl().snap_state = SnapState::Snap(snap);
        Ok(())
    }

    fn apply_snap(&self, storage: &RaftStorage, snap: Snapshot) -> Result<(), Error> {
        let snap_dir = storage.rl().get_snap_dir();
        let mut snap_file = try!(SnapFile::from_snap(snap_dir, &snap, SNAP_REV_PREFIX));
        snap_file.delete_when_drop();
        if !snap_file.exists() {
            return Err(box_err!("missing snap file {}", snap_file.path().display()));
        }
        let mut reader = try!(File::open(snap_file.path()));
        let len = try!(reader.decode_u64());
        // do we need to check RaftMsg here?
        try!(reader.seek(SeekFrom::Current(len as i64)));

        let engine = storage.rl().get_engine();

        // Async apply snapshot should not skip region data.
        // Delete everything in the region for this peer.
        let mut timer = Instant::now();
        let w = WriteBatch::new();
        box_try!(storage.rl().scan_region(engine.as_ref(),
                                          &mut |key, _| {
                                              if key.starts_with(keys::DATA_PREFIX_KEY) {
                                                  try!(w.delete(key));
                                              }
                                              Ok(true)
                                          }));
        info!("clean old data takes {:?}", timer.elapsed());

        // Write the snapshot into the region.
        timer = Instant::now();
        loop {
            // TODO: avoid too many allocation
            let key = try!(reader.decode_compact_bytes());
            if key.is_empty() {
                break;
            }
            let value = try!(reader.decode_compact_bytes());
            box_try!(w.put(&key, &value));
        }
        info!("apply new data takes {:?}", timer.elapsed());

        box_try!(engine.write(w));
        info!("apply new data takes {:?}", timer.elapsed());
        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Gen(s) => {
                metric_incr!("raftstore.generate_snap");
                let ts = Instant::now();
                if let Err(e) = self.generate_snap(&s) {
                    error!("failed to generate snap: {:?}!!!", e);
                    s.wl().snap_state = SnapState::Failed;
                    return;
                }
                metric_incr!("raftstore.generate_snap.success");
                metric_time!("raftstore.generate_snap.cost", ts.elapsed());
            }
            Task::Apply(s, snap) => {
                if let Err(e) = self.apply_snap(&s, snap) {
                    error!("failed to apply snap: {:?}", e);
                }
                s.wl().snap_state = SnapState::Relax;
            }
        }
    }
}
