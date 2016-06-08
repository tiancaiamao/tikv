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

use std::u64;

use raftstore::Result;

const RAFT_BASE_TICK_INTERVAL: u64 = 100;
const RAFT_HEARTBEAT_TICKS: usize = 3;
const RAFT_ELECTION_TIMEOUT_TICKS: usize = 15;
const RAFT_MAX_SIZE_PER_MSG: u64 = 1024 * 1024;
const RAFT_MAX_INFLIGHT_MSGS: usize = 256;
const RAFT_LOG_GC_INTERVAL: u64 = 5000;
const RAFT_LOG_GC_THRESHOLD: u64 = 50;
const RAFT_LOG_GC_LIMIT: u64 = 100000;
const SPLIT_REGION_CHECK_TICK_INTERVAL: u64 = 10000;
const REGION_SPLIT_SIZE: u64 = 64 * 1024 * 1024;
const REGION_MAX_SIZE: u64 = 80 * 1024 * 1024;
const REGION_CHECK_DIFF: u64 = 8 * 1024 * 1024;
const PD_HEARTBEAT_TICK_INTERVAL_MS: u64 = 5000;
const PD_STORE_HEARTBEAT_TICK_INTERVAL_MS: u64 = 30000;
const STORE_CAPACITY: u64 = u64::MAX;

#[derive(Debug, Clone)]
pub struct Config {
    // store capacity.
    // TODO: if not set, we will use disk capacity instead.
    // Now we will use a default capacity if not set.
    pub capacity: u64,

    // raft_base_tick_interval is a base tick interval (ms).
    pub raft_base_tick_interval: u64,
    pub raft_heartbeat_ticks: usize,
    pub raft_election_timeout_ticks: usize,
    pub raft_max_size_per_msg: u64,
    pub raft_max_inflight_msgs: usize,

    // Interval to gc unnecessary raft log (ms).
    pub raft_log_gc_tick_interval: u64,
    // A threshold to gc stale raft log, must >= 1.
    pub raft_log_gc_threshold: u64,
    // When entry count exceed this value, gc will be forced trigger.
    pub raft_log_gc_limit: u64,

    // Interval (ms) to check region whether need to be split or not.
    pub split_region_check_tick_interval: u64,
    /// When region [a, b) size meets region_max_size, it will be split
    /// into two region into [a, c), [c, b). And the size of [a, c) will
    /// be region_split_size (or a little bit smaller).
    pub region_max_size: u64,
    pub region_split_size: u64,
    /// When size change of region exceed the diff since last check, it
    /// will be checked again whether it should be split.
    pub region_check_size_diff: u64,
    pub pd_heartbeat_tick_interval: u64,
    pub pd_store_heartbeat_tick_interval: u64,

    /// Directory for snapshot files
    pub snap_dir: String,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            capacity: STORE_CAPACITY,
            raft_base_tick_interval: RAFT_BASE_TICK_INTERVAL,
            raft_heartbeat_ticks: RAFT_HEARTBEAT_TICKS,
            raft_election_timeout_ticks: RAFT_ELECTION_TIMEOUT_TICKS,
            raft_max_size_per_msg: RAFT_MAX_SIZE_PER_MSG,
            raft_max_inflight_msgs: RAFT_MAX_INFLIGHT_MSGS,
            raft_log_gc_tick_interval: RAFT_LOG_GC_INTERVAL,
            raft_log_gc_threshold: RAFT_LOG_GC_THRESHOLD,
            raft_log_gc_limit: RAFT_LOG_GC_LIMIT,
            split_region_check_tick_interval: SPLIT_REGION_CHECK_TICK_INTERVAL,
            region_max_size: REGION_MAX_SIZE,
            region_split_size: REGION_SPLIT_SIZE,
            region_check_size_diff: REGION_CHECK_DIFF,
            pd_heartbeat_tick_interval: PD_HEARTBEAT_TICK_INTERVAL_MS,
            pd_store_heartbeat_tick_interval: PD_STORE_HEARTBEAT_TICK_INTERVAL_MS,
            snap_dir: "".to_owned(),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        if self.raft_log_gc_threshold < 1 {
            return Err(box_err!("raft log gc threshold must >= 1, not {}",
                                self.raft_log_gc_threshold));
        }
        if self.snap_dir.is_empty() {
            return Err(box_err!("invalid snap path"));
        }

        Ok(())
    }
}
