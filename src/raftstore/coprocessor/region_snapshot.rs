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

use rocksdb::{DB, SeekKey, DBVector, DBIterator};
use rocksdb::rocksdb::Snapshot;
use kvproto::metapb::Region;

use raftstore::store::engine::Peekable;
use raftstore::store::{keys, util, PeerStorage};
use raftstore::{Error, Result};


type Kv<'a> = (&'a [u8], &'a [u8]);

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
pub struct RegionSnapshot<'a> {
    snap: Snapshot<'a>,
    region: Region,
}

impl<'a> RegionSnapshot<'a> {
    pub fn new(ps: &'a PeerStorage) -> RegionSnapshot<'a> {
        RegionSnapshot {
            snap: ps.raw_snapshot(),
            region: ps.get_region().clone(),
        }
    }

    pub fn from_raw(db: &'a DB, region: Region) -> RegionSnapshot<'a> {
        RegionSnapshot {
            snap: db.snapshot(),
            region: region,
        }
    }

    pub fn get_region(&self) -> &Region {
        &self.region
    }

    pub fn iter(&self) -> RegionIterator {
        RegionIterator::new(self.snap.iter(), self.region.clone())
    }

    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(&self, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
        where F: FnMut(&[u8], &[u8]) -> Result<bool>
    {
        let mut it = self.iter();
        if !try!(it.seek(start_key)) {
            return Ok(());
        }
        let mut r = true;
        while r && it.valid() {
            r = {
                let key = it.key();
                if !end_key.is_empty() && key >= end_key {
                    break;
                }
                try!(f(key, it.value()))
            };
            it.next();
        }

        Ok(())
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }
}

impl<'a> Peekable for RegionSnapshot<'a> {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        try!(util::check_key_in_region(key, &self.region));
        let data_key = keys::data_key(key);
        self.snap.get_value(&data_key)
    }
}

/// `RegionIterator` wrap a rocksdb iterator and only allow it to
/// iterate in the region. It behaves as if underlying
/// db only contains one region.
pub struct RegionIterator<'a> {
    iter: DBIterator<'a>,
    valid: bool,
    region: Region,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

// we use rocksdb's style iterator, so omit the warning.
#[allow(should_implement_trait)]
impl<'a> RegionIterator<'a> {
    pub fn new(iter: DBIterator<'a>, region: Region) -> RegionIterator<'a> {
        RegionIterator {
            iter: iter,
            valid: false,
            start_key: keys::enc_start_key(&region),
            end_key: keys::enc_end_key(&region),
            region: region,
        }
    }

    pub fn seek_to_first(&mut self) -> bool {
        self.valid = self.iter.seek(self.start_key.as_slice().into());

        self.update_valid()
    }

    #[inline]
    fn update_valid(&mut self) -> bool {
        if self.valid {
            let key = self.iter.key();
            self.valid = key >= &self.start_key && key < &self.end_key;
        }
        self.valid
    }

    pub fn seek_to_last(&mut self) -> bool {
        if !self.iter.seek(self.end_key.as_slice().into()) && !self.iter.seek(SeekKey::End) {
            self.valid = false;
            return self.valid;
        }

        while self.iter.key() >= &self.end_key && self.iter.prev() {
        }

        self.valid = self.iter.valid();
        self.update_valid()
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        let key = try!(self.should_seekable(key));
        if key == self.end_key {
            self.valid = false;
        } else {
            self.valid = self.iter.seek(key.as_slice().into());
        }

        Ok(self.update_valid())
    }

    pub fn prev(&mut self) -> bool {
        if !self.valid {
            return false;
        }
        self.valid = self.iter.prev();

        self.update_valid()
    }

    pub fn next(&mut self) -> bool {
        if !self.valid {
            return false;
        }
        self.valid = self.iter.next();

        self.update_valid()
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        assert!(self.valid);
        keys::origin_key(self.iter.key())
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        assert!(self.valid);
        self.iter.value()
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.valid
    }

    #[inline]
    fn should_seekable(&self, key: &[u8]) -> Result<Vec<u8>> {
        let key = keys::data_key(key);
        if key < self.start_key || key > self.end_key {
            return Err(Error::KeyNotInRegion(key.to_vec(), self.region.clone()));
        }
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use rocksdb::{Writable, DB};
    use raftstore::store::engine::*;
    use raftstore::store::keys::*;
    use raftstore::store::PeerStorage;
    use storage::{Cursor, Key};

    use super::*;
    use std::sync::Arc;
    use kvproto::metapb::Region;

    type DataSet = Vec<(Vec<u8>, Vec<u8>)>;

    fn new_temp_engine(path: &TempDir) -> Arc<DB> {
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();
        Arc::new(engine)
    }

    fn new_peer_storage(snap_dir: &TempDir, engine: Arc<DB>, r: &Region) -> PeerStorage {
        PeerStorage::new(engine, r, snap_dir.path().to_str().unwrap().to_owned()).unwrap()
    }

    fn new_snapshot(peer_storage: &PeerStorage) -> RegionSnapshot {
        RegionSnapshot::new(peer_storage)
    }

    fn load_default_dataset(engine: Arc<DB>, snap_dir: &TempDir) -> (PeerStorage, DataSet) {
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(b"a2".to_vec());
        r.set_end_key(b"a7".to_vec());

        let base_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];

        for &(ref k, ref v) in &base_data {
            engine.put(&data_key(k), v).expect("");
        }
        let store = new_peer_storage(snap_dir, engine.clone(), &r);
        (store, base_data)
    }

    #[test]
    fn test_peekable() {
        let path = TempDir::new("test-raftstore").unwrap();
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let engine = new_temp_engine(&path);
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(b"key0".to_vec());
        r.set_end_key(b"key4".to_vec());
        let store = new_peer_storage(&snap_dir, engine.clone(), &r);

        let (key1, value1) = (b"key1", 2u64);
        engine.put_u64(&data_key(key1), value1).expect("");
        let (key2, value2) = (b"key2", 2i64);
        engine.put_i64(&data_key(key2), value2).expect("");
        let key3 = b"key3";
        engine.put_msg(&data_key(key3), &r).expect("");

        let snap = RegionSnapshot::new(&store);
        let v1 = snap.get_u64(key1).expect("");
        assert_eq!(v1, Some(value1));
        let v2 = snap.get_i64(key2).expect("");
        assert_eq!(v2, Some(value2));
        let v3 = snap.get_msg(key3).expect("");
        assert_eq!(v3, Some(r));

        let v0 = snap.get_value(b"key0").expect("");
        assert!(v0.is_none());

        let v4 = snap.get_value(b"key5");
        assert!(v4.is_err());
    }

    #[test]
    fn test_iterate() {
        let path = TempDir::new("test-raftstore").unwrap();
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let engine = new_temp_engine(&path);
        let (store, base_data) = load_default_dataset(engine.clone(), &snap_dir);

        let snap = RegionSnapshot::new(&store);
        let mut data = vec![];
        snap.scan(b"a2",
                  &[0xFF, 0xFF],
                  &mut |key, value| {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(true)
                  })
            .unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(data, &base_data[1..3]);

        let mut iter = snap.iter();
        assert!(iter.seek(b"a1").is_err());
        assert!(iter.seek(b"a2").unwrap());
        assert_eq!(iter.key(), b"a3");
        assert_eq!(iter.value(), b"v3");
        assert!(!iter.seek(b"a6").unwrap());
        assert!(!iter.seek(b"a7").unwrap());
        assert!(iter.seek(b"a8").is_err());

        data.clear();
        snap.scan(b"a2",
                  &[0xFF, 0xFF],
                  &mut |key, value| {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(false)
                  })
            .unwrap();

        assert_eq!(data.len(), 1);

        assert!(iter.seek_to_first());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next() {
                break;
            }
        }
        assert_eq!(res, base_data[1..3].to_vec());

        // test last region
        let store = new_peer_storage(&snap_dir, engine.clone(), &Region::new());
        let snap = RegionSnapshot::new(&store);
        data.clear();
        snap.scan(b"",
                  &[0xFF, 0xFF],
                  &mut |key, value| {
                      data.push((key.to_vec(), value.to_vec()));
                      Ok(true)
                  })
            .unwrap();

        assert_eq!(data.len(), 4);
        assert_eq!(data, base_data);

        let mut iter = snap.iter();
        assert!(iter.seek(b"a1").unwrap());

        assert!(iter.seek_to_first());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next() {
                break;
            }
        }
        assert_eq!(res, base_data);
    }

    #[test]
    fn test_reverse_iterate() {
        let path = TempDir::new("test-raftstore").unwrap();
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let engine = new_temp_engine(&path);
        let (store, test_data) = load_default_dataset(engine.clone(), &snap_dir);

        let snap = RegionSnapshot::new(&store);
        let mut iter = snap.iter();
        assert!(!iter.reverse_seek(&Key::from_encoded(b"a2".to_vec())).unwrap());
        assert!(iter.reverse_seek(&Key::from_encoded(b"a7".to_vec())).unwrap());
        let mut pair = (iter.key().to_vec(), iter.value().to_vec());
        assert_eq!(pair, (b"a5".to_vec(), b"v5".to_vec()));
        assert!(iter.reverse_seek(&Key::from_encoded(b"a5".to_vec())).unwrap());
        pair = (iter.key().to_vec(), iter.value().to_vec());
        assert_eq!(pair, (b"a3".to_vec(), b"v3".to_vec()));
        assert!(!iter.reverse_seek(&Key::from_encoded(b"a3".to_vec())).unwrap());
        assert!(iter.reverse_seek(&Key::from_encoded(b"a1".to_vec())).is_err());
        assert!(iter.reverse_seek(&Key::from_encoded(b"a8".to_vec())).is_err());

        assert!(iter.seek_to_last());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.prev() {
                break;
            }
        }
        let mut expect = test_data[1..3].to_vec();
        expect.reverse();
        assert_eq!(res, expect);

        // test last region
        let store = new_peer_storage(&snap_dir, engine.clone(), &Region::new());
        let snap = RegionSnapshot::new(&store);
        let mut iter = snap.iter();
        assert!(!iter.reverse_seek(&Key::from_encoded(b"a1".to_vec())).unwrap());
        assert!(iter.reverse_seek(&Key::from_encoded(b"a2".to_vec())).unwrap());
        let pair = (iter.key().to_vec(), iter.value().to_vec());
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        for kv_pairs in test_data.windows(2) {
            let seek_key = Key::from_encoded(kv_pairs[1].0.clone());
            assert!(iter.reverse_seek(&seek_key).unwrap(),
                    format!("{}", seek_key));
            let pair = (iter.key().to_vec(), iter.value().to_vec());
            assert_eq!(pair, kv_pairs[0]);
        }

        assert!(iter.seek_to_last());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.prev() {
                break;
            }
        }
        let mut expect = test_data.clone();
        expect.reverse();
        assert_eq!(res, expect);
    }
}
