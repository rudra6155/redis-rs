//! Expiry contract (SPEC.md section 5).
//!
//! There is no `DBSIZE`, so "was the key physically deleted without anyone touching
//! it?" is observed through the library (`Store::raw_len`): reading the key over
//! the wire would trigger lazy expiry and prove nothing about the sweeper.

mod common;

use common::*;
use redis_rs::expiry::{now_ms, Sweeper, MAX_ROUNDS_PER_SHARD, SAMPLES_PER_ROUND};
use redis_rs::store::Store;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Sweeper (active expiry)
// ---------------------------------------------------------------------------

#[test]
fn sweeper_removes_an_untouched_key_after_px_50_and_150ms() {
    let store = Arc::new(Store::new(4));
    let sweeper = Sweeper::spawn(store.clone());
    let now = now_ms();
    store.locked(b"k", |sh| sh.set(b"k", b"v", Some(now + 50)));
    assert_eq!(store.raw_len(), 1);

    std::thread::sleep(Duration::from_millis(150));

    // Nothing has read the key. It is *physically* gone, and a read agrees.
    assert_eq!(store.raw_len(), 0, "sweeper did not reclaim the expired key");
    assert_eq!(store.ttl_len(), 0);
    assert_eq!(store.locked(b"k", |sh| sh.get(b"k", now_ms())), Ok(None));
    sweeper.stop();
}

#[test]
fn sweeper_reclaims_within_500ms_of_the_deadline() {
    let store = Arc::new(Store::new(4));
    let sweeper = Sweeper::spawn(store.clone());
    let deadline = now_ms() + 50;
    store.locked(b"k", |sh| sh.set(b"k", b"v", Some(deadline)));
    let started = Instant::now();
    while store.raw_len() != 0 {
        assert!(started.elapsed() < Duration::from_secs(5), "never reclaimed");
        std::thread::sleep(Duration::from_millis(5));
    }
    let late_by = now_ms().saturating_sub(deadline);
    assert!(late_by <= 500, "reclaimed {late_by} ms after the deadline (limit 500)");
    sweeper.stop();
}

#[test]
fn sweeper_leaves_live_keys_and_keys_without_ttl_alone() {
    let store = Arc::new(Store::new(4));
    let sweeper = Sweeper::spawn(store.clone());
    let now = now_ms();
    store.locked(b"live", |sh| sh.set(b"live", b"v", Some(now + 60_000)));
    store.locked(b"forever", |sh| sh.set(b"forever", b"v", None));
    store.locked(b"dies", |sh| sh.set(b"dies", b"v", Some(now + 20)));
    std::thread::sleep(Duration::from_millis(350));
    assert_eq!(store.raw_len(), 2);
    store.check_invariants();
    sweeper.stop();
}

#[test]
fn sweep_work_per_tick_is_bounded_and_converges() {
    // The documented tradeoff: expired keys can outlive a tick when there are many.
    let store = Store::new(4);
    let n = 20_000;
    for i in 0..n {
        let key = format!("k{i}");
        store.locked(key.as_bytes(), |sh| sh.set(key.as_bytes(), b"v", Some(1)));
    }
    let now = now_ms();
    let first = store.sweep_cycle(now);
    let cap = 4 * MAX_ROUNDS_PER_SHARD * SAMPLES_PER_ROUND;
    assert!(first.sampled <= cap, "sampled {} > cap {cap}", first.sampled);
    assert!(first.deleted > 0);
    assert!(store.raw_len() > 0, "a single tick must not be able to reap everything (that is the bound)");

    let mut ticks = 1;
    while store.raw_len() > 0 {
        store.sweep_cycle(now);
        ticks += 1;
        assert!(ticks < 200, "sweeping did not converge");
    }
    store.check_invariants();
}

// ---------------------------------------------------------------------------
// Lazy expiry (no sweeper at all)
// ---------------------------------------------------------------------------

#[test]
fn lazy_expiry_works_with_the_sweeper_disabled() {
    let store = Store::new(4); // no Sweeper::spawn: nothing but lazy expiry exists
    let now = now_ms();
    store.locked(b"k", |sh| sh.set(b"k", b"v", Some(now + 50)));
    std::thread::sleep(Duration::from_millis(120));
    let later = now_ms();

    assert_eq!(store.raw_len(), 1, "with no sweeper the expired key must still be physically present");
    assert_eq!(store.locked(b"k", |sh| sh.get(b"k", later)), Ok(None), "but it must not be readable");
    assert_eq!(store.raw_len(), 0, "the read path deleted it (lazy expiry)");
}

/// A fresh store holding one key `k` whose deadline is `NOW + 10` ms.
fn planted(hash: bool) -> Store {
    let store = Store::new(4);
    store.locked(b"k", |sh| {
        if hash {
            sh.hset(b"k", &[b"f", b"v"], NOW).unwrap();
            sh.expire_at(b"k", (NOW + 10) as i64, NOW);
        } else {
            sh.set(b"k", b"5", Some(NOW + 10));
        }
    });
    assert_eq!(store.raw_len(), 1);
    store
}

const NOW: u64 = 1_000_000;
/// 90 ms past the deadline.
const AFTER: u64 = NOW + 100;

fn assert_gone(store: &Store, path: &str) {
    assert_eq!(store.raw_len(), 0, "{path}: the expired key was not removed by this command");
    store.check_invariants();
}

#[test]
fn every_read_and_write_path_expires_lazily() {
    let s = planted(false);
    assert_eq!(s.locked(b"k", |sh| sh.get(b"k", AFTER)), Ok(None));
    assert_gone(&s, "GET");

    let s = planted(false);
    assert!(!s.locked(b"k", |sh| sh.exists(b"k", AFTER)));
    assert_gone(&s, "EXISTS");

    let s = planted(false);
    assert_eq!(s.locked(b"k", |sh| sh.ttl(b"k", AFTER)), -2);
    assert_gone(&s, "TTL");

    let s = planted(false);
    assert!(!s.locked(b"k", |sh| sh.del(b"k", AFTER)), "DEL of an expired key counts 0");
    assert_gone(&s, "DEL");

    let s = planted(false);
    assert!(!s.locked(b"k", |sh| sh.expire_at(b"k", (AFTER + 1000) as i64, AFTER)));
    assert_gone(&s, "EXPIRE");

    let s = planted(false);
    assert_eq!(s.locked(b"k", |sh| sh.incr(b"k", AFTER)), Ok(1), "INCR starts from scratch");
    assert_eq!(s.locked(b"k", |sh| sh.ttl(b"k", AFTER)), -1, "and the fresh key carries no TTL");
    s.check_invariants();

    let s = planted(true);
    assert_eq!(s.locked(b"k", |sh| sh.hget(b"k", b"f", AFTER)), Ok(None));
    assert_gone(&s, "HGET");

    let s = planted(true);
    assert_eq!(s.locked(b"k", |sh| sh.hlen(b"k", AFTER)), Ok(0));
    assert_gone(&s, "HLEN");

    let s = planted(true);
    assert_eq!(s.locked(b"k", |sh| sh.hgetall(b"k", AFTER)), Ok(vec![]));
    assert_gone(&s, "HGETALL");

    let s = planted(true);
    assert_eq!(s.locked(b"k", |sh| sh.hdel(b"k", &[b"f"], AFTER)), Ok(0));
    assert_gone(&s, "HDEL");

    let s = planted(true);
    assert_eq!(s.locked(b"k", |sh| sh.hset(b"k", &[b"g", b"w"], AFTER)), Ok(1), "HSET builds a new hash");
    assert_eq!(s.locked(b"k", |sh| sh.hlen(b"k", AFTER)), Ok(1));
    assert_eq!(s.locked(b"k", |sh| sh.ttl(b"k", AFTER)), -1);
    s.check_invariants();
}

// ---------------------------------------------------------------------------
// Over the wire
// ---------------------------------------------------------------------------

fn wire_expiry(sweeper: &str) {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&["--sweeper", sweeper], dir.path());
    let mut c = server.client();
    assert_eq!(c.cmd(&["SET", "k", "v", "PX", "50"]), ok());
    assert_eq!(c.cmd(&["GET", "k"]), bulk("v"));
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(c.cmd(&["GET", "k"]), redis_rs::resp::Frame::Null, "sweeper={sweeper}");
    assert_eq!(c.cmd(&["EXISTS", "k"]), int(0));
    assert_eq!(c.cmd(&["TTL", "k"]), int(-2));
}

#[test]
fn px_50_key_is_unreadable_after_150ms_with_the_sweeper_on() {
    wire_expiry("yes");
}

#[test]
fn px_50_key_is_unreadable_after_150ms_with_the_sweeper_off() {
    wire_expiry("no");
}

#[test]
fn ttl_reports_minus2_minus1_and_remaining_seconds() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&[], dir.path());
    let mut c = server.client();

    assert_eq!(c.cmd(&["TTL", "missing"]), int(-2));

    assert_eq!(c.cmd(&["SET", "plain", "v"]), ok());
    assert_eq!(c.cmd(&["TTL", "plain"]), int(-1));

    // Redis rounds to the nearest second (SPEC 4.1): a fresh EX 100 key reads 100, not 99.
    assert_eq!(c.cmd(&["SET", "ex", "v", "EX", "100"]), ok());
    assert_eq!(c.cmd(&["TTL", "ex"]), int(100));
    // remaining ~1.9 s -> 2 (truncation would say 1)
    assert_eq!(c.cmd(&["SET", "ms", "v", "PX", "1900"]), ok());
    assert_eq!(c.cmd(&["TTL", "ms"]), int(2));
}

#[test]
fn set_and_expire_interact_with_ttl_as_specified() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start(&[], dir.path());
    let mut c = server.client();

    // SET without EX/PX clears the TTL
    c.cmd(&["SET", "k", "v", "EX", "100"]);
    c.cmd(&["SET", "k", "v2"]);
    assert_eq!(c.cmd(&["TTL", "k"]), int(-1));

    // EXPIRE: 1 on success, replaces an existing TTL, 0 if the key is missing
    assert_eq!(c.cmd(&["EXPIRE", "k", "50"]), int(1));
    assert_eq!(c.cmd(&["TTL", "k"]), int(50));
    assert_eq!(c.cmd(&["EXPIRE", "k", "200"]), int(1));
    assert_eq!(c.cmd(&["TTL", "k"]), int(200));
    assert_eq!(c.cmd(&["EXPIRE", "missing", "10"]), int(0));

    // EXPIRE with a non-positive time deletes the key
    assert_eq!(c.cmd(&["EXPIRE", "k", "0"]), int(1));
    assert_eq!(c.cmd(&["EXISTS", "k"]), int(0));

    // INCR and HSET preserve the TTL
    c.cmd(&["SET", "n", "1", "EX", "100"]);
    assert_eq!(c.cmd(&["INCR", "n"]), int(2));
    assert_eq!(c.cmd(&["TTL", "n"]), int(100));
    c.cmd(&["HSET", "h", "a", "1"]);
    c.cmd(&["EXPIRE", "h", "100"]);
    assert_eq!(c.cmd(&["HSET", "h", "b", "2"]), int(1));
    assert_eq!(c.cmd(&["TTL", "h"]), int(100));

    // SET and DEL on a key with a TTL
    assert_eq!(c.cmd(&["DEL", "n"]), int(1));
    assert_eq!(c.cmd(&["TTL", "n"]), int(-2));
}

// ---------------------------------------------------------------------------
// Model-based test: the TTL index and every command agree with a trivial reference
// ---------------------------------------------------------------------------

mod model {
    use bytes::Bytes;
    use redis_rs::resp::parse_i64;
    use redis_rs::store::{Store, StoreError};
    use std::collections::HashMap;

    pub enum MValue {
        Str(Vec<u8>),
        Hash(Vec<(Vec<u8>, Vec<u8>)>), // insertion ordered
    }

    pub struct Model {
        pub map: HashMap<Vec<u8>, (MValue, Option<u64>)>,
    }

    impl Model {
        pub fn new() -> Model {
            Model { map: HashMap::new() }
        }

        fn live(&mut self, key: &[u8], now: u64) -> bool {
            if let Some((_, Some(t))) = self.map.get(key) {
                if now > *t {
                    self.map.remove(key);
                }
            }
            self.map.contains_key(key)
        }

        pub fn set(&mut self, key: &[u8], v: &[u8], at: Option<u64>) {
            self.map.insert(key.to_vec(), (MValue::Str(v.to_vec()), at));
        }

        pub fn get(&mut self, key: &[u8], now: u64) -> Result<Option<Bytes>, StoreError> {
            if !self.live(key, now) {
                return Ok(None);
            }
            match &self.map[key].0 {
                MValue::Str(s) => Ok(Some(Bytes::copy_from_slice(s))),
                MValue::Hash(_) => Err(StoreError::WrongType),
            }
        }

        pub fn del(&mut self, key: &[u8], now: u64) -> bool {
            self.live(key, now) && self.map.remove(key).is_some()
        }

        pub fn ttl(&mut self, key: &[u8], now: u64) -> i64 {
            if !self.live(key, now) {
                return -2;
            }
            match self.map[key].1 {
                None => -1,
                Some(t) => ((t - now + 500) / 1000) as i64,
            }
        }

        pub fn expire_at(&mut self, key: &[u8], at: i64, now: u64) -> bool {
            if !self.live(key, now) {
                return false;
            }
            if at <= now as i64 {
                self.map.remove(key);
            } else {
                self.map.get_mut(key).unwrap().1 = Some(at as u64);
            }
            true
        }

        pub fn incr(&mut self, key: &[u8], now: u64) -> Result<i64, StoreError> {
            if !self.live(key, now) {
                self.map.insert(key.to_vec(), (MValue::Str(b"1".to_vec()), None));
                return Ok(1);
            }
            match &mut self.map.get_mut(key).unwrap().0 {
                MValue::Hash(_) => Err(StoreError::WrongType),
                MValue::Str(s) => {
                    let cur = parse_i64(s).ok_or(StoreError::NotInteger)?;
                    let new = cur.checked_add(1).ok_or(StoreError::Overflow)?;
                    *s = new.to_string().into_bytes();
                    Ok(new)
                }
            }
        }

        pub fn hset(&mut self, key: &[u8], pairs: &[&[u8]], now: u64) -> Result<usize, StoreError> {
            if !self.live(key, now) {
                self.map.insert(key.to_vec(), (MValue::Hash(vec![]), None));
            }
            let MValue::Hash(h) = &mut self.map.get_mut(key).unwrap().0 else {
                return Err(StoreError::WrongType);
            };
            let mut added = 0;
            for kv in pairs.chunks(2) {
                match h.iter_mut().find(|(f, _)| f == kv[0]) {
                    Some(slot) => slot.1 = kv[1].to_vec(),
                    None => {
                        h.push((kv[0].to_vec(), kv[1].to_vec()));
                        added += 1;
                    }
                }
            }
            Ok(added)
        }

        pub fn hget(&mut self, key: &[u8], f: &[u8], now: u64) -> Result<Option<Bytes>, StoreError> {
            if !self.live(key, now) {
                return Ok(None);
            }
            match &self.map[key].0 {
                MValue::Hash(h) => Ok(h.iter().find(|(k, _)| k == f).map(|(_, v)| Bytes::copy_from_slice(v))),
                MValue::Str(_) => Err(StoreError::WrongType),
            }
        }

        pub fn hdel(&mut self, key: &[u8], fields: &[&[u8]], now: u64) -> Result<usize, StoreError> {
            if !self.live(key, now) {
                return Ok(0);
            }
            let MValue::Hash(h) = &mut self.map.get_mut(key).unwrap().0 else {
                return Err(StoreError::WrongType);
            };
            let mut removed = 0;
            for f in fields {
                if let Some(i) = h.iter().position(|(k, _)| k == f) {
                    h.remove(i);
                    removed += 1;
                }
            }
            if h.is_empty() {
                self.map.remove(key);
            }
            Ok(removed)
        }

        pub fn hgetall(&mut self, key: &[u8], now: u64) -> Result<Vec<Bytes>, StoreError> {
            if !self.live(key, now) {
                return Ok(vec![]);
            }
            match &self.map[key].0 {
                MValue::Hash(h) => {
                    Ok(h.iter().flat_map(|(f, v)| [Bytes::copy_from_slice(f), Bytes::copy_from_slice(v)]).collect())
                }
                MValue::Str(_) => Err(StoreError::WrongType),
            }
        }

        pub fn live_count(&mut self, now: u64) -> usize {
            let keys: Vec<_> = self.map.keys().cloned().collect();
            keys.iter().filter(|k| self.live(k, now)).count()
        }
    }

    pub fn store() -> Store {
        Store::new(1) // one shard: every key shares one TTL index, the hardest case for slot bookkeeping
    }
}

#[test]
fn store_agrees_with_a_reference_model_under_random_operations() {
    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    let store = model::store();
    let mut m = model::Model::new();
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    let keys: Vec<Vec<u8>> = (0..12).map(|i| format!("key{i}").into_bytes()).collect();
    let fields: [&[u8]; 4] = [b"f0", b"f1", b"f2", b"f3"];
    let values: [&[u8]; 6] = [b"abc", b"7", b"-3", b"9223372036854775807", b"0", b"007"];
    let mut now = 1_000_000u64;

    for step in 0..60_000 {
        now += rng.below(250);
        let key = &keys[rng.below(keys.len() as u64) as usize];
        let k = key.as_slice();
        let at = if rng.below(2) == 0 { None } else { Some(now + rng.below(1500)) };
        let field = fields[rng.below(4) as usize];
        let value = values[rng.below(6) as usize];
        let op = rng.below(13);
        let ctx = format!("step {step} op {op} key {} now {now}", String::from_utf8_lossy(k));
        match op {
            0 => {
                store.locked(k, |sh| sh.set(k, value, at));
                m.set(k, value, at);
            }
            1 => assert_eq!(store.locked(k, |sh| sh.get(k, now)), m.get(k, now), "get: {ctx}"),
            2 => assert_eq!(store.locked(k, |sh| sh.del(k, now)), m.del(k, now), "del: {ctx}"),
            3 => assert_eq!(store.locked(k, |sh| sh.ttl(k, now)), m.ttl(k, now), "ttl: {ctx}"),
            4 => {
                let at = now as i64 + rng.below(3000) as i64 - 500;
                assert_eq!(store.locked(k, |sh| sh.expire_at(k, at, now)), m.expire_at(k, at, now), "expire: {ctx}");
            }
            5 => assert_eq!(store.locked(k, |sh| sh.incr(k, now)), m.incr(k, now), "incr: {ctx}"),
            6 | 7 => {
                let pairs: Vec<&[u8]> = vec![field, value, fields[rng.below(4) as usize], value];
                assert_eq!(store.locked(k, |sh| sh.hset(k, &pairs, now)), m.hset(k, &pairs, now), "hset: {ctx}");
            }
            8 => assert_eq!(store.locked(k, |sh| sh.hget(k, field, now)), m.hget(k, field, now), "hget: {ctx}"),
            9 => {
                let del: Vec<&[u8]> = vec![field, fields[rng.below(4) as usize]];
                assert_eq!(store.locked(k, |sh| sh.hdel(k, &del, now)), m.hdel(k, &del, now), "hdel: {ctx}");
            }
            10 => assert_eq!(store.locked(k, |sh| sh.hgetall(k, now)), m.hgetall(k, now), "hgetall: {ctx}"),
            11 => {
                assert_eq!(store.locked(k, |sh| sh.hlen(k, now)), m.hgetall(k, now).map(|v| v.len() / 2), "hlen: {ctx}")
            }
            _ => {
                // the active sweeper may run at any moment; it must be invisible to the model
                store.sweep_cycle(now);
            }
        }
        store.check_invariants();
    }
    // physical keys >= live keys (sweeper may lag), never fewer
    assert!(store.raw_len() >= m.live_count(now));
}
