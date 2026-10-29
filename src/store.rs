//! Sharded keyspace.
//!
//! * [`Store`] owns N [`Shard`]s, each behind its own `std::sync::Mutex`.
//!   A key lives in exactly one shard (`hash(key) % N`).
//! * [`Store::locked`] is the **only** way to reach a shard. The guard lives
//!   inside that synchronous function, so no caller can hold it across an
//!   `.await`, and a command that names one key can only ever hold one shard
//!   lock (see ARCHITECTURE.md, lock inventory).
//! * Every mutating `Shard` method appends its own AOF record (the *effect* of
//!   the command, with absolute deadlines) while the shard lock is still held,
//!   so the log order of two writes to the same key is the order they were
//!   applied in. Mutation and journaling cannot be separated by a caller.
//! * Every `Shard` method takes `now` (unix ms) explicitly: lazy expiry is a
//!   pure function of the arguments, which is what makes it unit-testable. AOF
//!   replay passes `now = 0`, under which nothing is ever expired.

use crate::aof::AofSink;
use crate::expiry::{MAX_ROUNDS_PER_SHARD, SAMPLES_PER_ROUND};
use crate::resp::{encode_request, parse_i64};
use bytes::Bytes;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{Mutex, PoisonError};

/// Default number of shards.
pub const DEFAULT_SHARDS: usize = 64;

/// Failures a data-level operation can report; `command.rs` maps them to Redis error strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    /// Key holds a different type than the command needs.
    WrongType,
    /// `INCR` on a value that is not a canonical `i64`.
    NotInteger,
    /// `INCR` past `i64::MAX`.
    Overflow,
}

enum Value {
    Str(Bytes),
    Hash(HashValue),
}

/// Hash value. Fields carry an insertion sequence number so `HGETALL` can
/// answer in insertion order (what Redis' listpack encoding does for small
/// hashes) while every other operation stays O(1).
#[derive(Default)]
struct HashValue {
    fields: HashMap<Bytes, (u64, Bytes)>,
    next_seq: u64,
}

impl HashValue {
    /// Insert/overwrite `[f, v, f, v, ...]`; returns how many fields were *new*.
    fn insert_all(&mut self, pairs: &[&[u8]]) -> usize {
        let mut added = 0;
        for &[field, value] in pairs.as_chunks::<2>().0 {
            let value = Bytes::copy_from_slice(value);
            match self.fields.get_mut(field) {
                Some(slot) => slot.1 = value,
                None => {
                    self.fields.insert(Bytes::copy_from_slice(field), (self.next_seq, value));
                    self.next_seq += 1;
                    added += 1;
                }
            }
        }
        added
    }
}

struct Entry {
    value: Value,
    /// Absolute deadline, unix ms. The key is expired once `now > expires_at` (same test as Redis).
    expires_at: Option<u64>,
    /// Index of this key in `Shard::ttl_keys`; meaningful only while `expires_at.is_some()`.
    ttl_slot: usize,
}

impl Entry {
    fn is_expired(&self, now: u64) -> bool {
        matches!(self.expires_at, Some(t) if now > t)
    }
}

/// What one sweep round did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Keys examined.
    pub sampled: usize,
    /// Of those, keys that were expired and removed.
    pub deleted: usize,
}

/// One shard: a hash map plus the list of keys that currently carry a TTL.
pub struct Shard {
    map: HashMap<Box<[u8]>, Entry>,
    /// Exactly the keys with `expires_at.is_some()`, each once. Lets the sweeper
    /// sample TTL keys uniformly in O(1); each `Entry` remembers its slot so
    /// removal is `swap_remove` + one fix-up.
    ttl_keys: Vec<Box<[u8]>>,
    aof: Option<AofSink>,
    /// Set whenever a record was queued for the AOF; drained by the caller.
    wrote: bool,
    rng: u64,
}

impl Shard {
    fn new(seed: u64) -> Shard {
        Shard { map: HashMap::new(), ttl_keys: Vec::new(), aof: None, wrote: false, rng: seed | 1 }
    }

    /// Did a method since the last call queue an AOF record? Resets the flag.
    pub fn take_wrote(&mut self) -> bool {
        std::mem::take(&mut self.wrote)
    }

    /// Queue one AOF record (a RESP array of bulk strings). No-op without an AOF.
    fn log(&mut self, args: &[&[u8]]) {
        if let Some(aof) = &self.aof {
            let mut rec = Vec::with_capacity(32 + args.iter().map(|a| a.len() + 16).sum::<usize>());
            encode_request(args, &mut rec);
            aof.record(rec);
            self.wrote = true;
        }
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64: plenty for choosing sweep samples.
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    // ---- internal primitives: the only places `map` and `ttl_keys` change ----

    fn insert_entry(&mut self, key: &[u8], value: Value, expires_at: Option<u64>) {
        debug_assert!(!self.map.contains_key(key));
        let ttl_slot = if expires_at.is_some() {
            self.ttl_keys.push(key.into());
            self.ttl_keys.len() - 1
        } else {
            0
        };
        self.map.insert(key.into(), Entry { value, expires_at, ttl_slot });
    }

    fn remove_entry(&mut self, key: &[u8]) -> Option<Entry> {
        let entry = self.map.remove(key)?;
        if entry.expires_at.is_some() {
            self.untrack_ttl(entry.ttl_slot);
        }
        Some(entry)
    }

    fn untrack_ttl(&mut self, slot: usize) {
        self.ttl_keys.swap_remove(slot);
        // The former last element now sits at `slot`; point its entry there.
        if let Some(moved) = self.ttl_keys.get(slot) {
            if let Some(entry) = self.map.get_mut(&**moved) {
                entry.ttl_slot = slot;
            }
        }
    }

    fn set_expiry(&mut self, key: &[u8], at: Option<u64>) {
        let Some(entry) = self.map.get_mut(key) else {
            return;
        };
        match (entry.expires_at, at) {
            (None, Some(t)) => {
                entry.expires_at = Some(t);
                entry.ttl_slot = self.ttl_keys.len();
                self.ttl_keys.push(key.into());
            }
            (Some(_), Some(t)) => entry.expires_at = Some(t),
            (Some(_), None) => {
                let slot = entry.ttl_slot;
                entry.expires_at = None;
                self.untrack_ttl(slot);
            }
            (None, None) => {}
        }
    }

    /// Lazy expiry: if `key` is past its deadline, delete it and journal a `DEL`.
    /// Returns whether it was deleted.
    ///
    /// The `DEL` matters: without it, replaying the AOF could keep a key that
    /// had expired at runtime and apply later commands (e.g. `INCR`) to it.
    fn expire_if_needed(&mut self, key: &[u8], now: u64) -> bool {
        let expired = matches!(self.map.get(key), Some(e) if e.is_expired(now));
        if expired {
            self.remove_entry(key);
            self.log(&[b"DEL", key]);
        }
        expired
    }

    /// The live (non-expired) entry for `key`, expiring it first if needed.
    fn live(&mut self, key: &[u8], now: u64) -> Option<&mut Entry> {
        self.expire_if_needed(key, now);
        self.map.get_mut(key)
    }

    // ---- strings and generic keys ----

    /// `GET`. `Ok(None)` if missing or expired.
    pub fn get(&mut self, key: &[u8], now: u64) -> Result<Option<Bytes>, StoreError> {
        match self.map.get(key) {
            None => Ok(None),
            Some(e) if e.is_expired(now) => {
                self.expire_if_needed(key, now);
                Ok(None)
            }
            Some(e) => match &e.value {
                Value::Str(b) => Ok(Some(b.clone())),
                Value::Hash(_) => Err(StoreError::WrongType),
            },
        }
    }

    /// `SET`: replace whatever is at `key` (any type) and set the deadline in the
    /// same lock scope. `expires_at = None` clears any previous TTL.
    pub fn set(&mut self, key: &[u8], value: &[u8], expires_at: Option<u64>) {
        self.remove_entry(key);
        // Copy: `value` borrows the connection's read buffer; storing a slice of it
        // would pin the whole buffer for as long as the key lives.
        self.insert_entry(key, Value::Str(Bytes::copy_from_slice(value)), expires_at);
        match expires_at {
            Some(at) => {
                let at = at.to_string();
                self.log(&[b"SET", key, value, b"PXAT", at.as_bytes()]);
            }
            None => self.log(&[b"SET", key, value]),
        }
    }

    /// `DEL`. True if a live key was removed.
    pub fn del(&mut self, key: &[u8], now: u64) -> bool {
        if self.expire_if_needed(key, now) {
            return false;
        }
        let removed = self.remove_entry(key).is_some();
        if removed {
            self.log(&[b"DEL", key]);
        }
        removed
    }

    /// `EXISTS`.
    pub fn exists(&mut self, key: &[u8], now: u64) -> bool {
        self.live(key, now).is_some()
    }

    /// `TTL`: -2 missing/expired, -1 no TTL, else remaining seconds **rounded to
    /// nearest** (`(ms + 500) / 1000`), which is what Redis does (SPEC 4.1).
    pub fn ttl(&mut self, key: &[u8], now: u64) -> i64 {
        match self.live(key, now) {
            None => -2,
            Some(e) => match e.expires_at {
                None => -1,
                Some(t) => ((t - now + 500) / 1000) as i64,
            },
        }
    }

    /// `EXPIRE`/`PEXPIREAT`: set an absolute deadline. A deadline at or before
    /// `now` deletes the key (Redis: "already expired"). False if no such live key.
    pub fn expire_at(&mut self, key: &[u8], at_ms: i64, now: u64) -> bool {
        if self.live(key, now).is_none() {
            return false;
        }
        if at_ms <= now as i64 {
            self.remove_entry(key);
            self.log(&[b"DEL", key]);
        } else {
            self.set_expiry(key, Some(at_ms as u64));
            let at = at_ms.to_string();
            self.log(&[b"PEXPIREAT", key, at.as_bytes()]);
        }
        true
    }

    /// `INCR`: creates `"1"` if missing, preserves the TTL otherwise.
    pub fn incr(&mut self, key: &[u8], now: u64) -> Result<i64, StoreError> {
        let new = match self.live(key, now) {
            None => {
                self.insert_entry(key, Value::Str(Bytes::from_static(b"1")), None);
                1
            }
            Some(entry) => match &mut entry.value {
                Value::Hash(_) => return Err(StoreError::WrongType),
                Value::Str(s) => {
                    let cur = parse_i64(s).ok_or(StoreError::NotInteger)?;
                    let new = cur.checked_add(1).ok_or(StoreError::Overflow)?;
                    *s = Bytes::from(new.to_string());
                    new
                }
            },
        };
        self.log(&[b"INCR", key]);
        Ok(new)
    }

    // ---- hashes ----

    /// `HSET key f v [f v ...]` (`pairs` is the flat `[f, v, ...]` list, even length).
    /// Returns the number of **new** fields. Preserves any TTL.
    pub fn hset(&mut self, key: &[u8], pairs: &[&[u8]], now: u64) -> Result<usize, StoreError> {
        debug_assert!(!pairs.is_empty() && pairs.len().is_multiple_of(2));
        let added = match self.live(key, now) {
            Some(Entry { value: Value::Hash(h), .. }) => h.insert_all(pairs),
            Some(_) => return Err(StoreError::WrongType),
            None => {
                let mut h = HashValue::default();
                let added = h.insert_all(pairs);
                self.insert_entry(key, Value::Hash(h), None);
                added
            }
        };
        let mut rec: Vec<&[u8]> = Vec::with_capacity(2 + pairs.len());
        rec.push(b"HSET");
        rec.push(key);
        rec.extend_from_slice(pairs);
        self.log(&rec);
        Ok(added)
    }

    /// `HGET`.
    pub fn hget(&mut self, key: &[u8], field: &[u8], now: u64) -> Result<Option<Bytes>, StoreError> {
        match self.live(key, now) {
            None => Ok(None),
            Some(Entry { value: Value::Hash(h), .. }) => Ok(h.fields.get(field).map(|(_, v)| v.clone())),
            Some(_) => Err(StoreError::WrongType),
        }
    }

    /// `HDEL`. Deleting the last field deletes the key (as in Redis).
    pub fn hdel(&mut self, key: &[u8], fields: &[&[u8]], now: u64) -> Result<usize, StoreError> {
        let (removed, emptied) = match self.live(key, now) {
            None => return Ok(0),
            Some(Entry { value: Value::Hash(h), .. }) => {
                let removed = fields.iter().filter(|f| h.fields.remove(**f).is_some()).count();
                (removed, h.fields.is_empty())
            }
            Some(_) => return Err(StoreError::WrongType),
        };
        if emptied {
            self.remove_entry(key);
        }
        if removed > 0 {
            let mut rec: Vec<&[u8]> = Vec::with_capacity(2 + fields.len());
            rec.push(b"HDEL");
            rec.push(key);
            rec.extend_from_slice(fields);
            self.log(&rec);
        }
        Ok(removed)
    }

    /// `HLEN`.
    pub fn hlen(&mut self, key: &[u8], now: u64) -> Result<usize, StoreError> {
        match self.live(key, now) {
            None => Ok(0),
            Some(Entry { value: Value::Hash(h), .. }) => Ok(h.fields.len()),
            Some(_) => Err(StoreError::WrongType),
        }
    }

    /// `HGETALL`: flat `[f, v, f, v, ...]` in insertion order. Copies out under the lock.
    pub fn hgetall(&mut self, key: &[u8], now: u64) -> Result<Vec<Bytes>, StoreError> {
        match self.live(key, now) {
            None => Ok(Vec::new()),
            Some(Entry { value: Value::Hash(h), .. }) => {
                let mut all: Vec<(u64, &Bytes, &Bytes)> = h.fields.iter().map(|(f, (seq, v))| (*seq, f, v)).collect();
                all.sort_unstable_by_key(|(seq, _, _)| *seq);
                let mut out = Vec::with_capacity(all.len() * 2);
                for (_, f, v) in all {
                    out.push(f.clone());
                    out.push(v.clone());
                }
                Ok(out)
            }
            Some(_) => Err(StoreError::WrongType),
        }
    }

    // ---- active expiry ----

    /// One sweep round: sample up to `samples` keys that have a TTL (uniformly,
    /// with replacement) and delete those that are expired.
    pub fn sweep(&mut self, now: u64, samples: usize) -> SweepStats {
        let mut stats = SweepStats::default();
        for _ in 0..samples {
            if self.ttl_keys.is_empty() {
                break;
            }
            let idx = (self.next_rand() % self.ttl_keys.len() as u64) as usize;
            stats.sampled += 1;
            let expired = match self.map.get(&*self.ttl_keys[idx]) {
                Some(e) => e.is_expired(now),
                None => unreachable!("ttl_keys entry without a map entry"),
            };
            if expired {
                let key = self.ttl_keys[idx].clone();
                self.remove_entry(&key);
                self.log(&[b"DEL", &key]);
                stats.deleted += 1;
            }
        }
        stats
    }

    /// Physical number of keys, expired-but-unreaped ones included.
    pub fn raw_len(&self) -> usize {
        self.map.len()
    }

    /// Number of keys carrying a TTL.
    pub fn ttl_len(&self) -> usize {
        self.ttl_keys.len()
    }

    /// Panics if the TTL index and the map disagree. Used by tests.
    pub fn check_invariants(&self) {
        let with_ttl = self.map.values().filter(|e| e.expires_at.is_some()).count();
        assert_eq!(self.ttl_keys.len(), with_ttl, "ttl_keys vs entries with a TTL");
        for (slot, key) in self.ttl_keys.iter().enumerate() {
            let entry = self.map.get(&**key).expect("ttl key present in map");
            assert!(entry.expires_at.is_some(), "ttl key without deadline");
            assert_eq!(entry.ttl_slot, slot, "stale ttl_slot");
        }
    }
}

/// The whole keyspace: N independently locked shards.
pub struct Store {
    shards: Box<[Mutex<Shard>]>,
    hasher: RandomState,
}

impl Store {
    /// Create a store with `n_shards` shards (at least 1).
    pub fn new(n_shards: usize) -> Store {
        let n = n_shards.max(1);
        let seeds = RandomState::new();
        let shards: Vec<Mutex<Shard>> = (0..n).map(|i| Mutex::new(Shard::new(seeds.hash_one(i as u64)))).collect();
        Store { shards: shards.into_boxed_slice(), hasher: RandomState::new() }
    }

    /// Give every shard a handle to the AOF writer. Done once, after replay and
    /// before the store is shared (needs `&mut self`).
    pub fn attach_aof(&mut self, sink: AofSink) {
        for shard in self.shards.iter_mut() {
            shard.get_mut().unwrap_or_else(PoisonError::into_inner).aof = Some(sink.clone());
        }
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard_index(&self, key: &[u8]) -> usize {
        (self.hasher.hash_one(key) % self.shards.len() as u64) as usize
    }

    /// Run `f` with exclusive access to the shard owning `key`.
    ///
    /// This is a plain (non-async) function and `f` is `FnOnce`, so the lock is
    /// taken and released inside one synchronous call: it can never be held
    /// across an `.await`, and it never nests with another shard lock.
    ///
    /// Poisoning is ignored on purpose: a panic in a handler must not turn one
    /// bug into a permanently dead shard. (Every mutation updates `map` and
    /// `ttl_keys` through the small primitives above; none of them panics
    /// between the two updates.)
    pub fn locked<R>(&self, key: &[u8], f: impl FnOnce(&mut Shard) -> R) -> R {
        let mut guard = self.shards[self.shard_index(key)].lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut guard)
    }

    /// One full sweeper tick: for each shard, rounds of sampling while more than
    /// 25 % of the sample was expired (Redis' adaptive rule), capped per shard.
    /// Locks one shard at a time, one round at a time.
    pub fn sweep_cycle(&self, now: u64) -> SweepStats {
        let mut total = SweepStats::default();
        for shard in self.shards.iter() {
            for _ in 0..MAX_ROUNDS_PER_SHARD {
                let stats = shard.lock().unwrap_or_else(PoisonError::into_inner).sweep(now, SAMPLES_PER_ROUND);
                total.sampled += stats.sampled;
                total.deleted += stats.deleted;
                if stats.sampled == 0 || stats.deleted * 4 <= stats.sampled {
                    break;
                }
            }
        }
        total
    }

    /// Physical key count including expired-but-unreaped keys (tests only; there is no `DBSIZE`).
    pub fn raw_len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap_or_else(PoisonError::into_inner).raw_len()).sum()
    }

    /// Number of keys with a TTL across all shards.
    pub fn ttl_len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap_or_else(PoisonError::into_inner).ttl_len()).sum()
    }

    /// Run every shard's invariant check (tests).
    pub fn check_invariants(&self) {
        for s in self.shards.iter() {
            s.lock().unwrap_or_else(PoisonError::into_inner).check_invariants();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_index_stays_consistent_through_mixed_operations() {
        let mut sh = Shard::new(7);
        for i in 0..200u32 {
            let key = format!("k{i}");
            sh.set(key.as_bytes(), b"v", if i % 2 == 0 { Some(1_000 + u64::from(i)) } else { None });
        }
        sh.check_invariants();
        for i in 0..200u32 {
            let key = format!("k{i}");
            match i % 5 {
                0 => {
                    sh.del(key.as_bytes(), 0);
                }
                1 => {
                    sh.expire_at(key.as_bytes(), 5_000, 0);
                }
                2 => sh.set(key.as_bytes(), b"w", None),
                3 => {
                    sh.expire_at(key.as_bytes(), -1, 0);
                }
                _ => {
                    let _ = sh.incr(key.as_bytes(), 0);
                }
            }
            sh.check_invariants();
        }
    }

    #[test]
    fn sweep_removes_only_expired_keys() {
        let mut sh = Shard::new(3);
        sh.set(b"old", b"v", Some(10));
        sh.set(b"new", b"v", Some(10_000));
        sh.set(b"forever", b"v", None);
        let stats = sh.sweep(1_000, 50);
        assert_eq!(stats.deleted, 1);
        assert_eq!(sh.raw_len(), 2);
        sh.check_invariants();
    }
}
