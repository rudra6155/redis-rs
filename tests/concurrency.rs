//! Concurrency contract (SPEC.md section 7).
//!
//! The server runs in-process on a multi-threaded tokio runtime and is driven over
//! real TCP connections. Every test is wrapped in a 30 s deadlock guard: on
//! timeout it writes `results/deadlock_report.txt` (backtrace + what was running)
//! and fails, instead of hanging CI.

mod common;

use bytes::{Bytes, BytesMut};
use common::results_dir;
use redis_rs::resp::Frame;
use redis_rs::server::{Server, ServerConfig};
use redis_rs::store::Store;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Run `fut`; if it takes longer than 30 s assume a deadlock, write the report, fail.
async fn guard<T>(what: &str, fut: impl Future<Output = T>) -> T {
    match tokio::time::timeout(DEADLOCK_TIMEOUT, fut).await {
        Ok(v) => v,
        Err(_) => {
            let report = format!(
                "deadlock suspected: `{what}` did not finish within {DEADLOCK_TIMEOUT:?}\n\
                 command that hung: {what}\n\
                 RUST_BACKTRACE of the waiting test thread:\n{}\n\
                 (shard locks are never held across an await, so a hang here points at the AOF queue, \
                 the sweeper, or a lost wake-up; re-run with RUST_BACKTRACE=1 and attach gdb/`rust-gdb` \
                 to the test binary for all-thread stacks)\n",
                std::backtrace::Backtrace::force_capture()
            );
            let _ = std::fs::create_dir_all(results_dir());
            let _ = std::fs::write(results_dir().join("deadlock_report.txt"), &report);
            panic!("{report}");
        }
    }
}

struct Harness {
    addr: std::net::SocketAddr,
    store: Arc<Store>,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl Harness {
    async fn start(shards: usize) -> Harness {
        let store = Arc::new(Store::new(shards));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let server = Server::new(store.clone(), None, ServerConfig::default());
        let join = tokio::spawn(async move {
            server
                .run(listener, async {
                    let _ = rx.await;
                })
                .await
        });
        Harness { addr, store, shutdown: Some(tx), join: Some(join) }
    }

    async fn stop(mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        self.join.take().unwrap().await.unwrap().unwrap();
    }
}

struct AClient {
    stream: TcpStream,
    buf: BytesMut,
}

impl AClient {
    async fn connect(addr: std::net::SocketAddr) -> AClient {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        AClient { stream, buf: BytesMut::new() }
    }

    async fn cmd(&mut self, args: &[&str]) -> Frame {
        self.stream.write_all(&Frame::command(args).to_vec()).await.unwrap();
        loop {
            if let Some((frame, used)) = Frame::parse(&self.buf).unwrap() {
                let _ = self.buf.split_to(used);
                return frame;
            }
            let n = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(n > 0, "server closed the connection");
        }
    }
}

fn as_int(f: &Frame) -> i64 {
    match f {
        Frame::Integer(n) => *n,
        other => panic!("expected integer, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifty_tasks_times_1000_incr_on_one_key_is_exactly_50000() {
    guard("50 tasks x 1000 INCR counter", async {
        let h = Harness::start(64).await;
        let mut tasks = Vec::new();
        for _ in 0..50 {
            let addr = h.addr;
            tasks.push(tokio::spawn(async move {
                let mut c = AClient::connect(addr).await;
                let mut seen = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    seen.push(as_int(&c.cmd(&["INCR", "counter"]).await));
                }
                seen
            }));
        }
        let mut all: Vec<i64> = Vec::with_capacity(50_000);
        for t in tasks {
            let seen = t.await.unwrap();
            // each task observes a strictly increasing sequence
            assert!(seen.windows(2).all(|w| w[0] < w[1]));
            all.extend(seen);
        }
        // Atomicity, not just the final total: every value 1..=50000 was handed out exactly once.
        all.sort_unstable();
        assert!(all.iter().copied().eq(1..=50_000), "lost or duplicated INCR results");

        let mut c = AClient::connect(h.addr).await;
        assert_eq!(c.cmd(&["GET", "counter"]).await, Frame::Bulk(Bytes::from_static(b"50000")));
        h.store.check_invariants();
        h.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_tasks_on_distinct_keys_see_no_cross_talk_and_no_deadlock() {
    guard("20 tasks SET/GET distinct keys", async {
        let h = Harness::start(64).await;
        let mut tasks = Vec::new();
        for t in 0..20 {
            let addr = h.addr;
            tasks.push(tokio::spawn(async move {
                let mut c = AClient::connect(addr).await;
                let key = format!("task:{t}");
                let hash = format!("hash:{t}");
                for round in 0..500 {
                    let value = format!("value-of-task-{t}-round-{round}");
                    assert_eq!(c.cmd(&["SET", &key, &value]).await, Frame::ok());
                    assert_eq!(
                        c.cmd(&["GET", &key]).await,
                        Frame::Bulk(Bytes::from(value.clone())),
                        "task {t} read someone else's value"
                    );
                    assert_eq!(
                        c.cmd(&["HSET", &hash, "owner", &t.to_string()]).await,
                        Frame::Integer(i64::from(round == 0))
                    );
                    assert_eq!(c.cmd(&["HGET", &hash, "owner"]).await, Frame::Bulk(Bytes::from(t.to_string())));
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        h.store.check_invariants();
        h.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_commands_over_shared_keys_with_the_sweeper_running_do_not_deadlock() {
    guard("mixed workload + sweeper", async {
        let h = Harness::start(8).await; // few shards on purpose: maximise contention
        let started = std::time::Instant::now();
        let mut tasks = Vec::new();
        for t in 0..32u64 {
            let addr = h.addr;
            tasks.push(tokio::spawn(async move {
                let mut c = AClient::connect(addr).await;
                let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (t + 1).wrapping_mul(0xA24B_AED4_963E_E407);
                let mut ops = 0u64;
                while started.elapsed() < Duration::from_millis(2500) {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let key = format!("shared:{}", rng % 40);
                    let field = format!("f{}", (rng >> 8) % 4);
                    let reply = match (rng >> 16) % 11 {
                        0 => c.cmd(&["SET", &key, "5", "PX", "40"]).await,
                        1 => c.cmd(&["SET", &key, "7"]).await,
                        2 => c.cmd(&["GET", &key]).await,
                        3 => c.cmd(&["DEL", &key]).await,
                        4 => c.cmd(&["EXPIRE", &key, "1"]).await,
                        5 => c.cmd(&["INCR", &key]).await,
                        6 => c.cmd(&["HSET", &key, &field, "v"]).await,
                        7 => c.cmd(&["HDEL", &key, &field]).await,
                        8 => c.cmd(&["HGETALL", &key]).await,
                        9 => c.cmd(&["TTL", &key]).await,
                        _ => c.cmd(&["EXISTS", &key]).await,
                    };
                    // WRONGTYPE / not-an-integer are legitimate outcomes of this chaos; a hang or
                    // a broken connection is not (cmd() would have panicked).
                    if let Frame::Error(e) = &reply {
                        let e = String::from_utf8_lossy(e);
                        assert!(e.starts_with("WRONGTYPE") || e.starts_with("ERR value is not an integer"), "{e}");
                    }
                    ops += 1;
                }
                ops
            }));
        }
        let mut total = 0;
        for t in tasks {
            total += t.await.unwrap();
        }
        assert!(total > 1000, "suspiciously few operations completed: {total}");
        h.store.check_invariants();
        h.stop().await;
    })
    .await;
}

/// Library level, no network: 16 OS threads, one hot key and many cold ones.
#[test]
fn raw_threads_hammering_the_store_directly_lose_no_updates() {
    let store = Arc::new(Store::new(64));
    let hot_total = 16 * 10_000;
    let handles: Vec<_> = (0..16)
        .map(|t| {
            let store = store.clone();
            std::thread::spawn(move || {
                for i in 0..10_000 {
                    store.locked(b"hot", |sh| sh.incr(b"hot", 0)).unwrap();
                    let cold = format!("cold:{}", (t * 10_000 + i) % 500);
                    store.locked(cold.as_bytes(), |sh| sh.set(cold.as_bytes(), b"x", None));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(store.locked(b"hot", |sh| sh.get(b"hot", 0)).unwrap().unwrap(), Bytes::from(hot_total.to_string()));
    store.check_invariants();
}
