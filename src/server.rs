//! Accept loop and graceful shutdown.
//!
//! Shutdown order (each step finishes before the next starts):
//! 1. the listener is dropped - no new connections;
//! 2. connections are told to stop; each finishes the batch it is executing and closes;
//! 3. wait for them (bounded by `drain_timeout`, then abort the stragglers);
//! 4. stop the sweeper;
//! 5. drain the AOF queue, `fsync`, join the writer threads.
//!
//! Step 5 comes last on purpose: only after every connection is gone can nothing
//! enqueue another record, so "flush the AOF" really means flush everything.

use crate::aof::{Aof, FsyncPolicy};
use crate::conn::{self, ConnShared};
use crate::expiry::Sweeper;
use crate::store::Store;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

/// Tunables for [`Server`].
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Connections beyond this get `-ERR max number of clients reached` (Redis `maxclients`).
    pub max_clients: usize,
    /// How long shutdown waits for connections before aborting them.
    pub drain_timeout: Duration,
    /// Run the background sweeper (tests turn it off to prove lazy expiry on its own).
    pub sweeper: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig { max_clients: 10_000, drain_timeout: Duration::from_secs(5), sweeper: true }
    }
}

/// A server instance: keyspace, optional AOF, config.
pub struct Server {
    store: Arc<Store>,
    aof: Option<Aof>,
    config: ServerConfig,
}

impl Server {
    /// `store` must already have the AOF sink attached if `aof` is `Some`.
    pub fn new(store: Arc<Store>, aof: Option<Aof>, config: ServerConfig) -> Server {
        Server { store, aof, config }
    }

    /// Serve on `listener` until `shutdown` resolves, then shut down gracefully.
    pub async fn run(self, listener: TcpListener, shutdown: impl Future<Output = ()>) -> io::Result<()> {
        let Server { store, aof, config } = self;
        let barrier = aof.as_ref().filter(|a| a.policy() == FsyncPolicy::Always).map(Aof::sink);
        let shared = Arc::new(ConnShared { store: store.clone(), barrier });
        let sweeper = config.sweeper.then(|| Sweeper::spawn(store.clone()));

        let (stop_tx, stop_rx) = watch::channel(false);
        let mut conns: JoinSet<()> = JoinSet::new();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _peer)) => {
                        if conns.len() >= config.max_clients {
                            // Best effort, like Redis: tell the client why, then drop it.
                            let _ = stream.try_write(b"-ERR max number of clients reached\r\n");
                        } else {
                            conns.spawn(conn::handle(stream, shared.clone(), stop_rx.clone()));
                        }
                    }
                    Err(e) => {
                        // EMFILE and friends: log, back off, keep serving existing clients.
                        eprintln!("accept error: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                Some(res) = conns.join_next(), if !conns.is_empty() => {
                    if let Err(e) = res {
                        if e.is_panic() {
                            eprintln!("connection task panicked: {e}");
                        }
                    }
                }
            }
        }

        // 1. stop accepting
        drop(listener);
        // 2. tell connections to finish up
        let _ = stop_tx.send(true);
        // 3. wait for them
        let drain = async { while conns.join_next().await.is_some() {} };
        if tokio::time::timeout(config.drain_timeout, drain).await.is_err() {
            eprintln!("drain timeout: aborting {} connection(s)", conns.len());
            conns.abort_all();
            while conns.join_next().await.is_some() {}
        }
        // 4 + 5. stop the sweeper, then flush the AOF (blocking calls: keep them off the reactor)
        let finish = tokio::task::spawn_blocking(move || {
            if let Some(sweeper) = sweeper {
                sweeper.stop();
            }
            if let Some(mut aof) = aof {
                aof.shutdown();
            }
        });
        let _ = finish.await;
        Ok(())
    }
}
