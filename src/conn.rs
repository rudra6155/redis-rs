//! One client connection: read, execute, reply.
//!
//! ```text
//!   loop {
//!       process()   // sync: decode every complete request in the buffer, run it, encode replies
//!       barrier     // `appendfsync always` only: wait until the AOF has fsynced our writes
//!       write_all   // one write for the whole pipelined batch
//!       read_buf    // or: shutdown signal -> close
//!   }
//! ```
//!
//! `process` is a synchronous function. All lock guards (shard locks) live and
//! die inside it, so none can be held across the `.await`s in [`handle`].

use crate::aof::AofSink;
use crate::command::{execute, Ctx};
use crate::resp::{parse_request, Frame};
use crate::store::Store;
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

const READ_CHUNK: usize = 16 * 1024;
/// Flush replies once this many bytes are pending, so a client that pipelines a
/// million commands cannot make us buffer a million replies.
const OUT_FLUSH_THRESHOLD: usize = 64 * 1024;
/// Redis' `client-query-buffer-limit` default: close the connection beyond this.
const MAX_QUERY_BUF: usize = 1 << 30;

/// State shared by every connection.
pub struct ConnShared {
    /// The keyspace.
    pub store: Arc<Store>,
    /// Present only under `appendfsync always`: connections wait on it before replying to writes.
    pub barrier: Option<AofSink>,
}

struct Batch {
    consumed: usize,
    wrote: bool,
    /// Protocol error: flush the error reply, then close.
    close: bool,
    /// Stopped early because the output buffer is full; more complete requests may be buffered.
    more: bool,
}

/// Execute every complete request in `input`, appending replies to `out`.
fn process(store: &Store, input: &[u8], out: &mut Vec<u8>) -> Batch {
    let mut ctx = Ctx::new(store);
    let mut pos = 0;
    let (mut close, mut more) = (false, false);
    loop {
        match parse_request(&input[pos..]) {
            Ok(Some((args, used))) => {
                pos += used;
                // `*0` / `*-1`: Redis skips these without replying.
                if !args.is_empty() {
                    execute(&mut ctx, &args).encode(out);
                }
                if out.len() >= OUT_FLUSH_THRESHOLD {
                    more = true;
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                Frame::error(format!("ERR {}", e.message())).encode(out);
                close = true;
                break;
            }
        }
    }
    Batch { consumed: pos, wrote: ctx.take_wrote(), close, more }
}

/// Serve one connection until the peer closes, a protocol error occurs, or shutdown is signalled.
///
/// On shutdown an *idle* connection is closed at once; a connection in the middle
/// of a batch finishes that batch (including its AOF barrier) and then closes.
pub async fn handle(mut stream: TcpStream, shared: Arc<ConnShared>, mut shutdown: watch::Receiver<bool>) {
    let _ = stream.set_nodelay(true);
    let mut inbuf = BytesMut::with_capacity(READ_CHUNK);
    let mut out: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    loop {
        let batch = process(&shared.store, &inbuf, &mut out);
        inbuf.advance(batch.consumed);

        if batch.wrote {
            if let Some(sink) = &shared.barrier {
                if let Some(done) = sink.barrier() {
                    let _ = done.await;
                }
            }
        }
        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() {
                return;
            }
            out.clear();
        }
        if batch.close {
            let _ = stream.shutdown().await;
            return;
        }
        if batch.more {
            continue;
        }
        if *shutdown.borrow() {
            return;
        }

        inbuf.reserve(READ_CHUNK);
        tokio::select! {
            read = stream.read_buf(&mut inbuf) => match read {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            },
            _ = shutdown.changed() => return,
        }
        if inbuf.len() > MAX_QUERY_BUF {
            let mut err = Vec::new();
            Frame::error("ERR Protocol error: query buffer limit exceeded").encode(&mut err);
            let _ = stream.write_all(&err).await;
            return;
        }
    }
}
