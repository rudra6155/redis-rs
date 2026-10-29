//! Shared helpers for the integration tests: spawn the real `redis-rs` binary,
//! talk RESP to it, and (for the oracle tests) start a real `redis:7`.
#![allow(dead_code)]

use redis_rs::resp::Frame;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub const SERVER_BIN: &str = env!("CARGO_BIN_EXE_redis-rs");
const READ_TIMEOUT: Duration = Duration::from_secs(10);

pub fn results_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("results")
}

/// A running `redis-rs` child process.
pub struct TestServer {
    child: Option<Child>,
    pub addr: SocketAddr,
    pub stderr_path: PathBuf,
}

pub struct StartFailure {
    pub status: Option<ExitStatus>,
    pub stderr: String,
}

impl TestServer {
    /// Start with extra flags; `log_dir` receives `server-<n>.stderr`.
    pub fn start(args: &[&str], log_dir: &Path) -> TestServer {
        match TestServer::try_start(args, log_dir) {
            Ok(s) => s,
            Err(f) => panic!("server failed to start (status {:?}); stderr:\n{}", f.status, f.stderr),
        }
    }

    pub fn try_start(args: &[&str], log_dir: &Path) -> Result<TestServer, StartFailure> {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stderr_path = log_dir.join(format!("server-{}-{n}.stderr", std::process::id()));
        let stderr_file = File::create(&stderr_path).expect("create stderr log");

        let mut child = Command::new(SERVER_BIN)
            .args(["--bind", "127.0.0.1", "--port", "0"])
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr_file))
            .spawn()
            .expect("spawn redis-rs");

        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, rx) = mpsc::channel::<Option<SocketAddr>>();
        std::thread::spawn(move || {
            let mut sent = false;
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if !sent {
                    if let Some(addr) = line.strip_prefix("Ready to accept connections on ") {
                        let _ = tx.send(addr.trim().parse().ok());
                        sent = true;
                    }
                }
            }
            if !sent {
                let _ = tx.send(None); // EOF before readiness: the process exited
            }
        });

        let stderr_text = |p: &Path| std::fs::read_to_string(p).unwrap_or_default();
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Some(addr)) => Ok(TestServer { child: Some(child), addr, stderr_path }),
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let status = child.wait().ok();
                Err(StartFailure { status, stderr: stderr_text(&stderr_path) })
            }
        }
    }

    pub fn client(&self) -> Client {
        Client::connect(self.addr)
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("running").id()
    }

    /// `kill -9`.
    pub fn kill9(&mut self) {
        let mut child = self.child.take().expect("running");
        child.kill().expect("SIGKILL");
        let status = child.wait().expect("wait");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9), "expected death by SIGKILL, got {status:?}");
        }
        let _ = status;
    }

    #[cfg(unix)]
    pub fn signal(&self, sig: i32) {
        let rc = unsafe { libc::kill(self.pid() as i32, sig) };
        assert_eq!(rc, 0, "kill({sig}) failed");
    }

    /// Wait for the process to exit on its own.
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let child = self.child.as_mut().expect("running");
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                self.child = None;
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Blocking RESP client with an internal buffer (so partial and pipelined replies work).
pub struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    pub fn connect<A: ToSocketAddrs + std::fmt::Debug>(addr: A) -> Client {
        let stream = TcpStream::connect(&addr).unwrap_or_else(|e| panic!("connect {addr:?}: {e}"));
        stream.set_nodelay(true).unwrap();
        stream.set_read_timeout(Some(READ_TIMEOUT)).unwrap();
        Client { stream, buf: Vec::new() }
    }

    pub fn send_raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).expect("write");
    }

    /// A second handle on the same socket, for a writer thread while this one reads.
    pub fn try_clone_for_write(&self) -> Client {
        Client { stream: self.stream.try_clone().expect("clone socket"), buf: Vec::new() }
    }

    pub fn send_cmd<T: AsRef<[u8]>>(&mut self, args: &[T]) {
        let bytes = Frame::command(args).to_vec();
        self.send_raw(&bytes);
    }

    fn fill(&mut self) -> bool {
        let mut tmp = [0u8; 16 * 1024];
        match self.stream.read(&mut tmp) {
            Ok(0) => false,
            Ok(n) => {
                self.buf.extend_from_slice(&tmp[..n]);
                true
            }
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
                panic!("timed out waiting for a reply; buffered so far: {:?}", String::from_utf8_lossy(&self.buf))
            }
            Err(_) => false, // reset by peer counts as closed
        }
    }

    /// Read exactly one frame.
    pub fn read_frame(&mut self) -> Frame {
        loop {
            match Frame::parse(&self.buf) {
                Ok(Some((frame, n))) => {
                    self.buf.drain(..n);
                    return frame;
                }
                Ok(None) => {
                    if !self.fill() {
                        panic!("connection closed mid-reply; buffered: {:?}", String::from_utf8_lossy(&self.buf));
                    }
                }
                Err(e) => panic!("protocol error from server: {e}; buffered: {:?}", String::from_utf8_lossy(&self.buf)),
            }
        }
    }

    /// Read one frame and return its exact wire bytes.
    pub fn read_raw_frame(&mut self) -> Vec<u8> {
        loop {
            match Frame::parse(&self.buf) {
                Ok(Some((_, n))) => return self.buf.drain(..n).collect(),
                Ok(None) => {
                    if !self.fill() {
                        panic!("connection closed mid-reply; buffered: {:?}", String::from_utf8_lossy(&self.buf));
                    }
                }
                Err(e) => panic!("protocol error from server: {e}"),
            }
        }
    }

    /// Everything the peer sends until it closes the connection (or `timeout` passes).
    pub fn read_to_close(&mut self, timeout: Duration) -> Vec<u8> {
        self.stream.set_read_timeout(Some(timeout)).unwrap();
        let mut out = std::mem::take(&mut self.buf);
        let mut tmp = [0u8; 4096];
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&tmp[..n]),
                Err(_) => break,
            }
        }
        out
    }

    pub fn cmd<T: AsRef<[u8]>>(&mut self, args: &[T]) -> Frame {
        self.send_cmd(args);
        self.read_frame()
    }

    pub fn raw_cmd<T: AsRef<[u8]>>(&mut self, args: &[T]) -> Vec<u8> {
        self.send_cmd(args);
        self.read_raw_frame()
    }
}

// ---- frame builders for assertions ----

pub fn bulk(s: &str) -> Frame {
    Frame::bulk(bytes::Bytes::copy_from_slice(s.as_bytes()))
}

pub fn err(s: &str) -> Frame {
    Frame::error(bytes::Bytes::copy_from_slice(s.as_bytes()))
}

pub fn int(n: i64) -> Frame {
    Frame::Integer(n)
}

pub fn ok() -> Frame {
    Frame::ok()
}

pub fn array_of_bulks(items: &[&str]) -> Frame {
    Frame::Array(items.iter().map(|s| bulk(s)).collect())
}

/// Printable form of wire bytes: `\r` and `\n` visible, non-ASCII hex-escaped.
pub fn escape(bytes: &[u8]) -> String {
    let mut s = String::new();
    for &b in bytes {
        match b {
            b'\r' => s.push_str("\\r"),
            b'\n' => s.push_str("\\n"),
            b'\\' => s.push_str("\\\\"),
            0x20..=0x7e => s.push(b as char),
            _ => s.push_str(&format!("\\x{b:02x}")),
        }
    }
    s
}

// ---- the oracle: a real redis-server ----

/// A `docker run`-started container that is removed when dropped, including when a panic unwinds
/// through a half-finished `Oracle::start` (a container must never outlive a failed start-up).
struct Container(String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

pub struct Oracle {
    pub addr: String,
    pub version: String,
    pub source: String,
    _container: Option<Container>,
}

impl Oracle {
    /// Use `ORACLE_ADDR=host:port` if set (a *disposable* redis:7; it gets FLUSHALL),
    /// otherwise start `redis:7` with Docker. Fails loudly if neither is possible:
    /// an oracle test that silently skips would make "matches Redis" unfalsifiable.
    pub fn start() -> Oracle {
        let (addr, container, source) = match std::env::var("ORACLE_ADDR") {
            Ok(addr) if !addr.is_empty() => {
                wait_for_ping(std::slice::from_ref(&addr));
                (addr.clone(), None, format!("ORACLE_ADDR={addr}"))
            }
            _ => {
                let (container, candidates) = start_docker_redis();
                let addr = wait_for_ping(&candidates);
                (addr.clone(), Some(container), format!("docker run redis:7 ({addr})"))
            }
        };
        let mut c = Client::connect(addr.as_str());
        let flushed = c.cmd(&["FLUSHALL"]);
        assert_eq!(flushed, ok(), "oracle FLUSHALL");
        let info = match c.cmd(&["INFO", "server"]) {
            Frame::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
            other => panic!("INFO server: {other:?}"),
        };
        let version =
            info.lines().find_map(|l| l.strip_prefix("redis_version:")).unwrap_or("unknown").trim().to_string();
        Oracle { addr, version, source, _container: container }
    }

    pub fn client(&self) -> Client {
        Client::connect(self.addr.as_str())
    }
}

/// Start `redis:7` and return the guard plus every address it might be reachable on:
/// the published loopback port first (works for a process on the Docker host, on Linux,
/// macOS and Windows alike), then the container's bridge IP (works from another container
/// that shares the Docker host's network, where the published loopback port is not visible).
fn start_docker_redis() -> (Container, Vec<String>) {
    let run = Command::new("docker")
        .args(["run", "-d", "--rm", "-p", "127.0.0.1::6379", "redis:7"])
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "the oracle tests need Docker (could not run `docker`: {e}). Start a disposable redis:7 and set ORACLE_ADDR=host:port, or run `make test-nodocker`."
            )
        });
    assert!(run.status.success(), "`docker run redis:7` failed: {}", String::from_utf8_lossy(&run.stderr));
    let id = String::from_utf8_lossy(&run.stdout).trim().to_string();
    let container = Container(id.clone()); // from here on, any panic still removes the container

    let mut candidates = Vec::new();
    if let Ok(out) = Command::new("docker").args(["port", &id, "6379/tcp"]).output() {
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        candidates.extend(text.lines().map(str::trim).filter(|l| l.starts_with("127.0.0.1:")).map(String::from));
    }
    let fmt = "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}";
    if let Ok(out) = Command::new("docker").args(["inspect", "-f", fmt, &id]).output() {
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        candidates.extend(text.split_whitespace().map(|ip| format!("{ip}:6379")));
    }
    assert!(!candidates.is_empty(), "no reachable address found for the redis:7 container {id}");
    (container, candidates)
}

/// Wait until one of `addrs` answers PING; returns that address.
fn wait_for_ping(addrs: &[String]) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last: Vec<String> = vec!["not tried".into(); addrs.len()];
    loop {
        for (i, addr) in addrs.iter().enumerate() {
            last[i] = match ping_once(addr) {
                Ok(()) => return addr.clone(),
                Err(e) => e,
            };
        }
        assert!(
            Instant::now() < deadline,
            "oracle did not answer PING within 60 s; last error per candidate: {:?}",
            addrs.iter().zip(&last).collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn ping_once(addr: &str) -> Result<(), String> {
    let sock = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve: {e}"))?
        .next()
        .ok_or_else(|| "resolve: no address".to_string())?;
    let mut s = TcpStream::connect_timeout(&sock, Duration::from_millis(500)).map_err(|e| format!("connect: {e}"))?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok();
    s.write_all(b"*1\r\n$4\r\nPING\r\n").map_err(|e| format!("write: {e}"))?;
    let mut buf = [0u8; 16];
    let n = s.read(&mut buf).map_err(|e| format!("read: {e}"))?;
    if buf[..n].starts_with(b"+PONG") {
        Ok(())
    } else {
        Err(format!("unexpected reply {:?}", String::from_utf8_lossy(&buf[..n])))
    }
}

/// Unix seconds as `YYYY-MM-DDTHH:MM:SSZ` (no chrono dependency).
pub fn utc_iso(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // civil-from-days (Howard Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, (rem % 3600) / 60, rem % 60)
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}
