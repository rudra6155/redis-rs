//! `redis-rs` binary: flag parsing and start-up order.
//!
//! Start-up order matters: open + replay the AOF *before* binding the listener,
//! so nothing can connect to a half-loaded database.

#![forbid(unsafe_code)]
#![deny(clippy::await_holding_lock)]

use redis_rs::aof::{self, Aof, FsyncPolicy, ReplayError};
use redis_rs::server::{Server, ServerConfig};
use redis_rs::signal::ShutdownSignals;
use redis_rs::store::{Store, DEFAULT_SHARDS};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const USAGE: &str = "\
redis-rs - a RESP2 Redis subset

USAGE: redis-rs [FLAGS]    (each flag also accepts --flag=value)

  --bind <addr>              listen address            [127.0.0.1]
  --port <port>              listen port, 0 = any      [6379]
  --appendonly yes|no        enable the AOF            [no]
  --appendfsync always|everysec|no                     [everysec]
  --appendfilename <name>    AOF file name             [appendonly.aof]
  --dir <path>               directory for the AOF     [.]
  --sweeper yes|no           background expiry sweeper [yes]
  --shards <n>               keyspace shards           [64]
  --threads <n>              tokio worker threads      [number of CPUs]
  --maxclients <n>           connection limit          [10000]
  --drain-timeout-ms <ms>    graceful shutdown limit   [5000]
  --version                  print the version
  --help                     this text
";

struct Config {
    bind: String,
    port: u16,
    appendonly: bool,
    appendfsync: FsyncPolicy,
    appendfilename: String,
    dir: PathBuf,
    sweeper: bool,
    shards: usize,
    threads: Option<usize>,
    max_clients: usize,
    drain_timeout: Duration,
}

fn parse_bool(flag: &str, v: &str) -> Result<bool, String> {
    match v.to_ascii_lowercase().as_str() {
        "yes" | "true" | "on" | "1" => Ok(true),
        "no" | "false" | "off" | "0" => Ok(false),
        _ => Err(format!("{flag}: expected yes|no, got '{v}'")),
    }
}

fn parse_num<T: std::str::FromStr>(flag: &str, v: &str) -> Result<T, String> {
    v.parse().map_err(|_| format!("{flag}: invalid number '{v}'"))
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Option<Config>, String> {
        let mut cfg = Config {
            bind: "127.0.0.1".into(),
            port: 6379,
            appendonly: false,
            appendfsync: FsyncPolicy::Everysec,
            appendfilename: "appendonly.aof".into(),
            dir: PathBuf::from("."),
            sweeper: true,
            shards: DEFAULT_SHARDS,
            threads: None,
            max_clients: 10_000,
            drain_timeout: Duration::from_secs(5),
        };
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                return Ok(None);
            }
            if arg == "--version" || arg == "-V" {
                println!("redis-rs {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            let (flag, inline) = match arg.split_once('=') {
                Some((f, v)) => (f.to_string(), Some(v.to_string())),
                None => (arg, None),
            };
            let value = match inline {
                Some(v) => v,
                None => args.next().ok_or_else(|| format!("{flag}: missing value"))?,
            };
            match flag.as_str() {
                "--bind" => cfg.bind = value,
                "--port" => cfg.port = parse_num(&flag, &value)?,
                "--appendonly" => cfg.appendonly = parse_bool(&flag, &value)?,
                "--appendfsync" => cfg.appendfsync = value.parse()?,
                "--appendfilename" => cfg.appendfilename = value,
                "--dir" => cfg.dir = PathBuf::from(value),
                "--sweeper" => cfg.sweeper = parse_bool(&flag, &value)?,
                "--shards" => cfg.shards = parse_num::<usize>(&flag, &value)?.max(1),
                "--threads" => cfg.threads = Some(parse_num::<usize>(&flag, &value)?.max(1)),
                "--maxclients" => cfg.max_clients = parse_num(&flag, &value)?,
                "--drain-timeout-ms" => cfg.drain_timeout = Duration::from_millis(parse_num(&flag, &value)?),
                other => return Err(format!("unknown flag '{other}'")),
            }
        }
        Ok(Some(cfg))
    }
}

fn main() -> ExitCode {
    let cfg = match Config::parse(std::env::args().skip(1)) {
        Ok(Some(cfg)) => cfg,
        Ok(None) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    // Build keyspace, replay the AOF (may truncate a torn tail), then start the writer.
    let mut store = Store::new(cfg.shards);
    let aof = if cfg.appendonly {
        let path = cfg.dir.join(&cfg.appendfilename);
        match aof::replay(&path, &store) {
            Ok(outcome) => eprintln!(
                "AOF: replayed {} commands ({} bytes) from {}{}",
                outcome.commands,
                outcome.bytes,
                path.display(),
                if outcome.truncation.is_some() { ", dropped a torn final command" } else { "" }
            ),
            Err(e @ ReplayError::Corrupt { .. }) => {
                eprintln!("FATAL: {e}");
                return ExitCode::from(1);
            }
            Err(e) => {
                eprintln!("FATAL: cannot load AOF {}: {e}", path.display());
                return ExitCode::from(1);
            }
        }
        match Aof::open(&path, cfg.appendfsync) {
            Ok(aof) => {
                store.attach_aof(aof.sink());
                Some(aof)
            }
            Err(e) => {
                eprintln!("FATAL: cannot open AOF {} for append: {e}", path.display());
                return ExitCode::from(1);
            }
        }
    } else {
        None
    };

    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    if let Some(n) = cfg.threads {
        rt.worker_threads(n);
    }
    let rt = match rt.build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("FATAL: cannot start runtime: {e}");
            return ExitCode::from(1);
        }
    };

    let server_config =
        ServerConfig { max_clients: cfg.max_clients, drain_timeout: cfg.drain_timeout, sweeper: cfg.sweeper };
    let server = Server::new(Arc::new(store), aof, server_config);

    let result: Result<(), String> = rt.block_on(async move {
        // Handlers first, "Ready" second: see ShutdownSignals.
        let signals = match ShutdownSignals::install() {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!("warning: cannot install signal handlers ({e}); only SIGKILL will stop the server");
                None
            }
        };
        let listener = TcpListener::bind((cfg.bind.as_str(), cfg.port))
            .await
            .map_err(|e| format!("cannot bind {}:{}: {e}", cfg.bind, cfg.port))?;
        let addr = listener.local_addr().map_err(|e| e.to_string())?;
        // Tests and scripts parse this line to learn the port when started with --port 0.
        println!("Ready to accept connections on {addr}");
        // stdout is block-buffered when piped; make sure the line is out.
        use std::io::Write;
        let _ = std::io::stdout().flush();

        let shutdown = async move {
            match signals {
                Some(mut signals) => {
                    let name = signals.recv().await;
                    eprintln!("received {name}, shutting down gracefully");
                    signals.force_exit_on_next();
                }
                None => std::future::pending::<()>().await,
            }
        };
        server.run(listener, shutdown).await.map_err(|e| e.to_string())
    });

    match result {
        Ok(()) => {
            eprintln!("shutdown complete");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("FATAL: {e}");
            ExitCode::from(1)
        }
    }
}
