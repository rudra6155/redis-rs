//! Microbenchmarks with no I/O: they isolate the cost of the RESP parser (and,
//! separately, the store and the full command path) from network cost.
//!
//! Run: `cargo bench --bench ops` (or `make bench-parser`, which tees the
//! numbers into `results/parser_bench.txt`).

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use redis_rs::command::{execute, Ctx};
use redis_rs::resp::{encode_request, parse_request, Frame};
use redis_rs::store::Store;

fn request(args: &[&[u8]]) -> Vec<u8> {
    let mut v = Vec::new();
    encode_request(args, &mut v);
    v
}

fn bench_parser(c: &mut Criterion) {
    let set = request(&[b"SET", b"key:000000012345", b"value-payload-0123456789abcdef"]);
    let get = request(&[b"GET", b"key:000000012345"]);
    let hset = request(&[b"HSET", b"user:1000", b"name", b"alice", b"email", b"alice@example.com"]);
    let pipeline: Vec<u8> = (0..16).flat_map(|_| set.iter().copied()).collect();
    let reply = Frame::Bulk(bytes::Bytes::from_static(b"value-payload-0123456789abcdef")).to_vec();

    let mut g = c.benchmark_group("resp_parser");

    g.throughput(Throughput::Bytes(set.len() as u64));
    g.bench_function("parse_request SET (3 args)", |b| {
        b.iter(|| parse_request(black_box(&set)).unwrap().unwrap());
    });

    g.throughput(Throughput::Bytes(get.len() as u64));
    g.bench_function("parse_request GET (2 args)", |b| {
        b.iter(|| parse_request(black_box(&get)).unwrap().unwrap());
    });

    g.throughput(Throughput::Bytes(hset.len() as u64));
    g.bench_function("parse_request HSET (6 args)", |b| {
        b.iter(|| parse_request(black_box(&hset)).unwrap().unwrap());
    });

    g.throughput(Throughput::Bytes(pipeline.len() as u64));
    g.bench_function("parse_request 16-command SET pipeline", |b| {
        b.iter(|| {
            let buf = black_box(&pipeline[..]);
            let mut pos = 0;
            while let Some((args, used)) = parse_request(&buf[pos..]).unwrap() {
                black_box(&args);
                pos += used;
            }
            pos
        });
    });

    g.throughput(Throughput::Bytes(reply.len() as u64));
    g.bench_function("Frame::parse bulk reply (general decoder)", |b| {
        b.iter(|| Frame::parse(black_box(&reply)).unwrap().unwrap());
    });
    g.finish();
}

fn bench_store_and_commands(c: &mut Criterion) {
    let store = Store::new(64);
    for i in 0..10_000u32 {
        let key = format!("key:{i:012}");
        store.locked(key.as_bytes(), |sh| sh.set(key.as_bytes(), b"value-payload-0123456789abcdef", None));
    }
    let now = redis_rs::expiry::now_ms();
    let key = b"key:000000005000";

    let mut g = c.benchmark_group("store_and_command_path");
    g.bench_function("Shard::get hit", |b| {
        b.iter(|| store.locked(black_box(key), |sh| sh.get(key, now)));
    });
    g.bench_function("Shard::set overwrite", |b| {
        b.iter(|| store.locked(black_box(key), |sh| sh.set(key, b"value-payload-0123456789abcdef", None)));
    });

    let get_args: Vec<&[u8]> = vec![b"GET", key];
    let set_args: Vec<&[u8]> = vec![b"SET", key, b"value-payload-0123456789abcdef"];
    g.bench_function("execute GET (parse excluded, no I/O)", |b| {
        let mut ctx = Ctx::new(&store);
        b.iter(|| execute(&mut ctx, black_box(&get_args)));
    });
    g.bench_function("execute SET (parse excluded, no I/O)", |b| {
        let mut ctx = Ctx::new(&store);
        b.iter(|| execute(&mut ctx, black_box(&set_args)));
    });
    g.finish();
}

criterion_group!(benches, bench_parser, bench_store_and_commands);
criterion_main!(benches);
