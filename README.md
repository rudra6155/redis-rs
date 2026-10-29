# redis-rs
A Redis subset in Rust: RESP2, a sharded in-memory store with TTLs, an append-only file, and a tokio concurrency model. Replies are byte-identical to redis:7 for the supported commands (199 oracle rows, all passing). Every README claim maps to a committed artifact. Not a drop-in replacement: everything else returns ERR unknown command.
