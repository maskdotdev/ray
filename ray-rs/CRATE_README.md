# RayDB

RayDB is a high-performance embedded graph database with built-in vector search.
This crate provides the Rust core and the high-level Ray API.

## Features

- ACID transactions with WAL-based durability
- Node and edge CRUD with properties
- Labels, edge types, and schema helpers
- Fluent traversal and pathfinding (BFS, Dijkstra, Yen)
- Vector embeddings with IVF and IVF-PQ indexes
- Single-file and multi-file storage formats

## Install

```toml
[dependencies]
raydb = "0.1"
```

## Quick start (Ray API)

```rust
use raydb::api::ray::{EdgeDef, NodeDef, PropDef, Ray, RayOptions};
use raydb::types::PropValue;
use std::collections::HashMap;

fn main() -> raydb::error::Result<()> {
  let user = NodeDef::new("User", "user:")
    .prop(PropDef::string("name").required());
  let knows = EdgeDef::new("KNOWS");

  let mut ray = Ray::open("my_graph.raydb", RayOptions::new().node(user).edge(knows))?;

  let mut alice_props = HashMap::new();
  alice_props.insert("name".to_string(), PropValue::String("Alice".into()));
  let alice = ray.create_node("User", "alice", alice_props)?;

  let bob = ray.create_node("User", "bob", HashMap::new())?;
  ray.link(alice.id, "KNOWS", bob.id)?;

  let friends = ray.neighbors_out(alice.id, Some("KNOWS"))?;
  println!("friends: {friends:?}");

  Ok(())
}
```

## Lower-level API

If you want direct access to graph primitives, use `raydb::graph::db::open_graph_db`
and the modules under `raydb::graph`, `raydb::vector`, and `raydb::core`.

## Concurrent Access

RayDB supports concurrent reads when wrapped in a `RwLock`. Multiple threads can read simultaneously:

```rust
use std::sync::Arc;
use parking_lot::RwLock;
use raydb::api::ray::{Ray, RayOptions, NodeDef};

// Wrap Ray in RwLock for concurrent access
let ray = Ray::open("graph.raydb", RayOptions::new().node(NodeDef::new("User", "user:")))?;
let db = Arc::new(RwLock::new(ray));

// Multiple threads can read concurrently
let db_clone = Arc::clone(&db);
std::thread::spawn(move || {
    let guard = db_clone.read();  // Shared read lock
    let user = guard.get("User", "alice");
});

// Writes require exclusive access
{
    let mut guard = db.write();  // Exclusive write lock
    guard.create_node("User", "bob", HashMap::new())?;
}
```

**Concurrency model:**

- **Reads (`&self`)**: `get()`, `exists()`, `neighbors_out()`, `from()`, traversals - concurrent via `RwLock::read()`
- **Writes (`&mut self`)**: `create_node()`, `link()`, `set_prop()` - exclusive via `RwLock::write()`
- The internal data structures use `RwLock` for thread-safe access to delta state and schema mappings

## Documentation

```text
https://ray-kwaf.vercel.app/docs
```

## License

MIT License - see the main project LICENSE file for details.
