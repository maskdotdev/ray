import { createFileRoute, useLocation } from "@tanstack/solid-router";
import { Show } from "solid-js";
import DocPage from "~/components/doc-page";
import CodeBlock from "~/components/code-block";
import { findDocBySlug } from "~/lib/docs";

export const Route = createFileRoute("/docs/$")({
  component: DocSplatPage,
});

function DocSplatPage() {
  const location = useLocation();
  const slug = () => {
    const path = location().pathname;
    const match = path.match(/^\/docs\/(.+)$/);
    return match ? match[1] : "";
  };
  const doc = () => findDocBySlug(slug());

  return (
    <Show when={doc()} fallback={<DocNotFound slug={slug()} />}>
      <DocPageContent slug={slug()} />
    </Show>
  );
}

function DocNotFound(props: { slug: string }) {
  return (
    <div class="max-w-4xl mx-auto px-6 py-12">
      <div class="text-center">
        <h1 class="text-4xl font-extrabold text-slate-900 dark:text-white mb-4">
          Page Not Found
        </h1>
        <p class="text-lg text-slate-600 dark:text-slate-400 mb-8">
          The documentation page{" "}
          <code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">
            {props.slug}
          </code>{" "}
          doesn't exist yet.
        </p>
        <a
          href="/docs"
          class="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-cyan-500 to-violet-500 text-white font-semibold rounded-xl hover:shadow-lg hover:shadow-cyan-500/25 transition-all duration-200"
        >
          Back to Documentation
        </a>
      </div>
    </div>
  );
}

function DocPageContent(props: { slug: string }) {
  const slug = props.slug;

  // Benchmarks overview page (root level)
  if (slug === "benchmarks") {
    return (
      <DocPage slug={slug}>
        <p>
          Performance benchmarks for RayDB across graph operations, vector
          search, and multi-language bindings.
        </p>

        <h2 id="benchmark-categories">Benchmark Categories</h2>
        <ul>
          <li>
            <a href="/docs/benchmarks/graph">
              <strong>Graph Benchmarks</strong>
            </a>{" "}
            – Graph database operations compared against Memgraph (up to 150x
            faster)
          </li>
          <li>
            <a href="/docs/benchmarks/vector">
              <strong>Vector Benchmarks</strong>
            </a>{" "}
            – Vector search performance including IVF, PQ, and IVF-PQ indexes
          </li>
          <li>
            <a href="/docs/benchmarks/cross-language">
              <strong>Cross-Language Benchmarks</strong>
            </a>{" "}
            – Compare bindings (TypeScript, Python, Rust)
          </li>
        </ul>

        <h2 id="test-environment">Test Environment</h2>
        <ul>
          <li>macOS (Apple Silicon)</li>
          <li>Bun 1.3.5</li>
          <li>Python 3.12.8</li>
          <li>Rust 1.88.0</li>
          <li>RayDB 0.1.0</li>
        </ul>

        <h2 id="highlights">Performance Highlights</h2>

        <h3 id="graph-highlights">Graph Operations</h3>
        <table>
          <thead>
            <tr>
              <th>Category</th>
              <th>RayDB vs Memgraph</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Key Lookups</td>
              <td>100-780x faster</td>
            </tr>
            <tr>
              <td>1-Hop Traversals</td>
              <td>48-71x faster</td>
            </tr>
            <tr>
              <td>Multi-Hop (3-hop)</td>
              <td>51-730x faster</td>
            </tr>
            <tr>
              <td>Batch Writes</td>
              <td>1.5-19x faster</td>
            </tr>
          </tbody>
        </table>
        <p>
          <a href="/docs/benchmarks/graph">View detailed graph benchmarks →</a>
        </p>

        <h3 id="vector-highlights">Vector Operations</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>Performance</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Distance Functions</td>
              <td>500k-1.6M ops/sec</td>
            </tr>
            <tr>
              <td>Vector Store Insert</td>
              <td>487k vectors/sec</td>
            </tr>
            <tr>
              <td>IVF Search (k=10)</td>
              <td>2.2-11k ops/sec</td>
            </tr>
            <tr>
              <td>IVF-PQ Memory Savings</td>
              <td>15x compression</td>
            </tr>
          </tbody>
        </table>
        <p>
          <a href="/docs/benchmarks/vector">
            View detailed vector benchmarks →
          </a>
        </p>

        <h2 id="bindings">Binding Performance (Read p50)</h2>
        <table>
          <thead>
            <tr>
              <th>Language</th>
              <th>10k/50k</th>
              <th>100k/500k</th>
              <th>250k/1.25M</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Rust</td>
              <td>83ns</td>
              <td>291ns</td>
              <td>417ns</td>
            </tr>
            <tr>
              <td>TypeScript</td>
              <td>167ns</td>
              <td>459ns</td>
              <td>542ns</td>
            </tr>
            <tr>
              <td>Python</td>
              <td>250ns</td>
              <td>375ns</td>
              <td>458ns</td>
            </tr>
          </tbody>
        </table>

        <h2 id="running">Running Benchmarks</h2>
        <table>
          <thead>
            <tr>
              <th>Command</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>
                <code>bun run bench/benchmark.ts</code>
              </td>
              <td>Main benchmark (RayDB only)</td>
            </tr>
            <tr>
              <td>
                <code>bun run bench:memgraph</code>
              </td>
              <td>Graph comparison vs Memgraph</td>
            </tr>
            <tr>
              <td>
                <code>bun run bench/benchmark-vector.ts</code>
              </td>
              <td>Vector search benchmarks</td>
            </tr>
            <tr>
              <td>
                <code>bun run bench:mvcc:v2</code>
              </td>
              <td>MVCC performance testing</td>
            </tr>
          </tbody>
        </table>
      </DocPage>
    );
  }

  // Introduction page (empty slug)
  if (slug === "") {
    return (
      <DocPage slug="">
        <p>
          Welcome to RayDB, a high-performance embedded graph database with
          built-in vector search, designed for Bun and TypeScript.
        </p>

        <h2 id="what-is-raydb">What is RayDB?</h2>
        <p>
          RayDB is an embedded graph database that combines the power of graph
          relationships with semantic vector search. It's designed for modern
          TypeScript applications that need:
        </p>
        <ul>
          <li>
            <strong>Graph relationships</strong> – Model complex connections
            between entities
          </li>
          <li>
            <strong>Vector search</strong> – Find semantically similar content
            using embeddings
          </li>
          <li>
            <strong>Type safety</strong> – Full TypeScript support with inferred
            types
          </li>
          <li>
            <strong>High performance</strong> – Optimized for Bun with native
            bindings
          </li>
          <li>
            <strong>Zero setup</strong> – No external database to manage
          </li>
        </ul>

        <h2 id="key-features">Key Features</h2>
        <ul>
          <li>
            <strong>Graph-native</strong> – First-class nodes, edges, and
            traversals
          </li>
          <li>
            <strong>Vector search</strong> – HNSW-indexed similarity queries
          </li>
          <li>
            <strong>Embedded</strong> – Runs in your process, no server needed
          </li>
          <li>
            <strong>Type-safe</strong> – Schemas with full TypeScript inference
          </li>
          <li>
            <strong>Fast</strong> – 833k ops/sec writes, sub-ms traversals
          </li>
          <li>
            <strong>ACID</strong> – Full transaction support
          </li>
        </ul>

        <h2 id="quick-example">Quick Example</h2>
        <CodeBlock
          code={`import { ray, defineNode, defineEdge, prop } from '@ray-db/ray';

const user = defineNode('user', {
  key: (id: string) => \`user:\${id}\`,
  props: {
    name: prop.string('name'),
    embedding: prop.vector('embedding', 1536),
  },
});

const follows = defineEdge('follows', {
  from: user,
  to: user,
});

const db = await ray('./social.raydb', {
  nodes: [user],
  edges: [follows],
});

// Create users
await db.node(user).createMany([
  { id: 'alice', name: 'Alice', embedding: [...] },
  { id: 'bob', name: 'Bob', embedding: [...] },
]);

// Find similar users
const similar = await db.node(user)
  .vector('embedding')
  .similar(queryEmbedding, { limit: 5 })
  .all();`}
          language="typescript"
        />

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li>
            <a href="/docs/getting-started/installation">Installation</a> – Get
            RayDB set up
          </li>
          <li>
            <a href="/docs/getting-started/quick-start">Quick Start</a> – Build
            your first graph
          </li>
          <li>
            <a href="/docs/guides/schema">Schema Definition</a> – Design your
            data model
          </li>
        </ul>
      </DocPage>
    );
  }

  // Concurrency guide
  if (slug === "guides/concurrency") {
    return (
      <DocPage slug={slug}>
        <p>
          RayDB supports concurrent access from multiple threads, enabling
          parallel reads for improved throughput in multi-threaded applications.
        </p>

        <h2 id="concurrency-model">Concurrency Model</h2>
        <p>
          RayDB uses a <strong>readers-writer lock</strong> pattern:
        </p>
        <ul>
          <li>
            <strong>Multiple concurrent readers</strong> – Any number of threads
            can read simultaneously
          </li>
          <li>
            <strong>Exclusive writer</strong> – Write operations acquire
            exclusive access
          </li>
          <li>
            <strong>MVCC isolation</strong> – Transactions see consistent
            snapshots
          </li>
        </ul>

        <CodeBlock
          code={`┌─────────────────────────────────────────────────────────┐
│                    RayDB Instance                       │
│  ┌─────────────────────────────────────────────────┐   │
│  │              RwLock<Database>                    │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐         │   │
│  │  │ Reader  │  │ Reader  │  │ Reader  │  ...    │   │ Concurrent
│  │  │ Thread  │  │ Thread  │  │ Thread  │         │   │ Reads OK
│  │  └─────────┘  └─────────┘  └─────────┘         │   │
│  │                     │                           │   │
│  │              ┌─────────────┐                   │   │
│  │              │   Writer    │                   │   │ Exclusive
│  │              │   Thread    │                   │   │ Access
│  │              └─────────────┘                   │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘`}
          language="text"
        />

        <h2 id="typescript-example">TypeScript / JavaScript</h2>
        <p>In Node.js or Bun, use worker threads or async operations:</p>
        <CodeBlock
          code={`import { ray, node, prop } from '@anthropic-ai/ray';

const user = node('user', {
  key: (id: string) => \`user:\${id}\`,
  props: { name: prop.string('name') },
});

const db = await ray('./data.raydb', { nodes: [user] });

// Concurrent reads from multiple async operations
const results = await Promise.all([
  db.get(user, 'alice'),
  db.get(user, 'bob'),
  db.get(user, 'charlie'),
  db.from(user).out('follows').toArray(),
]);

// With worker threads, share the db path (each worker opens independently)
// Workers can read concurrently from the same database file`}
          language="typescript"
        />

        <h2 id="python-example">Python</h2>
        <p>
          Python's <code>threading</code> module works well for concurrent
          reads:
        </p>
        <CodeBlock
          code={`import threading
from raydb import ray, node, prop

user = node("user",
    key=lambda id: f"user:{id}",
    props={"name": prop.string("name")}
)

db = ray("./data.raydb", nodes=[user])

results = {}

def read_user(user_id: str):
    """Each thread can read concurrently"""
    results[user_id] = db.get(user, user_id)

# Spawn multiple reader threads
threads = [
    threading.Thread(target=read_user, args=(uid,))
    for uid in ["alice", "bob", "charlie", "dave"]
]

for t in threads:
    t.start()
for t in threads:
    t.join()

# All reads completed in parallel
print(results)`}
          language="python"
        />

        <h2 id="rust-example">Rust</h2>
        <p>
          In Rust, wrap the database in{" "}
          <code>Arc&lt;RwLock&lt;...&gt;&gt;</code>:
        </p>
        <CodeBlock
          code={`use raydb::Ray;
use std::sync::{Arc, RwLock};
use std::thread;

let db = Arc::new(RwLock::new(Ray::open("./data.raydb")?));

let handles: Vec<_> = (0..4).map(|i| {
    let db = Arc::clone(&db);
    thread::spawn(move || {
        // Multiple threads can acquire read locks simultaneously
        let guard = db.read().unwrap();
        guard.get_node(format!("user:{}", i))
    })
}).collect();

// Collect results
let results: Vec<_> = handles.into_iter()
    .map(|h| h.join().unwrap())
    .collect();`}
          language="rust"
        />

        <h2 id="performance">Performance Scaling</h2>
        <p>
          Benchmarks show ~1.5-1.8x throughput improvement with 4-8 reader
          threads:
        </p>
        <table>
          <thead>
            <tr>
              <th>Threads</th>
              <th>Relative Throughput</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1</td>
              <td>1.0x (baseline)</td>
              <td>Single-threaded</td>
            </tr>
            <tr>
              <td>2</td>
              <td>~1.3x</td>
              <td>Good scaling</td>
            </tr>
            <tr>
              <td>4</td>
              <td>~1.5-1.6x</td>
              <td>Sweet spot for most workloads</td>
            </tr>
            <tr>
              <td>8</td>
              <td>~1.6-1.8x</td>
              <td>Diminishing returns</td>
            </tr>
            <tr>
              <td>16</td>
              <td>~1.7-1.9x</td>
              <td>Lock contention increases</td>
            </tr>
          </tbody>
        </table>

        <h2 id="best-practices">Best Practices</h2>
        <ul>
          <li>
            <strong>Batch writes</strong> – Group multiple writes into single
            operations to minimize exclusive lock time
          </li>
          <li>
            <strong>Use transactions for consistency</strong> – MVCC ensures
            readers see consistent snapshots even during concurrent writes
          </li>
          <li>
            <strong>Profile your workload</strong> – The optimal thread count
            depends on your read/write ratio and data access patterns
          </li>
          <li>
            <strong>Avoid long-held locks</strong> – Keep critical sections
            short; do processing outside the lock
          </li>
        </ul>

        <h2 id="mvcc">MVCC and Snapshot Isolation</h2>
        <p>
          RayDB uses Multi-Version Concurrency Control (MVCC) to provide
          snapshot isolation:
        </p>
        <ul>
          <li>Readers never block writers</li>
          <li>Writers never block readers</li>
          <li>
            Each transaction sees a consistent snapshot from its start time
          </li>
          <li>Write conflicts are detected and one transaction is aborted</li>
        </ul>

        <CodeBlock
          code={`// Transaction isolation example
const tx1 = db.beginTransaction();
const tx2 = db.beginTransaction();

// tx1 reads value
const value1 = tx1.get(user, 'alice');

// tx2 modifies same value
tx2.update(user, 'alice', { name: 'Alice Updated' });
tx2.commit();

// tx1 still sees original value (snapshot isolation)
const value2 = tx1.get(user, 'alice');
console.log(value1.name === value2.name); // true`}
          language="typescript"
        />

        <h2 id="limitations">Limitations</h2>
        <ul>
          <li>
            <strong>Single-process only</strong> – Concurrent access is within a
            single process; multi-process access requires external coordination
          </li>
          <li>
            <strong>Write serialization</strong> – All writes are serialized;
            high-write workloads may see contention
          </li>
          <li>
            <strong>Memory overhead</strong> – MVCC maintains version history,
            using additional memory
          </li>
        </ul>

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li>
            <a href="/docs/guides/transactions">Transactions</a> – Learn about
            ACID guarantees
          </li>
          <li>
            <a href="/docs/benchmarks">Benchmarks</a> – See detailed performance
            numbers
          </li>
          <li>
            <a href="/docs/internals/architecture">Architecture</a> – Understand
            the internal design
          </li>
        </ul>
      </DocPage>
    );
  }

  // Default fallback for unknown pages
  return (
    <DocPage slug={slug}>
      <p>This page is coming soon.</p>
    </DocPage>
  );
}
