import { createFileRoute, useLocation } from '@tanstack/solid-router'
import { Show } from 'solid-js'
import DocPage from '~/components/doc-page'
import CodeBlock from '~/components/code-block'
import { findDocBySlug } from '~/lib/docs'

export const Route = createFileRoute('/docs/$')({
  component: DocSplatPage,
})

function DocSplatPage() {
  const location = useLocation()
  const slug = () => {
    const path = location().pathname
    const match = path.match(/^\/docs\/(.+)$/)
    return match ? match[1] : ''
  }
  const doc = () => findDocBySlug(slug())

  return (
    <Show
      when={doc()}
      fallback={<DocNotFound slug={slug()} />}
    >
      <DocPageContent slug={slug()} />
    </Show>
  )
}

function DocNotFound(props: { slug: string }) {
  return (
    <div class="max-w-4xl mx-auto px-6 py-12">
      <div class="text-center">
        <h1 class="text-4xl font-extrabold text-slate-900 dark:text-white mb-4">
          Page Not Found
        </h1>
        <p class="text-lg text-slate-600 dark:text-slate-400 mb-8">
          The documentation page <code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">{props.slug}</code> doesn't exist yet.
        </p>
        <a
          href="/docs"
          class="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-cyan-500 to-violet-500 text-white font-semibold rounded-xl hover:shadow-lg hover:shadow-cyan-500/25 transition-all duration-200"
        >
          Back to Documentation
        </a>
      </div>
    </div>
  )
}

function DocPageContent(props: { slug: string }) {
  const slug = props.slug

  // Benchmarks overview page (root level)
  if (slug === 'benchmarks') {
    return (
      <DocPage slug={slug}>
        <p>
          Performance benchmarks for RayDB across graph operations, vector search, and
          multi-language bindings.
        </p>

        <h2 id="benchmark-categories">Benchmark Categories</h2>
        <ul>
          <li>
            <a href="/docs/benchmarks/graph"><strong>Graph Benchmarks</strong></a> – 
            Graph database operations compared against Memgraph (up to 150x faster)
          </li>
          <li>
            <a href="/docs/benchmarks/vector"><strong>Vector Benchmarks</strong></a> – 
            Vector search performance including IVF, PQ, and IVF-PQ indexes
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
            <tr><td>Key Lookups</td><td>100-780x faster</td></tr>
            <tr><td>1-Hop Traversals</td><td>48-71x faster</td></tr>
            <tr><td>Multi-Hop (3-hop)</td><td>51-730x faster</td></tr>
            <tr><td>Batch Writes</td><td>1.5-19x faster</td></tr>
          </tbody>
        </table>
        <p><a href="/docs/benchmarks/graph">View detailed graph benchmarks →</a></p>

        <h3 id="vector-highlights">Vector Operations</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>Performance</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Distance Functions</td><td>500k-1.6M ops/sec</td></tr>
            <tr><td>Vector Store Insert</td><td>487k vectors/sec</td></tr>
            <tr><td>IVF Search (k=10)</td><td>2.2-11k ops/sec</td></tr>
            <tr><td>IVF-PQ Memory Savings</td><td>15x compression</td></tr>
          </tbody>
        </table>
        <p><a href="/docs/benchmarks/vector">View detailed vector benchmarks →</a></p>

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
            <tr><td>Rust</td><td>83ns</td><td>291ns</td><td>417ns</td></tr>
            <tr><td>TypeScript</td><td>167ns</td><td>459ns</td><td>542ns</td></tr>
            <tr><td>Python</td><td>250ns</td><td>375ns</td><td>458ns</td></tr>
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
            <tr><td><code>bun run bench/benchmark.ts</code></td><td>Main benchmark (RayDB only)</td></tr>
            <tr><td><code>bun run bench:memgraph</code></td><td>Graph comparison vs Memgraph</td></tr>
            <tr><td><code>bun run bench/benchmark-vector.ts</code></td><td>Vector search benchmarks</td></tr>
            <tr><td><code>bun run bench:mvcc:v2</code></td><td>MVCC performance testing</td></tr>
          </tbody>
        </table>
      </DocPage>
    )
  }

  // Introduction page (empty slug)
  if (slug === '') {
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
          <li><strong>Graph relationships</strong> – Model complex connections between entities</li>
          <li><strong>Vector search</strong> – Find semantically similar content using embeddings</li>
          <li><strong>Type safety</strong> – Full TypeScript support with inferred types</li>
          <li><strong>High performance</strong> – Optimized for Bun with native bindings</li>
          <li><strong>Zero setup</strong> – No external database to manage</li>
        </ul>

        <h2 id="key-features">Key Features</h2>
        <ul>
          <li><strong>Graph-native</strong> – First-class nodes, edges, and traversals</li>
          <li><strong>Vector search</strong> – HNSW-indexed similarity queries</li>
          <li><strong>Embedded</strong> – Runs in your process, no server needed</li>
          <li><strong>Type-safe</strong> – Schemas with full TypeScript inference</li>
          <li><strong>Fast</strong> – 833k ops/sec writes, sub-ms traversals</li>
          <li><strong>ACID</strong> – Full transaction support</li>
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
          <li><a href="/docs/getting-started/installation">Installation</a> – Get RayDB set up</li>
          <li><a href="/docs/getting-started/quick-start">Quick Start</a> – Build your first graph</li>
          <li><a href="/docs/guides/schema">Schema Definition</a> – Design your data model</li>
        </ul>
      </DocPage>
    )
  }

  // Default fallback for unknown pages
  return (
    <DocPage slug={slug}>
      <p>This page is coming soon.</p>
    </DocPage>
  )
}
