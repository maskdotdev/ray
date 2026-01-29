import { createFileRoute, useLocation, Link } from '@tanstack/solid-router'
import { Show } from 'solid-js'
import DocPage from '~/components/doc-page'
import { findDocBySlug } from '~/lib/docs'

export const Route = createFileRoute('/docs/benchmarks/$')({
  component: BenchmarksSplatPage,
})

function BenchmarksSplatPage() {
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
          The benchmark page <code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">{props.slug}</code> doesn't exist yet.
        </p>
        <a
          href="/docs/benchmarks"
          class="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-cyan-500 to-violet-500 text-white font-semibold rounded-xl hover:shadow-lg hover:shadow-cyan-500/25 transition-all duration-200"
        >
          Back to Benchmarks
        </a>
      </div>
    </div>
  )
}

function DocPageContent(props: { slug: string }) {
  const slug = props.slug

  // Graph Benchmarks page
  if (slug === 'benchmarks/graph') {
    return (
      <DocPage slug={slug}>
        <p>
          Performance benchmarks comparing RayDB's graph operations against other graph databases.
        </p>

        <h2 id="test-configuration">Test Configuration</h2>
        <ul>
          <li><strong>Memgraph</strong>: Docker container with 2GB memory limit</li>
          <li><strong>RayDB</strong>: Embedded, in-process</li>
          <li><strong>Graph structure</strong>: Power-law distribution with 1% hub nodes</li>
          <li><strong>Edge types</strong>: CALLS (40%), REFERENCES (35%), IMPORTS (15%), EXTENDS (10%)</li>
          <li><strong>Warmup</strong>: 1000 iterations before measurement</li>
        </ul>

        <h2 id="small-scale">Results: Small Scale (10k nodes, 50k edges)</h2>

        <h3 id="small-key-lookups">Key Lookups</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Uniform random</td><td>1.23us</td><td>125us</td><td>101x</td></tr>
            <tr><td>Sequential</td><td>0.89us</td><td>98us</td><td>110x</td></tr>
            <tr><td>Missing keys</td><td>0.45us</td><td>89us</td><td>198x</td></tr>
          </tbody>
        </table>

        <h3 id="small-traversals">1-Hop Traversals</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Out neighbors</td><td>2.34us</td><td>156us</td><td>67x</td></tr>
            <tr><td>In neighbors</td><td>2.45us</td><td>162us</td><td>66x</td></tr>
            <tr><td>Filtered (by type)</td><td>1.89us</td><td>134us</td><td>71x</td></tr>
          </tbody>
        </table>

        <h3 id="small-multi-hop">Multi-Hop Traversals</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>2-hop traversal</td><td>12.3us</td><td>890us</td><td>72x</td></tr>
            <tr><td>3-hop traversal</td><td>45.7us</td><td>2340us</td><td>51x</td></tr>
          </tbody>
        </table>

        <h3 id="small-writes">Writes</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Batch insert (5k)</td><td>23.5ms</td><td>456ms</td><td>19x</td></tr>
          </tbody>
        </table>

        <p><strong>Overall: ~150x faster at small scale</strong></p>

        <h2 id="large-scale">Results: Large Scale (100k nodes, 1M edges)</h2>

        <h3 id="large-key-lookups">Key Lookups</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Uniform random</td><td>750ns</td><td>306us</td><td>408x</td></tr>
            <tr><td>Sequential</td><td>333ns</td><td>254us</td><td>763x</td></tr>
            <tr><td>Missing keys</td><td>375ns</td><td>292us</td><td>780x</td></tr>
          </tbody>
        </table>

        <h3 id="large-traversals">1-Hop Traversals</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Out neighbors</td><td>5.75us</td><td>277us</td><td>48x</td></tr>
            <tr><td>In neighbors</td><td>3.96us</td><td>202us</td><td>51x</td></tr>
            <tr><td>Filtered (by type)</td><td>2.67us</td><td>152us</td><td>57x</td></tr>
          </tbody>
        </table>

        <h3 id="large-multi-hop">Multi-Hop Traversals</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>2-hop traversal</td><td>5.54us</td><td>482us</td><td>87x</td></tr>
            <tr><td>3-hop traversal</td><td>27.3us</td><td>19.9ms</td><td>730x</td></tr>
          </tbody>
        </table>

        <h3 id="large-writes">Writes</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB p50</th>
              <th>Memgraph p50</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Batch insert (10k)</td><td>35.1ms</td><td>51.1ms</td><td>1.5x</td></tr>
          </tbody>
        </table>

        <p><strong>Overall: ~118x faster at large scale</strong></p>

        <h2 id="summary">Summary by Category</h2>
        <table>
          <thead>
            <tr>
              <th>Category</th>
              <th>Small Scale</th>
              <th>Large Scale</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Key Lookups</td><td>125x</td><td>624x</td></tr>
            <tr><td>Traversals</td><td>57x</td><td>52x</td></tr>
            <tr><td>Edge Checks</td><td>167x</td><td>164x</td></tr>
            <tr><td>Multi-Hop</td><td>60x</td><td>252x</td></tr>
            <tr><td>Writes</td><td>19x</td><td>1.5x</td></tr>
            <tr><td><strong>Geometric Mean</strong></td><td><strong>150x</strong></td><td><strong>118x</strong></td></tr>
          </tbody>
        </table>

        <h2 id="why-faster">Why RayDB is Faster</h2>

        <h3 id="architecture">Architecture Differences</h3>
        <table>
          <thead>
            <tr>
              <th>Aspect</th>
              <th>RayDB</th>
              <th>Memgraph</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Deployment</td><td>Embedded (in-process)</td><td>Client-server</td></tr>
            <tr><td>Protocol</td><td>Direct function calls</td><td>Bolt over TCP</td></tr>
            <tr><td>Storage</td><td>Memory-mapped CSR</td><td>In-memory + persistence</td></tr>
            <tr><td>Query</td><td>Direct data access</td><td>Cypher parsing + planning</td></tr>
          </tbody>
        </table>

        <h3 id="overhead">Per-Operation Overhead</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>RayDB</th>
              <th>Memgraph</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Network round-trip</td><td>0</td><td>~0.5-2ms</td></tr>
            <tr><td>Query parsing</td><td>0</td><td>~0.1ms</td></tr>
            <tr><td>Query planning</td><td>0</td><td>~0.1ms</td></tr>
            <tr><td>Data serialization</td><td>0</td><td>~0.05ms per result</td></tr>
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
            <tr><td><code>bun run bench:memgraph</code></td><td>Default scale (10k nodes, 50k edges)</td></tr>
            <tr><td><code>bun run bench:memgraph:scale</code></td><td>Both small and large scales</td></tr>
            <tr><td><code>bun run bench/memgraph-batch-test.ts</code></td><td>Batch write stress test</td></tr>
          </tbody>
        </table>
      </DocPage>
    )
  }

  // Vector Benchmarks page
  if (slug === 'benchmarks/vector') {
    return (
      <DocPage slug={slug}>
        <p>
          Performance benchmarks for RayDB's vector embedding operations, including distance 
          calculations, indexing algorithms, and search performance.
        </p>

        <h2 id="distance-functions">Distance Functions (768D)</h2>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>Time (p50)</th>
              <th>Throughput</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Dot product</td><td>4.21us</td><td>204,997 ops/sec</td></tr>
            <tr><td>Cosine distance</td><td>458ns</td><td>539,963 ops/sec</td></tr>
            <tr><td>Squared Euclidean</td><td>459ns</td><td>1,180,650 ops/sec</td></tr>
            <tr><td>Euclidean distance</td><td>583ns</td><td>1,632,848 ops/sec</td></tr>
            <tr><td>Normalize</td><td>792ns</td><td>548,321 ops/sec</td></tr>
            <tr><td>L2 norm</td><td>625ns</td><td>1,416,599 ops/sec</td></tr>
          </tbody>
        </table>

        <h2 id="batch-operations">Batch Distance Operations</h2>
        <p>
          Batch operations process multiple vectors in a single pass, enabling better 
          CPU cache utilization.
        </p>
        <table>
          <thead>
            <tr>
              <th>Batch Size</th>
              <th>Cosine p50</th>
              <th>Throughput</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>64</td><td>~15us</td><td>~4.3M vectors/sec</td></tr>
            <tr><td>256</td><td>~50us</td><td>~5.1M vectors/sec</td></tr>
            <tr><td>1024</td><td>~200us</td><td>~5.1M vectors/sec</td></tr>
          </tbody>
        </table>

        <h2 id="vector-store">Vector Store (10k vectors, 768D)</h2>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>Time (p50)</th>
              <th>Throughput</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Insert (per vector)</td><td>1.38us</td><td>487,983 vectors/sec</td></tr>
            <tr><td>Random lookup</td><td>333ns</td><td>1,457,567 ops/sec</td></tr>
            <tr><td>Sequential lookup</td><td>125ns</td><td>6,120,962 ops/sec</td></tr>
          </tbody>
        </table>

        <h2 id="ivf-index">IVF Index Performance</h2>

        <h3 id="ivf-build">Index Build (10k vectors, 768D)</h3>
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>Time</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Add training vectors</td><td>2.06ms</td><td>10,000 vectors</td></tr>
            <tr><td>K-means training</td><td>1.94s</td><td>100 clusters</td></tr>
            <tr><td>Insert into index</td><td>213.77ms</td><td>10,000 vectors</td></tr>
          </tbody>
        </table>

        <h3 id="ivf-search">IVF Search (k=10)</h3>
        <table>
          <thead>
            <tr>
              <th>nProbe</th>
              <th>Time (p50)</th>
              <th>Throughput</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>1</td><td>79.25us</td><td>11,400 ops/sec</td></tr>
            <tr><td>5</td><td>232.04us</td><td>4,304 ops/sec</td></tr>
            <tr><td>10</td><td>447.96us</td><td>2,195 ops/sec</td></tr>
            <tr><td>20</td><td>907.63us</td><td>866 ops/sec</td></tr>
          </tbody>
        </table>

        <h2 id="brute-force-vs-ivf">Brute Force vs IVF (k=10, nProbe=10)</h2>
        <table>
          <thead>
            <tr>
              <th>Vectors</th>
              <th>Brute Force (p50)</th>
              <th>IVF (p50)</th>
              <th>Speedup</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>1,000</td><td>243.46us</td><td>85.79us</td><td>2.8x</td></tr>
            <tr><td>5,000</td><td>1.26ms</td><td>256.46us</td><td>4.9x</td></tr>
            <tr><td>10,000</td><td>2.46ms</td><td>422.50us</td><td>5.8x</td></tr>
          </tbody>
        </table>

        <h2 id="pq">Product Quantization (PQ)</h2>
        <table>
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>PQ train</td><td>23.20s (96 subspaces, 256 centroids)</td></tr>
            <tr><td>PQ encode</td><td>1.39s (7,209 vectors/sec)</td></tr>
            <tr><td>PQ ADC distance (1000 vectors)</td><td>56.25us p50 (4.6x vs full distance)</td></tr>
          </tbody>
        </table>

        <h2 id="ivf-pq">IVF-PQ Combined Index</h2>
        <table>
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>IVF-PQ train</td><td>27.84s (100 clusters, 96 subspaces)</td></tr>
            <tr><td>IVF-PQ search (k=10, nProbe=10)</td><td>263.83us p50</td></tr>
            <tr><td>IVF-PQ vs IVF speedup</td><td>1.7x</td></tr>
            <tr><td>Memory savings</td><td>15.0x</td></tr>
          </tbody>
        </table>

        <h2 id="memory">Memory Estimates (10k vectors, 768D)</h2>
        <table>
          <thead>
            <tr>
              <th>Component</th>
              <th>Size</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Vector data</td><td>29.30 MB</td></tr>
            <tr><td>IVF overhead</td><td>339.06 KB</td></tr>
            <tr><td>Total estimated</td><td>29.63 MB</td></tr>
            <tr><td>Bytes per vector</td><td>3,106.7</td></tr>
          </tbody>
        </table>

        <h2 id="recommendations">Recommendations</h2>

        <h3 id="index-selection">Choosing an Index Type</h3>
        <table>
          <thead>
            <tr>
              <th>Use Case</th>
              <th>Recommended Index</th>
              <th>Trade-offs</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>&lt; 10k vectors</td><td>Brute force</td><td>Exact results, no index overhead</td></tr>
            <tr><td>10k - 100k vectors</td><td>IVF</td><td>Good balance of speed/accuracy</td></tr>
            <tr><td>100k - 1M vectors</td><td>IVF-PQ</td><td>Memory efficient, ~95% recall</td></tr>
            <tr><td>&gt; 1M vectors</td><td>IVF-PQ + HNSW</td><td>Hybrid approach for best performance</td></tr>
          </tbody>
        </table>

        <h3 id="tuning">Tuning Parameters</h3>
        <table>
          <thead>
            <tr>
              <th>Parameter</th>
              <th>Effect</th>
              <th>Recommendation</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>nClusters (IVF)</td><td>More = finer partitioning</td><td>sqrt(N) to 4*sqrt(N)</td></tr>
            <tr><td>nProbe (IVF)</td><td>More = higher recall, slower</td><td>Start with 10, tune for recall target</td></tr>
            <tr><td>numSubspaces (PQ)</td><td>More = less compression</td><td>dims/8 to dims/16</td></tr>
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
            <tr><td><code>bun run bench/benchmark-vector.ts</code></td><td>Default config (10k vectors, 768D)</td></tr>
            <tr><td><code>bun run bench/benchmark-vector.ts --vectors 50000</code></td><td>Custom vector count</td></tr>
            <tr><td><code>bun run bench/benchmark-vector.ts --dimensions 384</code></td><td>Custom dimensions</td></tr>
          </tbody>
        </table>
      </DocPage>
    )
  }

  // Default fallback for unknown pages
  return (
    <DocPage slug={slug}>
      <p>This benchmark page is coming soon.</p>
    </DocPage>
  )
}
