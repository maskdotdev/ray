/**
 * Vector Index Benchmark (NAPI)
 *
 * Benchmarks vector index operations via native NAPI bindings.
 * Intended for comparison with Rust and Python vector benchmarks.
 *
 * Prerequisites:
 *   cd ray-rs && npm run build
 *
 * Usage:
 *   bun run bench/benchmark-napi-vector.ts [options]
 *
 * Options:
 *   --vectors N        Number of vectors (default: 10000)
 *   --dimensions D     Vector dimensions (default: 768)
 *   --iterations I     Iterations for latency benchmarks (default: 1000)
 *   --k N              Number of nearest neighbors (default: 10)
 *   --n-probe N         IVF nProbe (default: 10)
 *   --output FILE      Output file path (default: bench/results/benchmark-napi-vector-<timestamp>.txt)
 *   --no-output        Disable file output
 */

import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

// eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires
const nativeBinding = require("../ray-rs/index.js") as {
  createVectorIndex?: (options: VectorIndexOptions) => VectorIndex;
  VectorIndex?: new (options: VectorIndexOptions) => VectorIndex;
};

interface VectorIndex {
  set(nodeId: number, vector: Array<number>): void;
  get(nodeId: number): Array<number> | null;
  buildIndex(): void;
  search(query: Array<number>, options: SimilarOptions): Array<VectorSearchHit>;
  stats(): VectorIndexStats;
  clear(): void;
}

interface SimilarOptions {
  k: number;
  threshold?: number;
  nProbe?: number;
}

interface VectorSearchHit {
  nodeId: number;
  distance: number;
  similarity: number;
}

type JsDistanceMetric = "Cosine" | "Euclidean" | "DotProduct";

interface VectorIndexOptions {
  dimensions: number;
  metric?: JsDistanceMetric;
  rowGroupSize?: number;
  fragmentTargetSize?: number;
  normalize?: boolean;
  ivf?: {
    nClusters?: number;
    nProbe?: number;
    metric?: JsDistanceMetric;
  };
  trainingThreshold?: number;
  cacheMaxSize?: number;
}

interface VectorIndexStats {
  totalVectors: number;
  liveVectors: number;
  dimensions: number;
  metric: JsDistanceMetric;
  indexTrained: boolean;
  indexClusters?: number;
}

const createVectorIndex =
  nativeBinding.createVectorIndex ||
  (nativeBinding.VectorIndex
    ? (options: VectorIndexOptions) => new nativeBinding.VectorIndex!(options)
    : null);

if (!createVectorIndex) {
  console.error("Error: VectorIndex not found in NAPI bindings.");
  console.error("Make sure to build the NAPI bindings first:");
  console.error("  cd ray-rs && npm run build");
  process.exit(1);
}

// =============================================================================
// Configuration
// =============================================================================

interface BenchConfig {
  vectors: number;
  dimensions: number;
  iterations: number;
  k: number;
  nProbe: number;
  outputFile: string | null;
}

function parseArgs(): BenchConfig {
  const args = process.argv.slice(2);
  const now = new Date();
  const timestamp = now.toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const defaultOutput = join(
    dirname(import.meta.path),
    "results",
    `benchmark-napi-vector-${timestamp}.txt`
  );

  const config: BenchConfig = {
    vectors: 10000,
    dimensions: 768,
    iterations: 1000,
    k: 10,
    nProbe: 10,
    outputFile: defaultOutput,
  };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case "--vectors":
        config.vectors = Number.parseInt(args[++i] || "10000", 10);
        break;
      case "--dimensions":
        config.dimensions = Number.parseInt(args[++i] || "768", 10);
        break;
      case "--iterations":
        config.iterations = Number.parseInt(args[++i] || "1000", 10);
        break;
      case "--k":
        config.k = Number.parseInt(args[++i] || "10", 10);
        break;
      case "--n-probe":
        config.nProbe = Number.parseInt(args[++i] || "10", 10);
        break;
      case "--output":
        config.outputFile = args[++i] || null;
        break;
      case "--no-output":
        config.outputFile = null;
        break;
    }
  }

  return config;
}

// =============================================================================
// Output Logger
// =============================================================================

class Logger {
  private outputFile: string | null;
  private buffer: string[] = [];

  constructor(outputFile: string | null) {
    this.outputFile = outputFile;
  }

  log(message = ""): void {
    console.log(message);
    this.buffer.push(message);
  }

  async flush(): Promise<void> {
    if (this.outputFile && this.buffer.length > 0) {
      await mkdir(dirname(this.outputFile), { recursive: true });
      await writeFile(this.outputFile, `${this.buffer.join("\n")}\n`);
    }
  }

  getOutputPath(): string | null {
    return this.outputFile;
  }
}

let logger: Logger;

// =============================================================================
// Latency Tracking
// =============================================================================

interface LatencyStats {
  count: number;
  min: number;
  max: number;
  sum: number;
  p50: number;
  p95: number;
  p99: number;
}

class LatencyTracker {
  private samples: number[] = [];

  record(latencyNs: number): void {
    this.samples.push(latencyNs);
  }

  getStats(): LatencyStats {
    if (this.samples.length === 0) {
      return { count: 0, min: 0, max: 0, sum: 0, p50: 0, p95: 0, p99: 0 };
    }

    const sorted = [...this.samples].sort((a, b) => a - b);
    const count = sorted.length;

    return {
      count,
      min: sorted[0]!,
      max: sorted[count - 1]!,
      sum: sorted.reduce((a, b) => a + b, 0),
      p50: sorted[Math.floor(count * 0.5)]!,
      p95: sorted[Math.floor(count * 0.95)]!,
      p99: sorted[Math.floor(count * 0.99)]!,
    };
  }
}

function formatLatency(ns: number): string {
  if (ns < 1000) return `${ns.toFixed(0)}ns`;
  if (ns < 1_000_000) return `${(ns / 1000).toFixed(2)}us`;
  return `${(ns / 1_000_000).toFixed(2)}ms`;
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

function printLatencyTable(name: string, stats: LatencyStats): void {
  const opsPerSec =
    stats.sum > 0 ? stats.count / (stats.sum / 1_000_000_000) : 0;
  logger.log(
    `${name.padEnd(40)} p50=${formatLatency(stats.p50).padStart(10)} p95=${formatLatency(stats.p95).padStart(10)} p99=${formatLatency(stats.p99).padStart(10)} (${formatNumber(Math.round(opsPerSec))} ops/sec)`
  );
}

// =============================================================================
// Vector Generation
// =============================================================================

function generateRandomVector(dimensions: number): number[] {
  const vec = new Array<number>(dimensions);
  for (let d = 0; d < dimensions; d++) {
    vec[d] = Math.random() * 2 - 1;
  }
  return vec;
}

function generateRandomVectors(count: number, dimensions: number): number[][] {
  const vectors: number[][] = [];
  for (let i = 0; i < count; i++) {
    vectors.push(generateRandomVector(dimensions));
  }
  return vectors;
}

// =============================================================================
// Benchmarks
// =============================================================================

function benchmarkVectorIndex(config: BenchConfig): void {
  const index = createVectorIndex!({
    dimensions: config.dimensions,
    metric: "Cosine",
    ivf: { nProbe: config.nProbe, metric: "Cosine" },
    trainingThreshold: 1000,
  });

  logger.log("\n--- Vector Index Benchmarks (NAPI) ---");

  const vectors = generateRandomVectors(config.vectors, config.dimensions);

  logger.log("\n  Insert benchmarks:");
  const insertTracker = new LatencyTracker();
  const insertStart = Bun.nanoseconds();
  for (let i = 0; i < config.vectors; i++) {
    const start = Bun.nanoseconds();
    index.set(i, vectors[i]!);
    insertTracker.record(Bun.nanoseconds() - start);
  }
  const insertTime = Bun.nanoseconds() - insertStart;
  printLatencyTable(`Set (${formatNumber(config.vectors)} vectors)`, insertTracker.getStats());
  logger.log(
    `  Total set time: ${formatLatency(insertTime)} (${formatNumber(Math.round((config.vectors * 1_000_000_000) / insertTime))} vectors/sec)`
  );

  logger.log("\n  Index build:");
  const buildStart = Bun.nanoseconds();
  index.buildIndex();
  const buildTime = Bun.nanoseconds() - buildStart;
  logger.log(`  buildIndex(): ${formatLatency(buildTime)}`);

  logger.log("\n  Lookup benchmarks:");
  const lookupTracker = new LatencyTracker();
  for (let i = 0; i < config.iterations; i++) {
    const nodeId = Math.floor(Math.random() * config.vectors);
    const start = Bun.nanoseconds();
    index.get(nodeId);
    lookupTracker.record(Bun.nanoseconds() - start);
  }
  printLatencyTable("Random get", lookupTracker.getStats());

  logger.log("\n  Search benchmarks:");
  const searchTracker = new LatencyTracker();
  for (let i = 0; i < config.iterations; i++) {
    const query = generateRandomVector(config.dimensions);
    const start = Bun.nanoseconds();
    index.search(query, { k: config.k, nProbe: config.nProbe });
    searchTracker.record(Bun.nanoseconds() - start);
  }
  printLatencyTable(`Search (k=${config.k}, nProbe=${config.nProbe})`, searchTracker.getStats());

  const stats = index.stats();
  logger.log("\n  Index stats:");
  logger.log(`    Total vectors: ${formatNumber(stats.totalVectors)}`);
  logger.log(`    Live vectors: ${formatNumber(stats.liveVectors)}`);
  logger.log(`    Dimensions: ${stats.dimensions}`);
  logger.log(`    Metric: ${stats.metric}`);
  logger.log(`    Index trained: ${stats.indexTrained}`);
  if (stats.indexClusters !== undefined) {
    logger.log(`    Index clusters: ${stats.indexClusters}`);
  }
}

// =============================================================================
// Main
// =============================================================================

async function runBenchmarks(config: BenchConfig) {
  logger = new Logger(config.outputFile);

  const now = new Date();
  logger.log("=".repeat(120));
  logger.log("Vector Index Benchmark (NAPI)");
  logger.log("=".repeat(120));
  logger.log(`Date: ${now.toISOString()}`);
  logger.log(`Vectors: ${formatNumber(config.vectors)}`);
  logger.log(`Dimensions: ${config.dimensions}`);
  logger.log(`Iterations: ${formatNumber(config.iterations)}`);
  logger.log(`k: ${config.k}`);
  logger.log(`nProbe: ${config.nProbe}`);
  logger.log("=".repeat(120));

  benchmarkVectorIndex(config);

  logger.log(`\n${"=".repeat(120)}`);
  logger.log("Vector benchmark complete.");
  logger.log("=".repeat(120));

  await logger.flush();
  if (logger.getOutputPath()) {
    console.log(`\nResults saved to: ${logger.getOutputPath()}`);
  }
}

const config = parseArgs();
runBenchmarks(config).catch((err) => {
  console.error("Benchmark failed:", err);
  process.exit(1);
});
