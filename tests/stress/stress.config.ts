/**
 * Stress Test Configuration
 * 
 * Defines parameters for stress tests. Use environment variables to override defaults.
 */

export const STRESS_CONFIG = {
  // Concurrency stress test settings
  concurrency: {
    maxWorkers: Number(process.env.STRESS_MAX_WORKERS) || 50,
    durationSecs: Number(process.env.STRESS_DURATION_SECS) || 30,
    txPerWorker: Number(process.env.STRESS_TX_PER_WORKER) || 500,
    txStormTarget: Number(process.env.STRESS_TX_STORM_TARGET) || 5000,
  },

  // Volume stress test settings
  volume: {
    nodes: Number(process.env.STRESS_NODES) || 100_000,
    edges: Number(process.env.STRESS_EDGES) || 500_000,
    propsPerNode: Number(process.env.STRESS_PROPS_PER_NODE) || 10,
    maxFanOut: Number(process.env.STRESS_MAX_FAN_OUT) || 50_000,
    maxFanIn: Number(process.env.STRESS_MAX_FAN_IN) || 50_000,
  },

  // Version chain stress test settings
  versionChain: {
    maxChainDepth: Number(process.env.STRESS_MAX_CHAIN_DEPTH) || 5000,
    concurrentReaders: Number(process.env.STRESS_CONCURRENT_READERS) || 500,
  },

  // Durability stress test settings
  durability: {
    crashCycles: Number(process.env.STRESS_CRASH_CYCLES) || 50,
    walSizeBytes: Number(process.env.STRESS_WAL_SIZE_BYTES) || 100_000_000, // 100MB
  },

  // GC stress test settings
  gc: {
    txRate: Number(process.env.STRESS_TX_RATE) || 5000,
    gcIntervalMs: Number(process.env.STRESS_GC_INTERVAL_MS) || 100,
    retentionMs: Number(process.env.STRESS_RETENTION_MS) || 1000,
  },

  // Isolation stress test settings
  isolation: {
    iterations: Number(process.env.STRESS_ISOLATION_ITERATIONS) || 1000,
    rangeSize: Number(process.env.STRESS_RANGE_SIZE) || 100,
  },

  // Edge case stress test settings
  edgeCases: {
    iterations: Number(process.env.STRESS_EDGE_CASE_ITERATIONS) || 1000,
    chainLength: Number(process.env.STRESS_CHAIN_LENGTH) || 50_000,
    starSpokes: Number(process.env.STRESS_STAR_SPOKES) || 50_000,
    completeGraphSize: Number(process.env.STRESS_COMPLETE_GRAPH_SIZE) || 500,
  },

  // Quick mode settings (for CI/fast feedback)
  quick: {
    concurrency: {
      maxWorkers: 10,
      durationSecs: 5,
      txPerWorker: 100,
      txStormTarget: 500,
    },
    volume: {
      nodes: 10_000,
      edges: 50_000,
      propsPerNode: 5,
      maxFanOut: 5_000,
      maxFanIn: 5_000,
    },
    versionChain: {
      maxChainDepth: 500,
      concurrentReaders: 50,
    },
    durability: {
      crashCycles: 10,
      walSizeBytes: 10_000_000,
    },
    gc: {
      txRate: 500,
      gcIntervalMs: 100,
      retentionMs: 500,
    },
    isolation: {
      iterations: 100,
      rangeSize: 20,
    },
    edgeCases: {
      iterations: 100,
      chainLength: 5_000,
      starSpokes: 5_000,
      completeGraphSize: 100,
    },
  },
};

/**
 * Get configuration based on mode
 */
export function getConfig(quick = false) {
  if (quick || process.env.STRESS_QUICK === "1") {
    return {
      concurrency: STRESS_CONFIG.quick.concurrency,
      volume: STRESS_CONFIG.quick.volume,
      versionChain: STRESS_CONFIG.quick.versionChain,
      durability: STRESS_CONFIG.quick.durability,
      gc: STRESS_CONFIG.quick.gc,
      isolation: STRESS_CONFIG.quick.isolation,
      edgeCases: STRESS_CONFIG.quick.edgeCases,
    };
  }
  return {
    concurrency: STRESS_CONFIG.concurrency,
    volume: STRESS_CONFIG.volume,
    versionChain: STRESS_CONFIG.versionChain,
    durability: STRESS_CONFIG.durability,
    gc: STRESS_CONFIG.gc,
    isolation: STRESS_CONFIG.isolation,
    edgeCases: STRESS_CONFIG.edgeCases,
  };
}

/**
 * Check if running in quick mode
 */
export function isQuickMode(): boolean {
  return process.argv.includes("--quick") || process.env.STRESS_QUICK === "1";
}
