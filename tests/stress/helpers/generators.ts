/**
 * Graph Data Generators for Stress Tests
 *
 * Provides utilities to generate random graph data for testing.
 */

import type {
  GraphDB,
  NodeID,
  ETypeID,
  PropKeyID,
  PropValue,
  TxHandle,
} from "../../../src/types.ts";
import {
  beginTx,
  commit,
  createNode,
  defineEtype,
  definePropkey,
  addEdge,
  setNodeProp,
} from "../../../src/index.ts";
import { PropValueTag } from "../../../src/types.ts";

const envSeed = process.env.STRESS_SEED;
let seed = Number.isFinite(Number(envSeed)) ? Number(envSeed) >>> 0 : Date.now() >>> 0;
const initialSeed = seed;

console.log(`[STRESS] Using seed: ${initialSeed}`);

export function setSeed(nextSeed: number): void {
  seed = nextSeed >>> 0;
}

export function getSeed(): number {
  return seed >>> 0;
}

function seededRandom(): number {
  let t = (seed += 0x6d2b79f5);
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}

/**
 * Generate a random string of given length
 */
export function randomString(length: number): string {
  const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(seededRandom() * chars.length));
  }
  return result;
}

/**
 * Generate a random integer in range [min, max]
 */
export function randomInt(min: number, max: number): number {
  return Math.floor(seededRandom() * (max - min + 1)) + min;
}

/**
 * Generate a random bigint in range [min, max]
 */
export function randomBigInt(min: bigint, max: bigint): bigint {
  const range = max - min;
  return min + BigInt(Math.floor(seededRandom() * Number(range + 1n)));
}

/**
 * Generate a random property value
 */
export function randomPropValue(): PropValue {
  const tag = randomInt(0, 4);
  switch (tag) {
    case PropValueTag.NULL:
      return { tag: PropValueTag.NULL };
    case PropValueTag.BOOL:
      return { tag: PropValueTag.BOOL, value: seededRandom() > 0.5 };
    case PropValueTag.I64:
      return { tag: PropValueTag.I64, value: BigInt(randomInt(-1000000, 1000000)) };
    case PropValueTag.F64:
      return { tag: PropValueTag.F64, value: seededRandom() * 1000000 };
    case PropValueTag.STRING:
      return { tag: PropValueTag.STRING, value: randomString(randomInt(1, 100)) };
    default:
      return { tag: PropValueTag.NULL };
  }
}

/**
 * Pick a random element from an array
 */
export function randomPick<T>(arr: T[]): T {
  return arr[Math.floor(seededRandom() * arr.length)]!;
}

/**
 * Shuffle an array in place (Fisher-Yates)
 */
export function shuffle<T>(arr: T[]): T[] {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(seededRandom() * (i + 1));
    [arr[i], arr[j]] = [arr[j]!, arr[i]!];
  }
  return arr;
}

/**
 * Batch graph builder for creating large graphs efficiently
 */
export interface GraphBuilderResult {
  nodeIds: NodeID[];
  nodeKeys: string[];
  etypes: Map<string, ETypeID>;
  propKeys: Map<string, PropKeyID>;
}

export interface GraphBuilderOptions {
  nodeCount: number;
  edgeCount: number;
  etypes?: string[];
  propKeys?: string[];
  propsPerNode?: number;
  batchSize?: number;
  onProgress?: (phase: string, current: number, total: number) => void;
}

/**
 * Build a random graph with specified parameters
 */
export async function buildRandomGraph(
  db: GraphDB,
  options: GraphBuilderOptions
): Promise<GraphBuilderResult> {
  const {
    nodeCount,
    edgeCount,
    etypes = ["EDGE"],
    propKeys = [],
    propsPerNode = 0,
    batchSize = 5000,
    onProgress,
  } = options;

  const nodeIds: NodeID[] = [];
  const nodeKeys: string[] = [];
  const etypeMap = new Map<string, ETypeID>();
  const propKeyMap = new Map<string, PropKeyID>();

  // Phase 1: Create edge types and property keys
  {
    const tx = beginTx(db);
    for (const name of etypes) {
      etypeMap.set(name, defineEtype(tx, name));
    }
    for (const name of propKeys) {
      propKeyMap.set(name, definePropkey(tx, name));
    }
    await commit(tx);
  }

  // Phase 2: Create nodes in batches
  for (let batch = 0; batch < nodeCount; batch += batchSize) {
    const tx = beginTx(db);
    const end = Math.min(batch + batchSize, nodeCount);
    
    for (let i = batch; i < end; i++) {
      const key = `node_${i}_${randomString(8)}`;
      const nodeId = createNode(tx, { key });
      nodeIds.push(nodeId);
      nodeKeys.push(key);

      // Add properties if requested
      if (propsPerNode > 0 && propKeyMap.size > 0) {
        const propKeysArr = Array.from(propKeyMap.values());
        const propsToAdd = Math.min(propsPerNode, propKeysArr.length);
        for (let j = 0; j < propsToAdd; j++) {
          setNodeProp(tx, nodeId, propKeysArr[j % propKeysArr.length]!, randomPropValue());
        }
      }
    }
    
    await commit(tx);
    onProgress?.("nodes", end, nodeCount);
  }

  // Phase 3: Create edges in batches
  const etypeArr = Array.from(etypeMap.values());
  let edgesCreated = 0;
  
  while (edgesCreated < edgeCount) {
    const tx = beginTx(db);
    const batchTarget = Math.min(edgesCreated + batchSize, edgeCount);
    
    while (edgesCreated < batchTarget) {
      const src = randomPick(nodeIds);
      const dst = randomPick(nodeIds);
      if (src !== dst) {
        const etype = randomPick(etypeArr);
        addEdge(tx, src, etype, dst);
        edgesCreated++;
      }
    }
    
    await commit(tx);
    onProgress?.("edges", edgesCreated, edgeCount);
  }

  return { nodeIds, nodeKeys, etypes: etypeMap, propKeys: propKeyMap };
}

/**
 * Create a chain topology (linear graph)
 */
export async function buildChainGraph(
  db: GraphDB,
  length: number,
  batchSize = 5000,
  onProgress?: (current: number, total: number) => void
): Promise<{ nodeIds: NodeID[]; etype: ETypeID }> {
  const nodeIds: NodeID[] = [];
  let etype: ETypeID;

  // Create edge type
  {
    const tx = beginTx(db);
    etype = defineEtype(tx, "CHAIN");
    await commit(tx);
  }

  // Create nodes
  for (let batch = 0; batch < length; batch += batchSize) {
    const tx = beginTx(db);
    const end = Math.min(batch + batchSize, length);
    
    for (let i = batch; i < end; i++) {
      const nodeId = createNode(tx, { key: `chain_${i}` });
      nodeIds.push(nodeId);
    }
    
    await commit(tx);
    onProgress?.(end, length);
  }

  // Create edges
  for (let batch = 0; batch < length - 1; batch += batchSize) {
    const tx = beginTx(db);
    const end = Math.min(batch + batchSize, length - 1);
    
    for (let i = batch; i < end; i++) {
      addEdge(tx, nodeIds[i]!, etype, nodeIds[i + 1]!);
    }
    
    await commit(tx);
    onProgress?.(length + end, length * 2);
  }

  return { nodeIds, etype };
}

/**
 * Create a star topology (hub with many spokes)
 */
export async function buildStarGraph(
  db: GraphDB,
  spokes: number,
  batchSize = 5000,
  onProgress?: (current: number, total: number) => void
): Promise<{ hubId: NodeID; spokeIds: NodeID[]; etype: ETypeID }> {
  const spokeIds: NodeID[] = [];
  let hubId: NodeID;
  let etype: ETypeID;

  // Create hub node and edge type
  {
    const tx = beginTx(db);
    etype = defineEtype(tx, "SPOKE");
    hubId = createNode(tx, { key: "hub" });
    await commit(tx);
  }

  // Create spoke nodes
  for (let batch = 0; batch < spokes; batch += batchSize) {
    const tx = beginTx(db);
    const end = Math.min(batch + batchSize, spokes);
    
    for (let i = batch; i < end; i++) {
      const nodeId = createNode(tx, { key: `spoke_${i}` });
      spokeIds.push(nodeId);
    }
    
    await commit(tx);
    onProgress?.(end, spokes * 2);
  }

  // Create edges from hub to spokes
  for (let batch = 0; batch < spokes; batch += batchSize) {
    const tx = beginTx(db);
    const end = Math.min(batch + batchSize, spokes);
    
    for (let i = batch; i < end; i++) {
      addEdge(tx, hubId, etype, spokeIds[i]!);
    }
    
    await commit(tx);
    onProgress?.(spokes + end, spokes * 2);
  }

  return { hubId, spokeIds, etype };
}

/**
 * Create a complete graph (all nodes connected to all other nodes)
 * Warning: n^2 edges!
 */
export async function buildCompleteGraph(
  db: GraphDB,
  size: number,
  batchSize = 5000,
  onProgress?: (current: number, total: number) => void
): Promise<{ nodeIds: NodeID[]; etype: ETypeID }> {
  const nodeIds: NodeID[] = [];
  let etype: ETypeID;
  const totalEdges = size * (size - 1);

  // Create nodes and edge type
  {
    const tx = beginTx(db);
    etype = defineEtype(tx, "COMPLETE");
    for (let i = 0; i < size; i++) {
      const nodeId = createNode(tx, { key: `complete_${i}` });
      nodeIds.push(nodeId);
    }
    await commit(tx);
  }

  // Create all edges
  let edgesCreated = 0;
  let batch: Array<[NodeID, NodeID]> = [];

  for (let i = 0; i < size; i++) {
    for (let j = 0; j < size; j++) {
      if (i !== j) {
        batch.push([nodeIds[i]!, nodeIds[j]!]);
        
        if (batch.length >= batchSize) {
          const tx = beginTx(db);
          for (const [src, dst] of batch) {
            addEdge(tx, src, etype, dst);
          }
          await commit(tx);
          edgesCreated += batch.length;
          batch = [];
          onProgress?.(edgesCreated, totalEdges);
        }
      }
    }
  }

  // Flush remaining edges
  if (batch.length > 0) {
    const tx = beginTx(db);
    for (const [src, dst] of batch) {
      addEdge(tx, src, etype, dst);
    }
    await commit(tx);
    edgesCreated += batch.length;
    onProgress?.(edgesCreated, totalEdges);
  }

  return { nodeIds, etype };
}
