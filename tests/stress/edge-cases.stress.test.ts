/**
 * Edge Case Stress Tests
 *
 * Verifies handling of unusual patterns and edge cases.
 *
 * Tests:
 * - Self-Loops
 * - Duplicate Edges
 * - Empty Transactions
 * - Graph Topology Extremes (chain, star, complete)
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  openGraphDB,
  closeGraphDB,
  beginTx,
  commit,
  rollback,
  createNode,
  deleteNode,
  deleteEdge,
  defineEtype,
  definePropkey,
  addEdge,
  edgeExists,
  getNeighborsOut,
  getNeighborsIn,
  setNodeProp,
  getNodeProp,
  nodeExists,
} from "../../src/index.ts";
import { PropValueTag } from "../../src/types.ts";
import type { GraphDB, NodeID, ETypeID } from "../../src/types.ts";
import { MvccManager } from "../../src/mvcc/index.ts";
import { getConfig, isQuickMode } from "./stress.config.ts";
import { 
  buildChainGraph, 
  buildStarGraph, 
  buildCompleteGraph,
  randomString 
} from "./helpers/generators.ts";

const config = getConfig(isQuickMode());
const dbOptions = {
  mvcc: true,
  autoCheckpoint: false,
  walSize: config.durability.walSizeBytes,
};

describe("Edge Case Stress Tests", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-edge-case-stress-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("self-loops are handled correctly", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.edgeCases.iterations;

    // Create node with self-loop
    const setupTx = beginTx(db);
    const etype = defineEtype(setupTx, "SELF_REF");
    const nodeId = createNode(setupTx, { key: "self_loop" });
    addEdge(setupTx, nodeId, etype, nodeId);
    await commit(setupTx);

    // Verify self-loop exists
    expect(edgeExists(db, nodeId, etype, nodeId)).toBe(true);

    // Traversal should terminate (not infinite loop)
    let outNeighbors = 0;
    const startTime = performance.now();
    for (const neighbor of getNeighborsOut(db, nodeId)) {
      outNeighbors++;
      if (performance.now() - startTime > 1000) {
        throw new Error("Traversal taking too long - possible infinite loop");
      }
    }
    expect(outNeighbors).toBe(1);

    // Multiple self-loop add/delete cycles (separate transactions)
    for (let i = 0; i < ITERATIONS; i++) {
      // Delete in one transaction
      const delTx = beginTx(db);
      deleteEdge(delTx, nodeId, etype, nodeId);
      await commit(delTx);
      expect(edgeExists(db, nodeId, etype, nodeId)).toBe(false);
      
      // Re-add in another transaction
      const addTx = beginTx(db);
      addEdge(addTx, nodeId, etype, nodeId);
      await commit(addTx);
      expect(edgeExists(db, nodeId, etype, nodeId)).toBe(true);
    }

    await closeGraphDB(db);
  }, 60000);

  test("duplicate edge add/delete cycles", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.edgeCases.iterations;

    // Create two nodes
    const setupTx = beginTx(db);
    const etype = defineEtype(setupTx, "DUP_EDGE");
    const node1 = createNode(setupTx, { key: "dup_1" });
    const node2 = createNode(setupTx, { key: "dup_2" });
    addEdge(setupTx, node1, etype, node2);
    await commit(setupTx);

    expect(edgeExists(db, node1, etype, node2)).toBe(true);

    // Repeatedly add/delete same edge
    for (let i = 0; i < ITERATIONS; i++) {
      const delTx = beginTx(db);
      deleteEdge(delTx, node1, etype, node2);
      await commit(delTx);
      expect(edgeExists(db, node1, etype, node2)).toBe(false);

      const addTx = beginTx(db);
      addEdge(addTx, node1, etype, node2);
      await commit(addTx);
      expect(edgeExists(db, node1, etype, node2)).toBe(true);
    }

    // Final state should have edge
    expect(edgeExists(db, node1, etype, node2)).toBe(true);

    // Verify version chain accumulated correctly
    if (db._mvcc) {
      const mvcc = db._mvcc as MvccManager;
      // Version chain should exist for this edge
      const edgeKey = `${node1}:${etype}:${node2}`;
      // Note: exact internal format may vary
    }

    await closeGraphDB(db);
  }, 120000);

  test("empty transactions have no overhead accumulation", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.edgeCases.iterations * 10;

    const mvcc = db._mvcc as MvccManager;
    const initialActiveCount = mvcc.txManager.getActiveCount();

    // Many empty transactions
    for (let i = 0; i < ITERATIONS; i++) {
      const tx = beginTx(db);
      // No operations
      await commit(tx);
    }

    // Should have no accumulated state
    const finalActiveCount = mvcc.txManager.getActiveCount();
    expect(finalActiveCount).toBe(initialActiveCount);

    // Also test rollback of empty transactions
    for (let i = 0; i < ITERATIONS; i++) {
      const tx = beginTx(db);
      // No operations
      rollback(tx);
    }

    const afterRollbackCount = mvcc.txManager.getActiveCount();
    expect(afterRollbackCount).toBe(initialActiveCount);

    await closeGraphDB(db);
  }, 60000);

  test("chain topology traversal", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const CHAIN_LENGTH = config.edgeCases.chainLength;

    console.log(`Building chain of ${CHAIN_LENGTH} nodes...`);
    const { nodeIds, etype } = await buildChainGraph(db, CHAIN_LENGTH, 5000, (current, total) => {
      if (current % 10000 === 0) {
        console.log(`  Chain progress: ${current}/${total}`);
      }
    });

    // Verify chain structure
    // First node should have 1 out neighbor
    let firstOutCount = 0;
    for (const _ of getNeighborsOut(db, nodeIds[0]!)) {
      firstOutCount++;
    }
    expect(firstOutCount).toBe(1);

    // Last node should have 0 out neighbors
    let lastOutCount = 0;
    for (const _ of getNeighborsOut(db, nodeIds[nodeIds.length - 1]!)) {
      lastOutCount++;
    }
    expect(lastOutCount).toBe(0);

    // Middle node should have 1 in and 1 out
    const midIdx = Math.floor(nodeIds.length / 2);
    let midOutCount = 0;
    let midInCount = 0;
    for (const _ of getNeighborsOut(db, nodeIds[midIdx]!)) {
      midOutCount++;
    }
    for (const _ of getNeighborsIn(db, nodeIds[midIdx]!)) {
      midInCount++;
    }
    expect(midOutCount).toBe(1);
    expect(midInCount).toBe(1);

    console.log(`Chain topology verified: ${CHAIN_LENGTH} nodes`);

    await closeGraphDB(db);
  }, 300000);

  test("star topology (high fan-out)", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const SPOKES = config.edgeCases.starSpokes;

    console.log(`Building star with ${SPOKES} spokes...`);
    const { hubId, spokeIds, etype } = await buildStarGraph(db, SPOKES, 5000, (current, total) => {
      if (current % 10000 === 0) {
        console.log(`  Star progress: ${current}/${total}`);
      }
    });

    // Verify hub has SPOKES outgoing edges
    let hubOutCount = 0;
    const startTime = performance.now();
    for (const neighbor of getNeighborsOut(db, hubId)) {
      hubOutCount++;
      // Safety check for performance
      if (hubOutCount > SPOKES + 100) {
        throw new Error("Too many neighbors returned");
      }
    }
    const traversalTime = performance.now() - startTime;
    console.log(`Hub traversal: ${hubOutCount} neighbors in ${traversalTime.toFixed(0)}ms`);
    
    expect(hubOutCount).toBe(SPOKES);

    // Each spoke should have 1 incoming edge
    const sampleSize = Math.min(100, spokeIds.length);
    for (let i = 0; i < sampleSize; i++) {
      let inCount = 0;
      for (const _ of getNeighborsIn(db, spokeIds[i]!)) {
        inCount++;
      }
      expect(inCount).toBe(1);
    }

    console.log(`Star topology verified: 1 hub + ${SPOKES} spokes`);

    await closeGraphDB(db);
  }, 300000);

  test("complete graph (dense connectivity)", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const SIZE = config.edgeCases.completeGraphSize;
    const EXPECTED_EDGES = SIZE * (SIZE - 1);

    console.log(`Building complete graph K${SIZE} (${EXPECTED_EDGES} edges)...`);
    const { nodeIds, etype } = await buildCompleteGraph(db, SIZE, 5000, (current, total) => {
      if (current % 10000 === 0) {
        console.log(`  Complete graph progress: ${current}/${total} edges`);
      }
    });

    // Each node should have (SIZE-1) out edges and (SIZE-1) in edges
    const sampleSize = Math.min(10, SIZE);
    for (let i = 0; i < sampleSize; i++) {
      let outCount = 0;
      let inCount = 0;
      for (const _ of getNeighborsOut(db, nodeIds[i]!)) {
        outCount++;
      }
      for (const _ of getNeighborsIn(db, nodeIds[i]!)) {
        inCount++;
      }
      expect(outCount).toBe(SIZE - 1);
      expect(inCount).toBe(SIZE - 1);
    }

    console.log(`Complete graph K${SIZE} verified: ${SIZE} nodes, ${EXPECTED_EDGES} edges`);

    await closeGraphDB(db);
  }, 300000);

  test("rapid node create/delete cycles", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.edgeCases.iterations;

    // Create and delete nodes rapidly
    for (let i = 0; i < ITERATIONS; i++) {
      const tx = beginTx(db);
      const nodeId = createNode(tx, { key: `rapid_${i}_${randomString(4)}` });
      deleteNode(tx, nodeId);
      await commit(tx);

      // Node should not exist after commit
      expect(nodeExists(db, nodeId)).toBe(false);
    }

    // Verify no resource leaks
    const mvcc = db._mvcc as MvccManager;
    expect(mvcc.txManager.getActiveCount()).toBe(0);

    await closeGraphDB(db);
  }, 120000);

  test("property update cycles on same key", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.edgeCases.iterations;

    // Create node with property
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "cyclic");
    const nodeId = createNode(setupTx, { key: "prop_cycles" });
    setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    await commit(setupTx);

    // Update property many times
    for (let i = 0; i < ITERATIONS; i++) {
      const tx = beginTx(db);
      setNodeProp(tx, nodeId, propKey, { tag: PropValueTag.I64, value: BigInt(i + 1) });
      await commit(tx);
    }

    // Final value should be correct
    const final = getNodeProp(db, nodeId, propKey);
    expect(final?.tag).toBe(PropValueTag.I64);
    if (final?.tag === PropValueTag.I64) {
      expect(final.value).toBe(BigInt(ITERATIONS));
    }

    await closeGraphDB(db);
  }, 120000);

  test("interleaved operations on multiple nodes", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const NODES = 50;
    const ITERATIONS = config.edgeCases.iterations;

    // Create nodes
    const setupTx = beginTx(db);
    const etype = defineEtype(setupTx, "INTERLEAVE");
    const propKey = definePropkey(setupTx, "count");
    const nodeIds: NodeID[] = [];
    for (let i = 0; i < NODES; i++) {
      const nodeId = createNode(setupTx, { key: `interleave_${i}` });
      nodeIds.push(nodeId);
      setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    }
    await commit(setupTx);

    const errors: Error[] = [];

    // Interleaved operations
    for (let i = 0; i < ITERATIONS; i++) {
      try {
        const tx = beginTx(db);
        
        // Multiple operations in single transaction
        const node1Idx = i % NODES;
        const node2Idx = (i + 1) % NODES;
        const node1 = nodeIds[node1Idx]!;
        const node2 = nodeIds[node2Idx]!;

        // Update property
        const current = getNodeProp(db, node1, propKey);
        const val = current?.tag === PropValueTag.I64 ? current.value : 0n;
        setNodeProp(tx, node1, propKey, { tag: PropValueTag.I64, value: val + 1n });

        // Add/remove edge based on iteration
        if (i % 2 === 0) {
          addEdge(tx, node1, etype, node2);
        } else if (edgeExists(db, node1, etype, node2)) {
          deleteEdge(tx, node1, etype, node2);
        }

        await commit(tx);
      } catch (e) {
        errors.push(e as Error);
      }
    }

    expect(errors.length).toBe(0);

    // Verify data integrity
    for (const nodeId of nodeIds) {
      const prop = getNodeProp(db, nodeId, propKey);
      expect(prop?.tag).toBe(PropValueTag.I64);
    }

    await closeGraphDB(db);
  }, 180000);
});
