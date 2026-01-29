/**
 * Version Chain Stress Tests
 *
 * Verifies version chain management under load.
 *
 * Tests:
 * - Deep Version Chains
 * - Wide Version Spread
 * - GC Under Active Readers
 * - Memory Pressure
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
  setNodeProp,
  getNodeProp,
  definePropkey,
} from "../../src/index.ts";
import { PropValueTag } from "../../src/types.ts";
import type { GraphDB, NodeID, PropKeyID } from "../../src/types.ts";
import { MvccManager } from "../../src/mvcc/index.ts";
import { getConfig, isQuickMode } from "./stress.config.ts";

const config = getConfig(isQuickMode());
const dbOptions = {
  mvcc: true,
  autoCheckpoint: false,
  walSize: config.durability.walSizeBytes,
};

describe("Version Chain Stress Tests", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-vchain-stress-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("deep version chains (many updates to same node)", async () => {
    const db = await openGraphDB(testPath, { 
      ...dbOptions,
      mvccGcInterval: 60000, // Disable GC during test
      mvccRetentionMs: 300000,
    });
    const UPDATES = config.versionChain.maxChainDepth;

    // Create a node and update it many times
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "version");
    const nodeId = createNode(setupTx, { key: "deep_chain" });
    setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    await commit(setupTx);

    // Start a long-running reader to ensure version chains are created
    const readerTx = beginTx(db);

    // Accumulate a deep version chain
    for (let i = 1; i <= UPDATES; i++) {
      const tx = beginTx(db);
      setNodeProp(tx, nodeId, propKey, { tag: PropValueTag.I64, value: BigInt(i) });
      await commit(tx);
      
      if (i % 1000 === 0) {
        console.log(`Deep chain: ${i}/${UPDATES} updates`);
      }
    }

    // Clean up reader
    rollback(readerTx);

    // Verify we can read the latest version
    const latestVal = getNodeProp(db, nodeId, propKey);
    expect(latestVal?.tag).toBe(PropValueTag.I64);
    if (latestVal?.tag === PropValueTag.I64) {
      expect(latestVal.value).toBe(BigInt(UPDATES));
    }

    // Check version chain depth
    const mvcc = db._mvcc as MvccManager;
    const version = mvcc.versionChain.getNodePropVersion(nodeId, propKey);
    
    let depth = 0;
    let current = version;
    while (current !== null) {
      depth++;
      current = current.prev;
    }
    
    console.log(`Version chain depth: ${depth}`);
    // With concurrent reader, version chain should exist
    // Note: Depth may be less than UPDATES due to optimization or GC
    expect(depth).toBeGreaterThan(0);

    await closeGraphDB(db);
  }, 300000);

  test("wide version spread (many concurrent readers at different snapshots)", async () => {
    const db = await openGraphDB(testPath, { 
      ...dbOptions,
      mvccGcInterval: 60000,
      mvccRetentionMs: 300000,
    });
    const READERS = config.versionChain.concurrentReaders;
    const UPDATES = Math.min(READERS, 500);

    // Create a node
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "version");
    const nodeId = createNode(setupTx, { key: "wide_spread" });
    setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    await commit(setupTx);

    // Start readers at different snapshot points
    interface ReaderState {
      tx: ReturnType<typeof beginTx>;
      expectedVersion: bigint;
    }
    const readers: ReaderState[] = [];

    // Interleave reader starts with updates
    for (let i = 1; i <= UPDATES; i++) {
      // Start some readers before this update
      const readersToStart = Math.floor(READERS / UPDATES);
      for (let j = 0; j < readersToStart; j++) {
        const rtx = beginTx(db);
        readers.push({ tx: rtx, expectedVersion: BigInt(i - 1) });
      }

      // Do the update
      const wtx = beginTx(db);
      setNodeProp(wtx, nodeId, propKey, { tag: PropValueTag.I64, value: BigInt(i) });
      await commit(wtx);
    }

    // Verify each reader sees their expected version
    let mismatches = 0;

    for (const reader of readers) {
      const val = getNodeProp(reader.tx, nodeId, propKey);
      expect(val?.tag).toBe(PropValueTag.I64);
      if (val?.tag === PropValueTag.I64) {
        if (val.value !== reader.expectedVersion) {
          mismatches++;
        }
      } else {
        mismatches++;
      }
      rollback(reader.tx);
    }

    console.log(`Wide spread: ${readers.length - mismatches} correct, ${mismatches} mismatched reads`);
    expect(mismatches).toBe(0);

    await closeGraphDB(db);
  }, 120000);

  test("GC under active readers preserves needed versions", async () => {
    const db = await openGraphDB(testPath, { 
      ...dbOptions,
      mvccGcInterval: 100, // Aggressive GC
      mvccRetentionMs: 100,
    });
    const UPDATES = 100;

    // Create a node
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "version");
    const nodeId = createNode(setupTx, { key: "gc_test" });
    setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    await commit(setupTx);

    // Start a long-running reader
    const readerTx = beginTx(db);
    const initialVal = getNodeProp(readerTx, nodeId, propKey);
    expect(initialVal?.tag).toBe(PropValueTag.I64);
    const initialVersion = initialVal?.tag === PropValueTag.I64 ? initialVal.value : -1n;

    // Do many updates that should trigger GC
    for (let i = 1; i <= UPDATES; i++) {
      const tx = beginTx(db);
      setNodeProp(tx, nodeId, propKey, { tag: PropValueTag.I64, value: BigInt(i) });
      await commit(tx);
    }

    // Force GC multiple times
    const mvcc = db._mvcc as MvccManager;
    for (let i = 0; i < 10; i++) {
      mvcc.gc.forceGc();
      await new Promise(r => setTimeout(r, 50));
    }

    // Reader should still be able to read (no crash/corruption)
    const afterGcVal = getNodeProp(readerTx, nodeId, propKey);
    expect(afterGcVal?.tag).toBe(PropValueTag.I64);
    expect(afterGcVal?.tag === PropValueTag.I64 ? afterGcVal.value : -1n).toBe(initialVersion);

    rollback(readerTx);

    // After reader closes, GC can clean up
    mvcc.gc.forceGc();
    
    const gcStats = mvcc.gc.getStats();
    console.log(`GC stats: ${gcStats.gcRuns} runs, ${gcStats.versionsPruned} versions pruned`);
    expect(gcStats.gcRuns).toBeGreaterThan(0);

    await closeGraphDB(db);
  }, 60000);

  test("version chain integrity under heavy updates", async () => {
    const db = await openGraphDB(testPath, { 
      ...dbOptions,
      mvccGcInterval: 1000,
      mvccRetentionMs: 5000,
    });
    const NODES = 100;
    const UPDATES_PER_NODE = 50;

    // Create nodes
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "counter");
    const nodeIds: NodeID[] = [];
    for (let i = 0; i < NODES; i++) {
      const nodeId = createNode(setupTx, { key: `integrity_${i}` });
      nodeIds.push(nodeId);
      setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    }
    await commit(setupTx);

    // Track expected final values
    const expectedValues = new Map<NodeID, bigint>();
    for (const nodeId of nodeIds) {
      expectedValues.set(nodeId, 0n);
    }

    // Random updates
    const errors: Error[] = [];
    for (let round = 0; round < UPDATES_PER_NODE; round++) {
      // Update each node once per round
      for (const nodeId of nodeIds) {
        const tx = beginTx(db);
        try {
          const newVal = expectedValues.get(nodeId)! + 1n;
          setNodeProp(tx, nodeId, propKey, { tag: PropValueTag.I64, value: newVal });
          await commit(tx);
          expectedValues.set(nodeId, newVal);
        } catch (e) {
          errors.push(e as Error);
          rollback(tx);
        }
      }
    }

    expect(errors.length).toBe(0);

    // Verify all final values
    let mismatches = 0;
    for (const nodeId of nodeIds) {
      const actual = getNodeProp(db, nodeId, propKey);
      const expected = expectedValues.get(nodeId);
      if (actual?.tag === PropValueTag.I64) {
        if (actual.value !== expected) {
          mismatches++;
        }
      } else {
        mismatches++;
      }
    }

    expect(mismatches).toBe(0);

    await closeGraphDB(db);
  }, 120000);
});
