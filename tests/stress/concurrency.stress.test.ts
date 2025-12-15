/**
 * Concurrency Stress Tests
 *
 * Verifies correctness under high concurrency, not just performance.
 *
 * Tests:
 * - Reader-Writer Contention
 * - Write-Write Conflicts
 * - Long-Running Readers
 * - Transaction Storms
 * - Rollback Storms
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
  setNodeProp,
  getNodeProp,
  getNodeByKey,
  nodeExists,
  defineEtype,
  definePropkey,
  addEdge,
} from "../../src/index.ts";
import { PropValueTag, ConflictError } from "../../src/types.ts";
import type { GraphDB, NodeID, ETypeID, PropKeyID } from "../../src/types.ts";
import { MvccManager } from "../../src/mvcc/index.ts";
import { getConfig, isQuickMode } from "./stress.config.ts";
import { randomString, randomInt } from "./helpers/generators.ts";

const config = getConfig(isQuickMode());

describe("Concurrency Stress Tests", () => {
  let testDir: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-concurrency-stress-"));
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("reader-writer contention maintains snapshot isolation", async () => {
    const db = await openGraphDB(testDir, { mvcc: true });
    const READERS = Math.min(config.concurrency.maxWorkers, 20);
    const WRITERS = Math.min(config.concurrency.maxWorkers / 5, 5);
    const ITERATIONS = Math.min(config.concurrency.txPerWorker, 200);

    // Setup: create initial nodes
    const tx = beginTx(db);
    const propKey = definePropkey(tx, "counter");
    const nodeIds: NodeID[] = [];
    for (let i = 0; i < 10; i++) {
      const nodeId = createNode(tx, { key: `shared_${i}` });
      nodeIds.push(nodeId);
      setNodeProp(tx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    }
    await commit(tx);

    const errors: Error[] = [];
    const readResults: bigint[][] = [];

    // Spawn readers that take snapshots and verify consistency
    const readerPromises = Array.from({ length: READERS }, async (_, readerId) => {
      const reads: bigint[] = [];
      for (let i = 0; i < ITERATIONS; i++) {
        const rtx = beginTx(db);
        try {
          const readOnce = () => {
            const values: bigint[] = [];
            for (const nodeId of nodeIds) {
              const prop = getNodeProp(rtx, nodeId, propKey);
              if (prop && prop.tag === PropValueTag.I64) {
                values.push(prop.value);
              }
            }
            return values;
          };

          const firstRead = readOnce();
          const secondRead = readOnce();

          expect(secondRead).toEqual(firstRead);
          reads.push(...firstRead);
        } catch (e) {
          errors.push(e as Error);
        } finally {
          rollback(rtx);
        }
        // Small delay to interleave with writers
        if (i % 10 === 0) await new Promise(r => setTimeout(r, 1));
      }
      readResults[readerId] = reads;
    });

    // Spawn writers that increment counters
    const writerPromises = Array.from({ length: WRITERS }, async (_, writerId) => {
      for (let i = 0; i < ITERATIONS; i++) {
        const wtx = beginTx(db);
        try {
          const nodeIdx = randomInt(0, nodeIds.length - 1);
          const nodeId = nodeIds[nodeIdx]!;
          const current = getNodeProp(db, nodeId, propKey);
          const currentVal = current?.tag === PropValueTag.I64 ? current.value : 0n;
          setNodeProp(wtx, nodeId, propKey, { tag: PropValueTag.I64, value: currentVal + 1n });
          await commit(wtx);
        } catch (e) {
          if (e instanceof ConflictError) {
            rollback(wtx);
          } else {
            errors.push(e as Error);
            rollback(wtx);
          }
        }
      }
    });

    await Promise.all([...readerPromises, ...writerPromises]);

    expect(errors.length).toBe(0);
    
    // All reads should return valid i64 values
    for (const reads of readResults) {
      for (const val of reads) {
        expect(typeof val).toBe("bigint");
        expect(val).toBeGreaterThanOrEqual(0n);
      }
    }

    await closeGraphDB(db);
  }, 120000);

  test("concurrent writers detect all conflicts (no lost updates)", async () => {
    const db = await openGraphDB(testDir, { mvcc: true });
    const WRITERS = config.concurrency.maxWorkers;

    // Setup: create a shared node
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "value");
    const sharedNode = createNode(setupTx, { key: "shared" });
    setNodeProp(setupTx, sharedNode, propKey, { tag: PropValueTag.I64, value: 0n });
    await commit(setupTx);

    let successes = 0;
    let conflicts = 0;
    const errors: Error[] = [];

    // Begin ALL transactions synchronously FIRST to ensure they all have the same
    // startTs (snapshot timestamp). This simulates true concurrency where all
    // transactions start before any commits.
    const txHandles: Array<{ tx: ReturnType<typeof beginTx>; i: number }> = [];
    for (let i = 0; i < WRITERS; i++) {
      txHandles.push({ tx: beginTx(db), i });
    }

    // Now execute all writes and commits concurrently
    const promises = txHandles.map(async ({ tx, i }) => {
      try {
        // Read and modify
        const current = getNodeProp(db, sharedNode, propKey);
        const val = current?.tag === PropValueTag.I64 ? current.value : 0n;
        setNodeProp(tx, sharedNode, propKey, { tag: PropValueTag.I64, value: val + 1n });
        await commit(tx);
        successes++;
      } catch (e) {
        if (e instanceof ConflictError) {
          conflicts++;
          rollback(tx);
        } else {
          errors.push(e as Error);
          rollback(tx);
        }
      }
    });

    await Promise.all(promises);

    expect(errors.length).toBe(0);
    // At least one writer should succeed
    expect(successes).toBeGreaterThan(0);
    // Total should equal WRITERS (all either succeed or conflict)
    expect(successes + conflicts).toBe(WRITERS);

    const finalVal = getNodeProp(db, sharedNode, propKey);
    expect(finalVal?.tag).toBe(PropValueTag.I64);
    if (finalVal?.tag === PropValueTag.I64) {
      expect(finalVal.value).toBe(BigInt(successes));
      console.log(`Concurrent writers: ${successes} succeeded, ${conflicts} conflicts, final value: ${finalVal.value}`);
    }

    await closeGraphDB(db);
  }, 60000);

  test("long-running reader sees consistent snapshot while writers commit", async () => {
    const db = await openGraphDB(testDir, { mvcc: true });
    const UPDATES = Math.min(config.concurrency.txPerWorker, 500);

    // Setup: create nodes
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "version");
    const nodeIds: NodeID[] = [];
    for (let i = 0; i < 100; i++) {
      const nodeId = createNode(setupTx, { key: `node_${i}` });
      nodeIds.push(nodeId);
      setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    }
    await commit(setupTx);

    // Start a long-running reader
    const readerTx = beginTx(db);
    const initialValues = new Map<NodeID, bigint>();
    for (const nodeId of nodeIds) {
      const prop = getNodeProp(readerTx, nodeId, propKey);
      if (prop?.tag === PropValueTag.I64) {
        initialValues.set(nodeId, prop.value);
      }
    }

    // Have many transactions commit while reader is active
    for (let i = 0; i < UPDATES; i++) {
      const wtx = beginTx(db);
      const nodeIdx = randomInt(0, nodeIds.length - 1);
      const nodeId = nodeIds[nodeIdx]!;
      setNodeProp(wtx, nodeId, propKey, { tag: PropValueTag.I64, value: BigInt(i + 1) });
      await commit(wtx);
    }

    // Check if reader sees changes or not
    let changedCount = 0;
    for (const nodeId of nodeIds) {
      const prop = getNodeProp(readerTx, nodeId, propKey);
      const expected = initialValues.get(nodeId);
      if (prop?.tag === PropValueTag.I64 && expected !== undefined) {
        if (prop.value !== expected) {
          changedCount++;
        }
      }
    }

    rollback(readerTx);

    console.log(`Long-running reader: ${changedCount}/${nodeIds.length} values changed during read`);
    
    expect(changedCount).toBe(0);

    await closeGraphDB(db);
  }, 120000);

  test("transaction storms (rapid begin/commit cycles)", async () => {
    const db = await openGraphDB(testDir, { mvcc: true });
    const TARGET_TX = config.concurrency.txStormTarget;

    const startTime = performance.now();
    let completedTx = 0;
    const errors: Error[] = [];

    // Rapid transaction cycles
    const promises = Array.from({ length: Math.min(TARGET_TX, 1000) }, async (_, i) => {
      for (let j = 0; j < Math.ceil(TARGET_TX / 1000); j++) {
        const tx = beginTx(db);
        try {
          createNode(tx, { key: `storm_${i}_${j}_${randomString(8)}` });
          await commit(tx);
          completedTx++;
        } catch (e) {
          errors.push(e as Error);
          rollback(tx);
        }
      }
    });

    await Promise.all(promises);

    const elapsedMs = performance.now() - startTime;
    const txPerSec = (completedTx / elapsedMs) * 1000;

    console.log(`Transaction storm: ${completedTx} tx in ${elapsedMs.toFixed(0)}ms (${txPerSec.toFixed(0)} tx/sec)`);

    expect(errors.length).toBe(0);
    expect(completedTx).toBeGreaterThan(0);

    // Verify no resource leaks
    const mvcc = db._mvcc as MvccManager;
    // Active transactions should be 0 after all commits
    expect(mvcc.txManager.getActiveCount()).toBe(0);

    await closeGraphDB(db);
  }, 180000);

  test("rollback storms cleanup properly", async () => {
    const db = await openGraphDB(testDir, { mvcc: true });
    const ROLLBACKS = Math.min(config.concurrency.txStormTarget, 2000);

    const mvcc = db._mvcc as MvccManager;
    const initialActiveTx = mvcc.txManager.getActiveCount();

    const errors: Error[] = [];
    let rolledBack = 0;

    // Rapid rollback cycles
    const promises = Array.from({ length: Math.min(ROLLBACKS, 100) }, async (_, i) => {
      for (let j = 0; j < ROLLBACKS / 100; j++) {
        const tx = beginTx(db);
        try {
          // Do some work
          createNode(tx, { key: `rollback_${i}_${j}_${randomString(8)}` });
          // But rollback instead of commit
          rollback(tx);
          rolledBack++;
        } catch (e) {
          errors.push(e as Error);
        }
      }
    });

    await Promise.all(promises);

    expect(errors.length).toBe(0);
    expect(rolledBack).toBe(ROLLBACKS);

    // All transactions should be cleaned up
    expect(mvcc.txManager.getActiveCount()).toBe(initialActiveTx);

    // Rolled back nodes should not exist
    expect(getNodeByKey(db, "rollback_0_0")).toBeNull();

    await closeGraphDB(db);
  }, 60000);

  test("mixed concurrent operations with conflict detection", async () => {
    const db = await openGraphDB(testDir, { mvcc: true });
    const WORKERS = config.concurrency.maxWorkers;
    const OPS_PER_WORKER = Math.min(config.concurrency.txPerWorker, 100);

    // Setup: create some initial nodes
    const setupTx = beginTx(db);
    const etype = defineEtype(setupTx, "RELATES");
    const propKey = definePropkey(setupTx, "data");
    const nodeIds: NodeID[] = [];
    for (let i = 0; i < 50; i++) {
      nodeIds.push(createNode(setupTx, { key: `base_${i}` }));
    }
    await commit(setupTx);

    interface Stats {
      creates: number;
      updates: number;
      deletes: number;
      edges: number;
      conflicts: number;
      errors: number;
    }

    const workerStats: Stats[] = [];
    const errors: Error[] = [];

    const promises = Array.from({ length: WORKERS }, async (_, workerId) => {
      const stats: Stats = { creates: 0, updates: 0, deletes: 0, edges: 0, conflicts: 0, errors: 0 };
      workerStats[workerId] = stats;

      for (let i = 0; i < OPS_PER_WORKER; i++) {
        const tx = beginTx(db);
        try {
          const op = randomInt(0, 3);
          switch (op) {
            case 0: // Create
              createNode(tx, { key: `worker_${workerId}_${i}_${randomString(4)}` });
              stats.creates++;
              break;
            case 1: // Update existing
              if (nodeIds.length > 0) {
                const nodeId = nodeIds[randomInt(0, nodeIds.length - 1)]!;
                setNodeProp(tx, nodeId, propKey, { 
                  tag: PropValueTag.STRING, 
                  value: `${workerId}_${i}` 
                });
                stats.updates++;
              }
              break;
            case 2: // Add edge
              if (nodeIds.length >= 2) {
                const src = nodeIds[randomInt(0, nodeIds.length - 1)]!;
                const dst = nodeIds[randomInt(0, nodeIds.length - 1)]!;
                if (src !== dst) {
                  addEdge(tx, src, etype, dst);
                  stats.edges++;
                }
              }
              break;
            case 3: // Read (and maybe conflict)
              if (nodeIds.length > 0) {
                const nodeId = nodeIds[randomInt(0, nodeIds.length - 1)]!;
                getNodeProp(db, nodeId, propKey);
                // Small chance to also write (creating potential conflict)
                if (Math.random() < 0.3) {
                  setNodeProp(tx, nodeId, propKey, {
                    tag: PropValueTag.I64,
                    value: BigInt(workerId * 1000 + i),
                  });
                }
              }
              break;
          }
          await commit(tx);
        } catch (e) {
          if (e instanceof ConflictError) {
            stats.conflicts++;
            rollback(tx);
          } else {
            stats.errors++;
            errors.push(e as Error);
            rollback(tx);
          }
        }
      }
    });

    await Promise.all(promises);

    // Aggregate stats
    const totalStats = workerStats.reduce(
      (acc, s) => ({
        creates: acc.creates + s.creates,
        updates: acc.updates + s.updates,
        deletes: acc.deletes + s.deletes,
        edges: acc.edges + s.edges,
        conflicts: acc.conflicts + s.conflicts,
        errors: acc.errors + s.errors,
      }),
      { creates: 0, updates: 0, deletes: 0, edges: 0, conflicts: 0, errors: 0 }
    );

    console.log(`Mixed operations: ${JSON.stringify(totalStats)}`);

    expect(totalStats.errors).toBe(0);
    expect(errors.length).toBe(0);
    // Should have some conflicts with concurrent updates
    // (but not required - depends on timing)

    await closeGraphDB(db);
  }, 120000);
});
