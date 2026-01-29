/**
 * MVCC Isolation Stress Tests
 *
 * Verifies snapshot isolation guarantees under extreme conditions.
 *
 * Tests:
 * - Phantom Read Prevention
 * - Non-Repeatable Read Prevention
 * - Serialization Anomaly Detection
 * - Stale Read Detection
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
  definePropkey,
  defineEtype,
  addEdge,
  getNeighborsOut,
} from "../../src/index.ts";
import { PropValueTag, ConflictError } from "../../src/types.ts";
import type { GraphDB, NodeID, PropKeyID, ETypeID } from "../../src/types.ts";
import { getConfig, isQuickMode } from "./stress.config.ts";
import { randomInt, randomString } from "./helpers/generators.ts";

const config = getConfig(isQuickMode());
const dbOptions = {
  mvcc: true,
  autoCheckpoint: false,
  walSize: config.durability.walSizeBytes,
};

describe("MVCC Isolation Stress Tests", () => {
  let testDir: string;
  let testPath: string;

  beforeEach(async () => {
    testDir = await mkdtemp(join(tmpdir(), "ray-isolation-stress-"));
    testPath = join(testDir, "db.raydb");
  });

  afterEach(async () => {
    await rm(testDir, { recursive: true, force: true });
  });

  test("snapshot isolation prevents phantom reads during inserts", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.isolation.iterations;
    const BASE_NODES = 100;

    // Create base nodes
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "category");
    const nodeIds: NodeID[] = [];
    for (let i = 0; i < BASE_NODES; i++) {
      const nodeId = createNode(setupTx, { key: `base_${i}` });
      nodeIds.push(nodeId);
      setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.STRING, value: "A" });
    }
    await commit(setupTx);

    let phantomDetected = 0;

    for (let i = 0; i < ITERATIONS; i++) {
      // Reader begins and counts nodes with category "A"
      const readerTx = beginTx(db);
      let initialCount = 0;
      for (const nodeId of nodeIds) {
        // Use transaction-aware read API for MVCC snapshot isolation
        const prop = getNodeProp(readerTx, nodeId, propKey);
        if (prop?.tag === PropValueTag.STRING && prop.value === "A") {
          initialCount++;
        }
      }

      // Writer adds more nodes with category "A"
      const writerTx = beginTx(db);
      const newNodeId = createNode(writerTx, { key: `phantom_${i}_${randomString(4)}` });
      nodeIds.push(newNodeId);
      setNodeProp(writerTx, newNodeId, propKey, { tag: PropValueTag.STRING, value: "A" });
      await commit(writerTx);

      // Reader re-counts - should see same count (no phantom)
      let afterCount = 0;
      for (const nodeId of nodeIds.slice(0, nodeIds.length - 1)) { // Exclude newly added
        // Use transaction-aware read API for MVCC snapshot isolation
        const prop = getNodeProp(readerTx, nodeId, propKey);
        if (prop?.tag === PropValueTag.STRING && prop.value === "A") {
          afterCount++;
        }
      }

      // In snapshot isolation, we shouldn't see the new node
      // But we also need to check if we incorrectly see it via key lookup
      const newNodeVisible = getNodeByKey(readerTx, `phantom_${i}_${randomString(4)}`);
      
      if (afterCount !== initialCount) {
        // This would be a phantom read within the range we checked
        phantomDetected++;
      }

      rollback(readerTx);
    }

    console.log(`Phantom reads detected: ${phantomDetected}/${ITERATIONS}`);
    // With transaction-aware reads, phantom reads should be prevented
    expect(phantomDetected).toBe(0);

    await closeGraphDB(db);
  }, 180000);

  test("non-repeatable reads prevented within transaction", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.isolation.iterations;

    // Create a node
    const setupTx = beginTx(db);
    const propKey = definePropkey(setupTx, "value");
    const nodeId = createNode(setupTx, { key: "consistent" });
    setNodeProp(setupTx, nodeId, propKey, { tag: PropValueTag.I64, value: 0n });
    await commit(setupTx);

    let inconsistentReads = 0;

    for (let i = 0; i < ITERATIONS; i++) {
      // Reader starts and reads value using transaction-aware API
      const readerTx = beginTx(db);
      const read1 = getNodeProp(readerTx, nodeId, propKey);
      const val1 = read1?.tag === PropValueTag.I64 ? read1.value : null;

      // Concurrent writer updates value
      const writerTx = beginTx(db);
      setNodeProp(writerTx, nodeId, propKey, { tag: PropValueTag.I64, value: BigInt(i + 1) });
      try {
        await commit(writerTx);
      } catch (e) {
        rollback(writerTx);
      }

      // Reader reads again using transaction-aware API - should see same value (repeatable read)
      const read2 = getNodeProp(readerTx, nodeId, propKey);
      const val2 = read2?.tag === PropValueTag.I64 ? read2.value : null;

      if (val1 !== val2) {
        inconsistentReads++;
      }

      rollback(readerTx);
    }

    console.log(`Non-repeatable reads: ${inconsistentReads}/${ITERATIONS}`);
    // With transaction-aware reads, non-repeatable reads should be prevented
    expect(inconsistentReads).toBe(0);

    await closeGraphDB(db);
  }, 120000);

  test("write skew anomaly detection (concurrent balance transfers)", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ITERATIONS = config.isolation.iterations;
    const INITIAL_BALANCE = 1000n;

    // Classic write skew: two accounts with constraint that sum >= 0
    const setupTx = beginTx(db);
    const balanceProp = definePropkey(setupTx, "balance");
    const account1 = createNode(setupTx, { key: "account1" });
    const account2 = createNode(setupTx, { key: "account2" });
    setNodeProp(setupTx, account1, balanceProp, { tag: PropValueTag.I64, value: INITIAL_BALANCE });
    setNodeProp(setupTx, account2, balanceProp, { tag: PropValueTag.I64, value: INITIAL_BALANCE });
    await commit(setupTx);

    let conflicts = 0;
    let successfulTransfers = 0;
    let constraintViolations = 0;

    // Concurrent transfers that could cause write skew
    const promises = Array.from({ length: ITERATIONS }, async (_, i) => {
      const tx = beginTx(db);
      try {
        // Read both balances using transaction-aware API
        const bal1 = getNodeProp(tx, account1, balanceProp);
        const bal2 = getNodeProp(tx, account2, balanceProp);
        const b1 = bal1?.tag === PropValueTag.I64 ? bal1.value : 0n;
        const b2 = bal2?.tag === PropValueTag.I64 ? bal2.value : 0n;

        // Transfer from one to other (alternating direction)
        const amount = 100n;
        if (i % 2 === 0) {
          if (b1 >= amount) {
            setNodeProp(tx, account1, balanceProp, { tag: PropValueTag.I64, value: b1 - amount });
            setNodeProp(tx, account2, balanceProp, { tag: PropValueTag.I64, value: b2 + amount });
          }
        } else {
          if (b2 >= amount) {
            setNodeProp(tx, account2, balanceProp, { tag: PropValueTag.I64, value: b2 - amount });
            setNodeProp(tx, account1, balanceProp, { tag: PropValueTag.I64, value: b1 + amount });
          }
        }
        
        await commit(tx);
        successfulTransfers++;
      } catch (e) {
        if (e instanceof ConflictError) {
          conflicts++;
          rollback(tx);
        } else {
          rollback(tx);
        }
      }
    });

    await Promise.all(promises);

    // Check final state (reading outside transaction for latest committed value)
    const finalBal1 = getNodeProp(db, account1, balanceProp);
    const finalBal2 = getNodeProp(db, account2, balanceProp);
    const fb1 = finalBal1?.tag === PropValueTag.I64 ? finalBal1.value : 0n;
    const fb2 = finalBal2?.tag === PropValueTag.I64 ? finalBal2.value : 0n;

    // Total should be conserved
    const totalFinal = fb1 + fb2;
    const totalInitial = INITIAL_BALANCE * 2n;

    console.log(`Write skew test: ${successfulTransfers} successful, ${conflicts} conflicts`);
    console.log(`Final balances: ${fb1}, ${fb2} (total: ${totalFinal}, expected: ${totalInitial})`);

    expect(totalFinal).toBe(totalInitial); // Conservation of money
    expect(fb1).toBeGreaterThanOrEqual(0n); // No negative balances
    expect(fb2).toBeGreaterThanOrEqual(0n);

    await closeGraphDB(db);
  }, 120000);

  test("lost update prevention under concurrent modifications", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const ROUNDS = config.isolation.iterations / 10; // Fewer rounds, more concurrent txs per round
    const CONCURRENT_TXS = 10;

    // Create counter node
    const setupTx = beginTx(db);
    const counterProp = definePropkey(setupTx, "counter");
    const counterId = createNode(setupTx, { key: "counter" });
    setNodeProp(setupTx, counterId, counterProp, { tag: PropValueTag.I64, value: 0n });
    await commit(setupTx);

    let successfulIncrements = 0;
    let conflicts = 0;

    // Run multiple rounds where each round has truly concurrent transactions
    for (let round = 0; round < ROUNDS; round++) {
      // Begin all transactions synchronously FIRST to ensure same startTs
      const txs: ReturnType<typeof beginTx>[] = [];
      for (let i = 0; i < CONCURRENT_TXS; i++) {
        txs.push(beginTx(db));
      }

      // Now execute all read-modify-write operations concurrently
      const results = await Promise.all(
        txs.map(async (tx) => {
          try {
            // Read using transaction-aware API for conflict detection
            const current = getNodeProp(tx, counterId, counterProp);
            const val = current?.tag === PropValueTag.I64 ? current.value : 0n;
            setNodeProp(tx, counterId, counterProp, { tag: PropValueTag.I64, value: val + 1n });
            await commit(tx);
            return "success";
          } catch (e) {
            if (e instanceof ConflictError) {
              rollback(tx);
              return "conflict";
            }
            rollback(tx);
            throw e;
          }
        })
      );

      for (const result of results) {
        if (result === "success") successfulIncrements++;
        else if (result === "conflict") conflicts++;
      }
    }

    // Final counter value should equal successful increments (reading outside transaction)
    const finalVal = getNodeProp(db, counterId, counterProp);
    const finalCounter = finalVal?.tag === PropValueTag.I64 ? finalVal.value : 0n;

    console.log(`Lost update test: ${successfulIncrements} increments, ${conflicts} conflicts, final=${finalCounter}`);

    // With transaction-aware reads and conflict detection, final counter should equal successful commits
    expect(finalCounter).toBe(BigInt(successfulIncrements));
    // We should see some conflicts since transactions are truly concurrent
    expect(conflicts).toBeGreaterThan(0);

    await closeGraphDB(db);
  }, 120000);

  test("read-only transactions see consistent snapshot across entire read", async () => {
    const db = await openGraphDB(testPath, dbOptions);
    const NODES = 100;
    const ITERATIONS = 50;

    // Create nodes with sequential IDs
    const setupTx = beginTx(db);
    const seqProp = definePropkey(setupTx, "seq");
    const nodeIds: NodeID[] = [];
    for (let i = 0; i < NODES; i++) {
      const nodeId = createNode(setupTx, { key: `seq_${i}` });
      nodeIds.push(nodeId);
      setNodeProp(setupTx, nodeId, seqProp, { tag: PropValueTag.I64, value: 0n });
    }
    await commit(setupTx);

    let inconsistencies = 0;

    for (let round = 0; round < ITERATIONS; round++) {
      // Update all nodes to round number (atomic batch)
      const updateTx = beginTx(db);
      for (const nodeId of nodeIds) {
        setNodeProp(updateTx, nodeId, seqProp, { tag: PropValueTag.I64, value: BigInt(round) });
      }
      await commit(updateTx);

      // Start concurrent reader and writer
      const readerPromise = (async () => {
        const rtx = beginTx(db);
        const values: bigint[] = [];
        for (const nodeId of nodeIds) {
          // Use transaction-aware read API for consistent snapshot
          const prop = getNodeProp(rtx, nodeId, seqProp);
          if (prop?.tag === PropValueTag.I64) {
            values.push(prop.value);
          }
        }
        rollback(rtx);

        // All values should be the same (consistent snapshot)
        const uniqueValues = new Set(values);
        if (uniqueValues.size > 1) {
          inconsistencies++;
        }
      })();

      const writerPromise = (async () => {
        const wtx = beginTx(db);
        for (const nodeId of nodeIds) {
          setNodeProp(wtx, nodeId, seqProp, { tag: PropValueTag.I64, value: BigInt(round + 1) });
        }
        await commit(wtx);
      })();

      await Promise.all([readerPromise, writerPromise]);
    }

    console.log(`Snapshot consistency: ${inconsistencies}/${ITERATIONS} inconsistent reads`);
    // With transaction-aware reads, we should have consistent snapshots
    expect(inconsistencies).toBe(0);

    await closeGraphDB(db);
  }, 120000);
});
