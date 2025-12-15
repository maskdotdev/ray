/**
 * MVCC Conflict Detection
 * 
 * Detects read-write and write-write conflicts using optimistic concurrency control
 */

import type { MvccTransaction } from "../types.ts";
import { ConflictError } from "../types.ts";
import { TxManager } from "./tx-manager.ts";
import { VersionChainManager } from "./version-chain.ts";

export class ConflictDetector {
  constructor(
    private txManager: TxManager,
    private versionChain: VersionChainManager,
  ) {}

  /**
   * Check for conflicts before committing a transaction
   * 
   * Conflicts occur when:
   * 1. Read-Write: Transaction read a key that was modified by a concurrent committed transaction
   * 2. Write-Write: Transaction wrote a key that was also written by a concurrent committed transaction
   * 
   * Returns array of conflicting keys if conflicts found, empty array otherwise
   */
  checkConflicts(txid: bigint): string[] {
    const tx = this.txManager.getTx(txid);
    if (!tx || tx.status !== 'active') {
      return [];
    }

    const conflicts: string[] = [];
    const txSnapshotTs = tx.startTs;

    // Check read-write conflicts
    for (const readKey of tx.readSet) {
      // Check if any concurrent transaction modified this key
      const concurrentModifiers = this.findConcurrentModifiers(
        readKey,
        txSnapshotTs,
        txid,
      );

      if (concurrentModifiers.length > 0) {
        conflicts.push(readKey);
      }
    }

    // Check write-write conflicts
    for (const writeKey of tx.writeSet) {
      // Check if any concurrent transaction wrote this key
      const concurrentWriters = this.findConcurrentModifiers(
        writeKey,
        txSnapshotTs,
        txid,
      );

      if (concurrentWriters.length > 0) {
        conflicts.push(writeKey);
      }
    }

    return conflicts;
  }

  /**
   * Find transactions that modified a key concurrently
   * (committed between this transaction's start and now)
   */
  private findConcurrentModifiers(
    key: string,
    txSnapshotTs: bigint,
    currentTxid: bigint,
  ): bigint[] {
    const modifiers: bigint[] = [];

    // Check all committed transactions
    const allTxs = this.txManager.getAllTxs();
    for (const [txid, tx] of allTxs) {
      // Skip self
      if (txid === currentTxid) {
        continue;
      }

      // Only check committed transactions
      if (tx.status !== 'committed' || !tx.commitTs) {
        continue;
      }

      // Check if transaction committed after our snapshot started
      if (tx.commitTs > txSnapshotTs) {
        // Check if this transaction wrote the key
        if (tx.writeSet.has(key)) {
          modifiers.push(txid);
        }
      }
    }

    return modifiers;
  }

  /**
   * Validate transaction can commit (throws ConflictError if conflicts found)
   */
  validateCommit(txid: bigint): void {
    const conflicts = this.checkConflicts(txid);
    if (conflicts.length > 0) {
      throw new ConflictError(
        `Transaction ${txid} conflicts with concurrent transactions on keys: ${conflicts.join(', ')}`,
        txid,
        conflicts,
      );
    }
  }
}

