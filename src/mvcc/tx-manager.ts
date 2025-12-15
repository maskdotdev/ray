/**
 * MVCC Transaction Manager
 * 
 * Manages transaction lifecycle, timestamps, and active transaction tracking
 */

import type { MvccTransaction } from "../types.ts";

export class TxManager {
  private activeTxs: Map<bigint, MvccTransaction> = new Map();
  private nextTxId: bigint;
  private nextCommitTs: bigint;

  constructor(initialTxId: bigint = 1n, initialCommitTs: bigint = 1n) {
    this.nextTxId = initialTxId;
    this.nextCommitTs = initialCommitTs;
  }

  /**
   * Get the minimum active timestamp (oldest active transaction snapshot)
   * Used for GC horizon calculation
   */
  get minActiveTs(): bigint {
    if (this.activeTxs.size === 0) {
      return this.nextCommitTs;
    }

    let min = this.nextCommitTs;
    for (const tx of this.activeTxs.values()) {
      if (tx.status === 'active' && tx.startTs < min) {
        min = tx.startTs;
      }
    }
    return min;
  }

  /**
   * Begin a new transaction
   * Returns transaction ID and snapshot timestamp
   */
  beginTx(): { txid: bigint; startTs: bigint } {
    const txid = this.nextTxId++;
    const startTs = this.nextCommitTs; // Snapshot at current commit timestamp

    const tx: MvccTransaction = {
      txid,
      startTs,
      commitTs: null,
      status: 'active',
      readSet: new Set(),
      writeSet: new Set(),
    };

    this.activeTxs.set(txid, tx);
    return { txid, startTs };
  }

  /**
   * Get transaction by ID
   */
  getTx(txid: bigint): MvccTransaction | undefined {
    return this.activeTxs.get(txid);
  }

  /**
   * Check if transaction is active
   */
  isActive(txid: bigint): boolean {
    const tx = this.activeTxs.get(txid);
    return tx !== undefined && tx.status === 'active';
  }

  /**
   * Record a read operation
   */
  recordRead(txid: bigint, key: string): void {
    const tx = this.activeTxs.get(txid);
    if (tx && tx.status === 'active') {
      tx.readSet.add(key);
    }
  }

  /**
   * Record a write operation
   */
  recordWrite(txid: bigint, key: string): void {
    const tx = this.activeTxs.get(txid);
    if (tx && tx.status === 'active') {
      tx.writeSet.add(key);
    }
  }

  /**
   * Commit a transaction
   * Returns commit timestamp
   */
  commitTx(txid: bigint): bigint {
    const tx = this.activeTxs.get(txid);
    if (!tx) {
      throw new Error(`Transaction ${txid} not found`);
    }
    if (tx.status !== 'active') {
      throw new Error(`Transaction ${txid} is not active (status: ${tx.status})`);
    }

    const commitTs = this.nextCommitTs++;
    tx.commitTs = commitTs;
    tx.status = 'committed';

    // Keep transaction in map for GC visibility calculation
    // Will be removed when GC determines it's safe
    return commitTs;
  }

  /**
   * Abort a transaction
   */
  abortTx(txid: bigint): void {
    const tx = this.activeTxs.get(txid);
    if (!tx) {
      return; // Already removed or never existed
    }

    tx.status = 'aborted';
    tx.commitTs = null;
    // Remove immediately on abort
    this.activeTxs.delete(txid);
  }

  /**
   * Remove a committed transaction (called by GC when safe)
   */
  removeTx(txid: bigint): void {
    this.activeTxs.delete(txid);
  }

  /**
   * Get all active transaction IDs
   */
  getActiveTxIds(): bigint[] {
    return Array.from(this.activeTxs.values())
      .filter(tx => tx.status === 'active')
      .map(tx => tx.txid);
  }

  /**
   * Get transaction count
   */
  getActiveCount(): number {
    return Array.from(this.activeTxs.values())
      .filter(tx => tx.status === 'active').length;
  }

  /**
   * Get the next commit timestamp (for snapshot reads outside transactions)
   */
  getNextCommitTs(): bigint {
    return this.nextCommitTs;
  }

  /**
   * Get all transactions (for debugging/recovery)
   */
  getAllTxs(): Map<bigint, MvccTransaction> {
    return new Map(this.activeTxs);
  }

  /**
   * Clear all transactions (for testing/recovery)
   */
  clear(): void {
    this.activeTxs.clear();
  }
}

