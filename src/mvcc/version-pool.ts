/**
 * MVCC Version Pool - Struct-of-Arrays Storage
 * 
 * Memory-efficient storage for version chain metadata using columnar typed arrays.
 * Reduces per-version overhead from ~200 bytes (JS object) to ~25 bytes.
 * 
 * Layout per slot:
 * - txid: 8 bytes (BigUint64Array)
 * - commitTs: 8 bytes (BigUint64Array)
 * - prevIdx: 4 bytes (Int32Array, -1 = null)
 * - flags: 1 byte (Uint8Array, bit 0 = deleted, bit 1 = used)
 * - dataIdx: 4 bytes (Uint32Array, index into separate data store)
 * Total: 25 bytes per slot
 */

/** Initial capacity for version pools */
const INITIAL_CAPACITY = 1024;

/** Growth factor when pool is full */
const GROWTH_FACTOR = 2;

/** Sentinel value for null prev pointer */
export const NULL_IDX = -1;

/** Flag bits */
const FLAG_USED = 0b01;
const FLAG_DELETED = 0b10;

/**
 * Compact version record returned from pool lookups
 * This is a view into the pool, not a copy
 */
export interface PooledVersion<T> {
  idx: number;
  txid: bigint;
  commitTs: bigint;
  prevIdx: number;
  deleted: boolean;
  data: T;
}

/**
 * Version Pool using struct-of-arrays layout for memory efficiency
 */
export class VersionPool<T> {
  // Columnar arrays for version metadata
  private txids: BigUint64Array;
  private commitTss: BigUint64Array;
  private prevIdxs: Int32Array;
  private flags: Uint8Array;
  private dataIdxs: Uint32Array;
  
  // Separate array for actual data (can't be typed)
  private dataStore: (T | undefined)[];
  
  // Pool management
  private capacity: number;
  private nextFreeIdx: number;
  private freeList: number[]; // Recycled slots
  
  constructor(initialCapacity: number = INITIAL_CAPACITY) {
    this.capacity = initialCapacity;
    this.nextFreeIdx = 0;
    this.freeList = [];
    
    this.txids = new BigUint64Array(initialCapacity);
    this.commitTss = new BigUint64Array(initialCapacity);
    this.prevIdxs = new Int32Array(initialCapacity);
    this.flags = new Uint8Array(initialCapacity);
    this.dataIdxs = new Uint32Array(initialCapacity);
    this.dataStore = new Array(initialCapacity);
    
    // Initialize prevIdxs to NULL_IDX
    this.prevIdxs.fill(NULL_IDX);
  }
  
  /**
   * Allocate a new version slot
   * Returns the index of the allocated slot
   */
  alloc(txid: bigint, commitTs: bigint, prevIdx: number, deleted: boolean, data: T): number {
    let idx: number;
    
    // Try to reuse a freed slot first
    if (this.freeList.length > 0) {
      idx = this.freeList.pop()!;
    } else {
      // Allocate new slot
      if (this.nextFreeIdx >= this.capacity) {
        this.grow();
      }
      idx = this.nextFreeIdx++;
    }
    
    // Store metadata in columnar arrays
    this.txids[idx] = txid;
    this.commitTss[idx] = commitTs;
    this.prevIdxs[idx] = prevIdx;
    this.flags[idx] = FLAG_USED | (deleted ? FLAG_DELETED : 0);
    this.dataIdxs[idx] = idx; // Data stored at same index
    this.dataStore[idx] = data;
    
    return idx;
  }
  
  /**
   * Free a version slot for reuse
   */
  free(idx: number): void {
    if (idx < 0 || idx >= this.nextFreeIdx) return;
    if (!(this.flags[idx] & FLAG_USED)) return; // Already freed
    
    this.flags[idx] = 0;
    this.dataStore[idx] = undefined;
    this.freeList.push(idx);
  }
  
  /**
   * Get version at index (returns undefined if slot is unused)
   */
  get(idx: number): PooledVersion<T> | undefined {
    if (idx < 0 || idx >= this.nextFreeIdx) return undefined;
    if (!(this.flags[idx] & FLAG_USED)) return undefined;
    
    const dataIdx = this.dataIdxs[idx];
    const data = this.dataStore[dataIdx];
    if (data === undefined) return undefined;
    
    return {
      idx,
      txid: this.txids[idx],
      commitTs: this.commitTss[idx],
      prevIdx: this.prevIdxs[idx],
      deleted: (this.flags[idx] & FLAG_DELETED) !== 0,
      data,
    };
  }
  
  /**
   * Get txid at index (fast path, no object allocation)
   */
  getTxid(idx: number): bigint {
    return this.txids[idx];
  }
  
  /**
   * Get commitTs at index (fast path, no object allocation)
   */
  getCommitTs(idx: number): bigint {
    return this.commitTss[idx];
  }
  
  /**
   * Get prevIdx at index (fast path, no object allocation)
   */
  getPrevIdx(idx: number): number {
    return this.prevIdxs[idx];
  }
  
  /**
   * Check if slot is deleted
   */
  isDeleted(idx: number): boolean {
    return (this.flags[idx] & FLAG_DELETED) !== 0;
  }
  
  /**
   * Check if slot is used
   */
  isUsed(idx: number): boolean {
    return (this.flags[idx] & FLAG_USED) !== 0;
  }
  
  /**
   * Get data at index
   */
  getData(idx: number): T | undefined {
    const dataIdx = this.dataIdxs[idx];
    return this.dataStore[dataIdx];
  }
  
  /**
   * Set prevIdx (for chain truncation)
   */
  setPrevIdx(idx: number, prevIdx: number): void {
    this.prevIdxs[idx] = prevIdx;
  }
  
  /**
   * Grow the pool capacity
   */
  private grow(): void {
    const newCapacity = this.capacity * GROWTH_FACTOR;
    
    // Allocate new arrays
    const newTxids = new BigUint64Array(newCapacity);
    const newCommitTss = new BigUint64Array(newCapacity);
    const newPrevIdxs = new Int32Array(newCapacity);
    const newFlags = new Uint8Array(newCapacity);
    const newDataIdxs = new Uint32Array(newCapacity);
    const newDataStore: (T | undefined)[] = new Array(newCapacity);
    
    // Copy existing data
    newTxids.set(this.txids);
    newCommitTss.set(this.commitTss);
    newPrevIdxs.set(this.prevIdxs);
    newFlags.set(this.flags);
    newDataIdxs.set(this.dataIdxs);
    for (let i = 0; i < this.capacity; i++) {
      newDataStore[i] = this.dataStore[i];
    }
    
    // Initialize new prevIdxs to NULL_IDX
    newPrevIdxs.fill(NULL_IDX, this.capacity);
    
    // Swap arrays
    this.txids = newTxids;
    this.commitTss = newCommitTss;
    this.prevIdxs = newPrevIdxs;
    this.flags = newFlags;
    this.dataIdxs = newDataIdxs;
    this.dataStore = newDataStore;
    this.capacity = newCapacity;
  }
  
  /**
   * Get current capacity
   */
  getCapacity(): number {
    return this.capacity;
  }
  
  /**
   * Get number of allocated slots (including freed ones that haven't been reused)
   */
  getAllocatedCount(): number {
    return this.nextFreeIdx;
  }
  
  /**
   * Get number of active (used) slots
   */
  getActiveCount(): number {
    return this.nextFreeIdx - this.freeList.length;
  }
  
  /**
   * Clear all slots
   */
  clear(): void {
    this.nextFreeIdx = 0;
    this.freeList.length = 0;
    this.flags.fill(0);
    this.prevIdxs.fill(NULL_IDX);
    this.dataStore.fill(undefined);
  }
  
  /**
   * Get memory usage estimate in bytes
   */
  getMemoryUsage(): number {
    // Typed arrays
    const typedArrayBytes = 
      this.txids.byteLength +      // 8 * capacity
      this.commitTss.byteLength +  // 8 * capacity
      this.prevIdxs.byteLength +   // 4 * capacity
      this.flags.byteLength +      // 1 * capacity
      this.dataIdxs.byteLength;    // 4 * capacity
    
    // Note: dataStore overhead not counted as it varies by data type
    return typedArrayBytes;
  }
}

/**
 * SOA-backed version chain store for property versions
 * Uses VersionPool for memory-efficient storage
 */
export class SoaPropertyVersions<T> {
  private pool: VersionPool<T>;
  private heads: Map<bigint, number>; // key -> head index in pool
  
  constructor(initialCapacity: number = INITIAL_CAPACITY) {
    this.pool = new VersionPool<T>(initialCapacity);
    this.heads = new Map();
  }
  
  /**
   * Append a new version to the chain for the given key
   */
  append(key: bigint, data: T, txid: bigint, commitTs: bigint, deleted: boolean = false): void {
    const existingHeadIdx = this.heads.get(key) ?? NULL_IDX;
    const newIdx = this.pool.alloc(txid, commitTs, existingHeadIdx, deleted, data);
    this.heads.set(key, newIdx);
  }
  
  /**
   * Get the head version for a key
   */
  getHead(key: bigint): PooledVersion<T> | undefined {
    const headIdx = this.heads.get(key);
    if (headIdx === undefined) return undefined;
    return this.pool.get(headIdx);
  }
  
  /**
   * Get the head index for a key (for iteration)
   */
  getHeadIdx(key: bigint): number {
    return this.heads.get(key) ?? NULL_IDX;
  }
  
  /**
   * Get version at index
   */
  getAt(idx: number): PooledVersion<T> | undefined {
    return this.pool.get(idx);
  }
  
  /**
   * Fast path accessors (no object allocation)
   */
  getTxid(idx: number): bigint { return this.pool.getTxid(idx); }
  getCommitTs(idx: number): bigint { return this.pool.getCommitTs(idx); }
  getPrevIdx(idx: number): number { return this.pool.getPrevIdx(idx); }
  isDeleted(idx: number): boolean { return this.pool.isDeleted(idx); }
  getData(idx: number): T | undefined { return this.pool.getData(idx); }
  
  /**
   * Check if key has any versions
   */
  has(key: bigint): boolean {
    return this.heads.has(key);
  }
  
  /**
   * Delete a key's version chain
   */
  delete(key: bigint): boolean {
    const headIdx = this.heads.get(key);
    if (headIdx === undefined) return false;
    
    // Free all versions in the chain
    let idx = headIdx;
    while (idx !== NULL_IDX) {
      const prevIdx = this.pool.getPrevIdx(idx);
      this.pool.free(idx);
      idx = prevIdx;
    }
    
    return this.heads.delete(key);
  }
  
  /**
   * Prune versions older than horizonTs
   * Returns number of versions pruned
   */
  pruneOldVersions(horizonTs: bigint): number {
    let pruned = 0;
    const keysToDelete: bigint[] = [];
    
    for (const [key, headIdx] of this.heads) {
      // Find the boundary (newest old version)
      let keepPoint = NULL_IDX;
      let idx = headIdx;
      
      while (idx !== NULL_IDX) {
        const commitTs = this.pool.getCommitTs(idx);
        if (commitTs < horizonTs) {
          if (keepPoint === NULL_IDX) {
            keepPoint = idx;
          } else {
            // This is older than keepPoint, will be pruned
            pruned++;
          }
        }
        idx = this.pool.getPrevIdx(idx);
      }
      
      // If entire chain is old, mark for deletion
      if (keepPoint === headIdx && this.pool.getCommitTs(headIdx) < horizonTs) {
        keysToDelete.push(key);
        pruned++;
        continue;
      }
      
      // Truncate at keepPoint
      if (keepPoint !== NULL_IDX) {
        // Free all versions after keepPoint
        let toFree = this.pool.getPrevIdx(keepPoint);
        while (toFree !== NULL_IDX) {
          const next = this.pool.getPrevIdx(toFree);
          this.pool.free(toFree);
          toFree = next;
        }
        this.pool.setPrevIdx(keepPoint, NULL_IDX);
      }
    }
    
    // Delete old chains
    for (const key of keysToDelete) {
      this.delete(key);
    }
    
    return pruned;
  }
  
  /**
   * Truncate chains exceeding max depth
   * Preserves versions needed by active readers (commitTs <= minActiveTs)
   * 
   * @param maxDepth Maximum chain depth before truncation
   * @param minActiveTs Minimum active transaction timestamp - versions at or before this
   *                    timestamp must be preserved for snapshot isolation
   * @returns Number of chains truncated
   */
  truncateDeepChains(maxDepth: number, minActiveTs?: bigint): number {
    let truncated = 0;
    
    for (const [_, headIdx] of this.heads) {
      let depth = 0;
      let idx = headIdx;
      let truncatePoint = NULL_IDX;
      
      while (idx !== NULL_IDX && depth < maxDepth) {
        truncatePoint = idx;
        depth++;
        idx = this.pool.getPrevIdx(idx);
      }
      
      // If we reached maxDepth and there's more, check if we can truncate
      if (truncatePoint !== NULL_IDX && this.pool.getPrevIdx(truncatePoint) !== NULL_IDX) {
        // Check if any remaining versions are needed by active readers
        if (minActiveTs !== undefined) {
          let toCheck = this.pool.getPrevIdx(truncatePoint);
          let canTruncate = true;
          while (toCheck !== NULL_IDX) {
            const commitTs = this.pool.getCommitTs(toCheck);
            if (commitTs < minActiveTs) {
              // This version might be needed by active readers - can't truncate
              canTruncate = false;
              break;
            }
            toCheck = this.pool.getPrevIdx(toCheck);
          }
          if (!canTruncate) {
            continue; // Skip this chain
          }
        }
        
        // Free remaining versions
        let toFree = this.pool.getPrevIdx(truncatePoint);
        while (toFree !== NULL_IDX) {
          const next = this.pool.getPrevIdx(toFree);
          this.pool.free(toFree);
          toFree = next;
        }
        this.pool.setPrevIdx(truncatePoint, NULL_IDX);
        truncated++;
      }
    }
    
    return truncated;
  }
  
  /**
   * Get number of chains
   */
  get size(): number {
    return this.heads.size;
  }
  
  /**
   * Clear all versions
   */
  clear(): void {
    this.pool.clear();
    this.heads.clear();
  }
  
  /**
   * Get memory usage estimate
   */
  getMemoryUsage(): number {
    return this.pool.getMemoryUsage();
  }
  
  /**
   * Iterate over all keys
   */
  keys(): IterableIterator<bigint> {
    return this.heads.keys();
  }
  
  /**
   * Iterate over all entries (key, headIdx)
   */
  entries(): IterableIterator<[bigint, number]> {
    return this.heads.entries();
  }
}
