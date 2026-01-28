/**
 * MVCC Version Chain Store
 * 
 * Manages version chains for nodes, edges, and properties.
 * Uses SOA (struct-of-arrays) storage for property versions to reduce memory overhead.
 */

import type {
  VersionedRecord,
  VersionChainStore,
  NodeVersionData,
  EdgeVersionData,
  NodeID,
  ETypeID,
  PropKeyID,
  PropValue,
} from "../types.js";
import { SoaPropertyVersions, NULL_IDX, type PooledVersion } from "./version-pool.js";

/**
 * Adapter to convert PooledVersion to VersionedRecord for API compatibility
 */
function pooledToVersioned<T>(
  pooled: PooledVersion<T> | undefined,
  soaStore: SoaPropertyVersions<T>,
): VersionedRecord<T> | null {
  if (!pooled) return null;
  
  // Build the chain lazily by creating VersionedRecord wrappers
  const buildPrev = (prevIdx: number): VersionedRecord<T> | null => {
    if (prevIdx === NULL_IDX) return null;
    const prevPooled = soaStore.getAt(prevIdx);
    if (!prevPooled) return null;
    return {
      data: prevPooled.data,
      txid: prevPooled.txid,
      commitTs: prevPooled.commitTs,
      prev: buildPrev(prevPooled.prevIdx),
      deleted: prevPooled.deleted,
    };
  };
  
  return {
    data: pooled.data,
    txid: pooled.txid,
    commitTs: pooled.commitTs,
    prev: buildPrev(pooled.prevIdx),
    deleted: pooled.deleted,
  };
}

export class VersionChainManager {
  // Legacy store for node and edge versions (complex data types)
  private store: VersionChainStore;
  
  // SOA-backed stores for property versions (most numerous, benefit most from SOA)
  private soaNodeProps: SoaPropertyVersions<PropValue | null>;
  private soaEdgeProps: SoaPropertyVersions<PropValue | null>;
  
  // Flag to enable/disable SOA storage (for benchmarking/compatibility)
  private useSoa: boolean;

  constructor(useSoa: boolean = true) {
    this.useSoa = useSoa;
    this.store = {
      nodeVersions: new Map(),
      edgeVersions: new Map(),
      nodePropVersions: new Map(),
      edgePropVersions: new Map(),
    };
    
    // Initialize SOA stores
    this.soaNodeProps = new SoaPropertyVersions<PropValue | null>();
    this.soaEdgeProps = new SoaPropertyVersions<PropValue | null>();
  }

  /**
   * Compute numeric composite key for edge lookups
   * Uses bit packing: src (20 bits) | etype (20 bits) | dst (20 bits)
   * Supports NodeID/ETypeID up to ~1M values each
   */
  private edgeKey(src: NodeID, etype: ETypeID, dst: NodeID): bigint {
    return (BigInt(src) << 40n) | (BigInt(etype) << 20n) | BigInt(dst);
  }

  /**
   * Compute numeric composite key for node property lookups
   * Uses bit packing: nodeId (40 bits) | propKeyId (24 bits)
   * Supports NodeID up to ~1 trillion, PropKeyID up to ~16M
   */
  nodePropKey(nodeId: NodeID, propKeyId: PropKeyID): bigint {
    return (BigInt(nodeId) << 24n) | BigInt(propKeyId);
  }

  /**
   * Compute numeric composite key for edge property lookups
   * Uses bit packing to fit edge triple + propKeyId into 64 bits:
   * src (20 bits) | etype (12 bits) | dst (20 bits) | propKeyId (12 bits)
   * Supports NodeID up to ~1M, ETypeID up to 4K, PropKeyID up to 4K
   */
  edgePropKey(src: NodeID, etype: ETypeID, dst: NodeID, propKeyId: PropKeyID): bigint {
    return (BigInt(src) << 44n) | (BigInt(etype) << 32n) | (BigInt(dst) << 12n) | BigInt(propKeyId);
  }

  /**
   * Append a new version to a node's version chain
   */
  appendNodeVersion(
    nodeId: NodeID,
    data: NodeVersionData,
    txid: bigint,
    commitTs: bigint,
  ): void {
    const existing = this.store.nodeVersions.get(nodeId);
    const newVersion: VersionedRecord<NodeVersionData> = {
      data,
      txid,
      commitTs,
      prev: existing || null,
      deleted: false,
    };
    this.store.nodeVersions.set(nodeId, newVersion);
  }

  /**
   * Mark a node as deleted
   */
  deleteNodeVersion(
    nodeId: NodeID,
    txid: bigint,
    commitTs: bigint,
  ): void {
    const existing = this.store.nodeVersions.get(nodeId);
    const deletedVersion: VersionedRecord<NodeVersionData> = {
      data: { nodeId, delta: {} },  // Lazy allocation - empty NodeDelta
      txid,
      commitTs,
      prev: existing || null,
      deleted: true,
    };
    this.store.nodeVersions.set(nodeId, deletedVersion);
  }

  /**
   * Append a new version to an edge's version chain
   */
  appendEdgeVersion(
    src: NodeID,
    etype: ETypeID,
    dst: NodeID,
    added: boolean,
    txid: bigint,
    commitTs: bigint,
  ): void {
    const key = this.edgeKey(src, etype, dst);
    const existing = this.store.edgeVersions.get(key);
    const newVersion: VersionedRecord<EdgeVersionData> = {
      data: { src, etype, dst, added },
      txid,
      commitTs,
      prev: existing || null,
      deleted: false,
    };
    this.store.edgeVersions.set(key, newVersion);
  }

  /**
   * Append a new version to a node property's version chain
   */
  appendNodePropVersion(
    nodeId: NodeID,
    propKeyId: PropKeyID,
    value: PropValue | null,
    txid: bigint,
    commitTs: bigint,
  ): void {
    const key = this.nodePropKey(nodeId, propKeyId);
    
    if (this.useSoa) {
      this.soaNodeProps.append(key, value, txid, commitTs);
    } else {
      const existing = this.store.nodePropVersions.get(key);
      const newVersion: VersionedRecord<PropValue | null> = {
        data: value,
        txid,
        commitTs,
        prev: existing || null,
        deleted: false,
      };
      this.store.nodePropVersions.set(key, newVersion);
    }
  }

  /**
   * Append a new version to an edge property's version chain
   */
  appendEdgePropVersion(
    src: NodeID,
    etype: ETypeID,
    dst: NodeID,
    propKeyId: PropKeyID,
    value: PropValue | null,
    txid: bigint,
    commitTs: bigint,
  ): void {
    const key = this.edgePropKey(src, etype, dst, propKeyId);
    
    if (this.useSoa) {
      this.soaEdgeProps.append(key, value, txid, commitTs);
    } else {
      const existing = this.store.edgePropVersions.get(key);
      const newVersion: VersionedRecord<PropValue | null> = {
        data: value,
        txid,
        commitTs,
        prev: existing || null,
        deleted: false,
      };
      this.store.edgePropVersions.set(key, newVersion);
    }
  }

  /**
   * Get the latest version for a node (for visibility checking)
   */
  getNodeVersion(nodeId: NodeID): VersionedRecord<NodeVersionData> | null {
    return this.store.nodeVersions.get(nodeId) || null;
  }

  /**
   * Get the latest version for an edge (for visibility checking)
   */
  getEdgeVersion(
    src: NodeID,
    etype: ETypeID,
    dst: NodeID,
  ): VersionedRecord<EdgeVersionData> | null {
    const key = this.edgeKey(src, etype, dst);
    return this.store.edgeVersions.get(key) || null;
  }

  /**
   * Get the latest version for a node property (for visibility checking)
   */
  getNodePropVersion(
    nodeId: NodeID,
    propKeyId: PropKeyID,
  ): VersionedRecord<PropValue | null> | null {
    const key = this.nodePropKey(nodeId, propKeyId);
    
    if (this.useSoa) {
      const pooled = this.soaNodeProps.getHead(key);
      return pooledToVersioned(pooled, this.soaNodeProps);
    } else {
      return this.store.nodePropVersions.get(key) || null;
    }
  }

  /**
   * Get the latest version for an edge property (for visibility checking)
   */
  getEdgePropVersion(
    src: NodeID,
    etype: ETypeID,
    dst: NodeID,
    propKeyId: PropKeyID,
  ): VersionedRecord<PropValue | null> | null {
    const key = this.edgePropKey(src, etype, dst, propKeyId);
    
    if (this.useSoa) {
      const pooled = this.soaEdgeProps.getHead(key);
      return pooledToVersioned(pooled, this.soaEdgeProps);
    } else {
      return this.store.edgePropVersions.get(key) || null;
    }
  }

  /**
   * Prune old versions older than the given timestamp
   * Returns number of versions pruned
   */
  pruneOldVersions(horizonTs: bigint): number {
    let pruned = 0;

    // Prune node versions
    for (const [nodeId, version] of this.store.nodeVersions.entries()) {
      const prunedCount = this.pruneChain(version, horizonTs);
      if (prunedCount === -1) {
        // Entire chain was pruned
        this.store.nodeVersions.delete(nodeId);
        pruned++;
      } else {
        pruned += prunedCount;
      }
    }

    // Prune edge versions
    for (const [key, version] of this.store.edgeVersions.entries()) {
      const prunedCount = this.pruneChain(version, horizonTs);
      if (prunedCount === -1) {
        this.store.edgeVersions.delete(key);
        pruned++;
      } else {
        pruned += prunedCount;
      }
    }

    // Prune property versions
    if (this.useSoa) {
      pruned += this.soaNodeProps.pruneOldVersions(horizonTs);
      pruned += this.soaEdgeProps.pruneOldVersions(horizonTs);
    } else {
      // Prune node property versions (legacy path)
      for (const [key, version] of this.store.nodePropVersions.entries()) {
        const prunedCount = this.pruneChain(version, horizonTs);
        if (prunedCount === -1) {
          this.store.nodePropVersions.delete(key);
          pruned++;
        } else {
          pruned += prunedCount;
        }
      }

      // Prune edge property versions (legacy path)
      for (const [key, version] of this.store.edgePropVersions.entries()) {
        const prunedCount = this.pruneChain(version, horizonTs);
        if (prunedCount === -1) {
          this.store.edgePropVersions.delete(key);
          pruned++;
        } else {
          pruned += prunedCount;
        }
      }
    }

    return pruned;
  }

  /**
   * Prune a version chain, removing versions older than horizonTs
   * Returns: -1 if entire chain should be deleted, otherwise count of pruned versions
   */
  private pruneChain<T>(
    version: VersionedRecord<T>,
    horizonTs: bigint,
  ): number {
    // Find the first version we need to keep (newest version < horizonTs)
    // and count versions to prune
    let current: VersionedRecord<T> | null = version;
    let keepPoint: VersionedRecord<T> | null = null;
    let prunedCount = 0;

    // Walk to find the boundary
    while (current) {
      if (current.commitTs < horizonTs) {
        // This is an old version
        if (!keepPoint) {
          // Keep the newest old version as the boundary
          keepPoint = current;
        } else {
          // This is older than keepPoint, will be pruned
          prunedCount++;
        }
      }
      current = current.prev;
    }

    // If no old version found, nothing to prune
    if (!keepPoint) {
      return 0;
    }

    // If keepPoint is the head and all versions are old, delete entire chain
    if (keepPoint === version && version.commitTs < horizonTs) {
      return -1;
    }

    // Truncate the chain at keepPoint (remove versions older than keepPoint)
    if (keepPoint.prev !== null) {
      keepPoint.prev = null;
    }
    
    return prunedCount;
  }

  /**
   * Truncate version chains that exceed the max depth limit
   * This bounds the worst-case traversal time to O(maxDepth)
   * 
   * @param maxDepth Maximum chain depth before truncation
   * @param minActiveTs Minimum active transaction timestamp - versions at or before this
   *                    timestamp must be preserved for snapshot isolation
   * @returns The number of chains truncated
   */
  truncateDeepChains(maxDepth: number, minActiveTs?: bigint): number {
    let truncated = 0;

    // Truncate node version chains
    for (const version of this.store.nodeVersions.values()) {
      if (this.truncateChainAtDepth(version, maxDepth, minActiveTs)) {
        truncated++;
      }
    }

    // Truncate edge version chains
    for (const version of this.store.edgeVersions.values()) {
      if (this.truncateChainAtDepth(version, maxDepth, minActiveTs)) {
        truncated++;
      }
    }

    // Truncate property version chains
    if (this.useSoa) {
      truncated += this.soaNodeProps.truncateDeepChains(maxDepth, minActiveTs);
      truncated += this.soaEdgeProps.truncateDeepChains(maxDepth, minActiveTs);
    } else {
      // Truncate node property version chains (legacy path)
      for (const version of this.store.nodePropVersions.values()) {
        if (this.truncateChainAtDepth(version, maxDepth, minActiveTs)) {
          truncated++;
        }
      }

      // Truncate edge property version chains (legacy path)
      for (const version of this.store.edgePropVersions.values()) {
        if (this.truncateChainAtDepth(version, maxDepth, minActiveTs)) {
          truncated++;
        }
      }
    }

    return truncated;
  }

  /**
   * Truncate a single chain at the given depth
   * Preserves versions needed by active readers (commitTs <= minActiveTs)
   * Returns true if the chain was truncated
   */
  private truncateChainAtDepth<T>(
    head: VersionedRecord<T>,
    maxDepth: number,
    minActiveTs?: bigint,
  ): boolean {
    let depth = 0;
    let current: VersionedRecord<T> | null = head;
    let lastSafeToTruncate: VersionedRecord<T> | null = null;
    
    // Walk to maxDepth, tracking the last version that's safe to truncate after
    while (current && depth < maxDepth) {
      // If this version might be needed by active readers, don't truncate after it
      if (minActiveTs === undefined || current.commitTs >= minActiveTs) {
        lastSafeToTruncate = current;
      }
      depth++;
      current = current.prev;
    }
    
    // If we haven't reached maxDepth or there's nothing more, no truncation needed
    if (!current || current.prev === null) {
      return false;
    }
    
    // Check if we can safely truncate at the current position
    // We can only truncate if there are no active readers that need older versions
    if (minActiveTs !== undefined) {
      // Find if there's a version in the remaining chain that active readers need
      let check: VersionedRecord<T> | null = current.prev;
      while (check) {
        if (check.commitTs < minActiveTs) {
          // This version might be needed by active readers - can't truncate
          return false;
        }
        check = check.prev;
      }
    }
    
    // Safe to truncate
    current.prev = null;
    return true;
  }

  /**
   * Check if any edge versions exist (for fast-path optimization)
   * O(1) check to skip version chain lookups when no edges have been versioned
   */
  hasAnyEdgeVersions(): boolean {
    return this.store.edgeVersions.size > 0;
  }

  /**
   * Get the store (for recovery/debugging)
   */
  getStore(): VersionChainStore {
    return this.store;
  }

  /**
   * Clear all versions (for testing/recovery)
   */
  clear(): void {
    this.store.nodeVersions.clear();
    this.store.edgeVersions.clear();
    this.store.nodePropVersions.clear();
    this.store.edgePropVersions.clear();
    this.soaNodeProps.clear();
    this.soaEdgeProps.clear();
  }

  /**
   * Get memory usage estimate for SOA stores
   */
  getSoaMemoryUsage(): { nodePropBytes: number; edgePropBytes: number } {
    return {
      nodePropBytes: this.soaNodeProps.getMemoryUsage(),
      edgePropBytes: this.soaEdgeProps.getMemoryUsage(),
    };
  }

  /**
   * Check if SOA storage is enabled
   */
  isSoaEnabled(): boolean {
    return this.useSoa;
  }
}
