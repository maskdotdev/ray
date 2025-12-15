/**
 * MVCC Version Chain Store
 * 
 * Manages version chains for nodes, edges, and properties
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
} from "../types.ts";

export class VersionChainManager {
  private store: VersionChainStore;

  constructor() {
    this.store = {
      nodeVersions: new Map(),
      edgeVersions: new Map(),
      nodePropVersions: new Map(),
      edgePropVersions: new Map(),
    };
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
      data: { nodeId, delta: { labels: new Set(), labelsDeleted: new Set(), props: new Map() } },
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
    const key = `${src}:${etype}:${dst}`;
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
    const key = `${nodeId}:${propKeyId}`;
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
    const key = `${src}:${etype}:${dst}:${propKeyId}`;
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
    const key = `${src}:${etype}:${dst}`;
    return this.store.edgeVersions.get(key) || null;
  }

  /**
   * Get the latest version for a node property (for visibility checking)
   */
  getNodePropVersion(
    nodeId: NodeID,
    propKeyId: PropKeyID,
  ): VersionedRecord<PropValue | null> | null {
    const key = `${nodeId}:${propKeyId}`;
    return this.store.nodePropVersions.get(key) || null;
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
    const key = `${src}:${etype}:${dst}:${propKeyId}`;
    return this.store.edgePropVersions.get(key) || null;
  }

  /**
   * Prune old versions older than the given timestamp
   * Returns number of versions pruned
   */
  pruneOldVersions(horizonTs: bigint): number {
    let pruned = 0;

    // Prune node versions
    for (const [nodeId, version] of this.store.nodeVersions.entries()) {
      const prunedChain = this.pruneChain(version, horizonTs);
      if (prunedChain === null) {
        this.store.nodeVersions.delete(nodeId);
        pruned++;
      } else if (prunedChain !== version) {
        this.store.nodeVersions.set(nodeId, prunedChain);
        pruned++;
      }
    }

    // Prune edge versions
    for (const [key, version] of this.store.edgeVersions.entries()) {
      const prunedChain = this.pruneChain(version, horizonTs);
      if (prunedChain === null) {
        this.store.edgeVersions.delete(key);
        pruned++;
      } else if (prunedChain !== version) {
        this.store.edgeVersions.set(key, prunedChain);
        pruned++;
      }
    }

    // Prune node property versions
    for (const [key, version] of this.store.nodePropVersions.entries()) {
      const prunedChain = this.pruneChain(version, horizonTs);
      if (prunedChain === null) {
        this.store.nodePropVersions.delete(key);
        pruned++;
      } else if (prunedChain !== version) {
        this.store.nodePropVersions.set(key, prunedChain);
        pruned++;
      }
    }

    // Prune edge property versions
    for (const [key, version] of this.store.edgePropVersions.entries()) {
      const prunedChain = this.pruneChain(version, horizonTs);
      if (prunedChain === null) {
        this.store.edgePropVersions.delete(key);
        pruned++;
      } else if (prunedChain !== version) {
        this.store.edgePropVersions.set(key, prunedChain);
        pruned++;
      }
    }

    return pruned;
  }

  /**
   * Prune a version chain, removing versions older than horizonTs
   * that have newer committed successors
   */
  private pruneChain<T>(
    version: VersionedRecord<T>,
    horizonTs: bigint,
  ): VersionedRecord<T> | null {
    // Walk the chain to find the newest version older than horizon
    let current: VersionedRecord<T> | null = version;
    let newestOld: VersionedRecord<T> | null = null;

    while (current) {
      if (current.commitTs < horizonTs) {
        if (!newestOld || current.commitTs > newestOld.commitTs) {
          newestOld = current;
        }
      }
      current = current.prev;
    }

    // If no old version found, keep the entire chain
    if (!newestOld) {
      return version;
    }

    // If newestOld is the head, we can remove everything
    if (newestOld === version) {
      return null;
    }

    // Otherwise, truncate the chain at newestOld
    newestOld.prev = null;
    return version;
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
  }
}

