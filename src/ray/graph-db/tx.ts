import { join } from "node:path";
import {
  addEdge as deltaAddEdge,
  createNode as deltaCreateNode,
  defineEtype as deltaDefineEtype,
  defineLabel as deltaDefineLabel,
  definePropkey as deltaDefinePropkey,
  deleteEdge as deltaDeleteEdge,
  deleteEdgeProp as deltaDeleteEdgeProp,
  deleteNode as deltaDeleteNode,
  deleteNodeProp as deltaDeleteNodeProp,
  setEdgeProp as deltaSetEdgeProp,
  setNodeProp as deltaSetNodeProp,
  getNodeDelta,
  isNodeCreated,
} from "../../core/delta.ts";
import {
  getNodeProp as snapshotGetNodeProp,
  getEdgeProp as snapshotGetEdgeProp,
  getPhysNode,
} from "../../core/snapshot-reader.ts";
import {
  appendToWal,
  buildAddEdgePayload,
  buildBeginPayload,
  buildCommitPayload,
  buildCreateNodePayload,
  buildDefineEtypePayload,
  buildDefineLabelPayload,
  buildDefinePropkeyPayload,
  buildDelEdgePropPayload,
  buildDeleteEdgePayload,
  buildDeleteNodePayload,
  buildDelNodePropPayload,
  buildSetEdgePropPayload,
  buildSetNodePropPayload,
  type WalRecord,
} from "../../core/wal.ts";
import type {
  GraphDB,
  NodeID,
  TxHandle,
  TxState,
  PropKeyID,
  PropValue,
  ETypeID,
} from "../../types.ts";
import { WalRecordType } from "../../types.ts";
import { WAL_DIR, walFilename } from "../../constants.ts";
import { getCache } from "./cache-helper.ts";
import { getMvccManager, isMvccEnabled } from "../../mvcc/index.ts";

function createTxState(txid: bigint): TxState {
  return {
    txid,
    pendingCreatedNodes: new Map(),
    pendingDeletedNodes: new Set(),
    pendingOutAdd: new Map(),
    pendingOutDel: new Map(),
    pendingInAdd: new Map(),
    pendingInDel: new Map(),
    pendingNodeProps: new Map(),
    pendingEdgeProps: new Map(),
    pendingNewLabels: new Map(),
    pendingNewEtypes: new Map(),
    pendingNewPropkeys: new Map(),
    pendingKeyUpdates: new Map(),
    pendingKeyDeletes: new Set(),
  };
}

/**
 * Begin a transaction
 */
export function beginTx(db: GraphDB): TxHandle {
  if (db.readOnly) {
    throw new Error("Cannot begin transaction on read-only database");
  }

  // MVCC mode: allow concurrent transactions
  // Non-MVCC mode: single transaction at a time (backward compatibility)
  if (!isMvccEnabled(db) && db._currentTx) {
    throw new Error("Transaction already in progress");
  }

  const txid = db._nextTxId++;
  const tx = createTxState(txid);
  
  // In MVCC mode, don't set _currentTx (allow concurrent transactions)
  // In non-MVCC mode, maintain backward compatibility
  if (!isMvccEnabled(db)) {
    db._currentTx = tx;
  }

  // Register with MVCC manager if enabled
  const mvcc = getMvccManager(db);
  if (mvcc) {
    mvcc.txManager.beginTx();
  }

  return { _db: db, _tx: tx };
}

/**
 * Commit a transaction
 */
export async function commit(handle: TxHandle): Promise<void> {
  const { _db: db, _tx: tx } = handle;

  // MVCC mode: check transaction validity differently
  // Non-MVCC mode: ensure it's the current transaction
  if (!isMvccEnabled(db)) {
    if (db._currentTx !== tx) {
      throw new Error("Transaction is not current");
    }
  }

  const mvcc = getMvccManager(db);
  
  // MVCC: Check for conflicts before committing
  if (mvcc) {
    mvcc.conflictDetector.validateCommit(tx.txid);
    
    // Commit in MVCC manager (assigns commit timestamp)
    // Note: writes are already recorded during transaction operations (createNode, etc.)
    const commitTs = mvcc.txManager.commitTx(tx.txid);
    
    // Lazy version chain creation: only create version chains when there are 
    // other active transactions that might need to see old versions.
    // This optimization reduces allocations significantly in serial workloads.
    // After commitTx, this tx is no longer counted in activeCount, so we check
    // if there are any remaining active transactions (potential readers).
    const hasActiveReaders = mvcc.txManager.getActiveCount() > 0;
    
    if (hasActiveReaders) {
      const vc = mvcc.versionChain;
      
      // Node creations
      if (tx.pendingCreatedNodes.size > 0) {
        for (const [nodeId, nodeDelta] of tx.pendingCreatedNodes) {
          vc.appendNodeVersion(
            nodeId,
            { nodeId, delta: nodeDelta },
            tx.txid,
            commitTs,
          );
        }
      }
      
      // Node deletions
      if (tx.pendingDeletedNodes.size > 0) {
        for (const nodeId of tx.pendingDeletedNodes) {
          vc.deleteNodeVersion(nodeId, tx.txid, commitTs);
        }
      }
      
      // Edge additions
      if (tx.pendingOutAdd.size > 0) {
        for (const [src, patches] of tx.pendingOutAdd) {
          for (const patch of patches) {
            vc.appendEdgeVersion(
              src,
              patch.etype,
              patch.other,
              true,
              tx.txid,
              commitTs,
            );
          }
        }
      }
      
      // Edge deletions
      if (tx.pendingOutDel.size > 0) {
        for (const [src, patches] of tx.pendingOutDel) {
          for (const patch of patches) {
            vc.appendEdgeVersion(
              src,
              patch.etype,
              patch.other,
              false,
              tx.txid,
              commitTs,
            );
          }
        }
      }
      
      // Node property changes
      // For modifications, we need to initialize the version chain with the old value
      // so that active readers can still see it (snapshot isolation)
      if (tx.pendingNodeProps.size > 0) {
        for (const [nodeId, props] of tx.pendingNodeProps) {
          // Skip newly created nodes - they don't have old values
          if (tx.pendingCreatedNodes.has(nodeId)) {
            // New node - just append new values
            for (const [keyId, value] of props) {
              vc.appendNodePropVersion(
                nodeId,
                keyId,
                value,
                tx.txid,
                commitTs,
              );
            }
            continue;
          }
          
          // Existing node modification - may need to initialize version chain with old value
          for (const [keyId, value] of props) {
            // Check if version chain already exists for this property
            const existingVersion = vc.getNodePropVersion(nodeId, keyId);
            
            if (!existingVersion) {
              // No version chain exists - need to initialize it with the old value
              // so active readers can still see it
              // Use commitTs - 1 so it's visible to readers who started before this commit
              const oldValue = getOldNodePropValue(db, nodeId, keyId);
              if (oldValue !== undefined) {
                // Initialize with old value at a timestamp visible to all current readers
                vc.appendNodePropVersion(
                  nodeId,
                  keyId,
                  oldValue,
                  0n, // Use txid 0 for baseline values
                  0n, // commitTs 0 means visible to everyone
                );
              }
            }
            
            // Now append the new value
            vc.appendNodePropVersion(
              nodeId,
              keyId,
              value,
              tx.txid,
              commitTs,
            );
          }
        }
      }
      
      // Edge property changes - same pattern as node properties
      if (tx.pendingEdgeProps.size > 0) {
        for (const [edgeKey, props] of tx.pendingEdgeProps) {
          const [srcStr, etypeStr, dstStr] = edgeKey.split(":");
          const src = Number(srcStr!);
          const etype = Number.parseInt(etypeStr!, 10);
          const dst = Number(dstStr!);
          
          for (const [keyId, value] of props) {
            // Check if version chain already exists
            const existingVersion = vc.getEdgePropVersion(src, etype, dst, keyId);
            
            if (!existingVersion) {
              // No version chain - initialize with old value
              const oldValue = getOldEdgePropValue(db, src, etype, dst, keyId);
              if (oldValue !== undefined) {
                vc.appendEdgePropVersion(
                  src,
                  etype,
                  dst,
                  keyId,
                  oldValue,
                  0n,
                  0n,
                );
              }
            }
            
            vc.appendEdgePropVersion(
              src,
              etype,
              dst,
              keyId,
              value,
              tx.txid,
              commitTs,
            );
          }
        }
      }
    }
  }

  // Build WAL records
  const records: WalRecord[] = [];

  // BEGIN
  records.push({
    type: WalRecordType.BEGIN,
    txid: tx.txid,
    payload: buildBeginPayload(),
  });

  // Definitions first
  for (const [labelId, name] of tx.pendingNewLabels) {
    records.push({
      type: WalRecordType.DEFINE_LABEL,
      txid: tx.txid,
      payload: buildDefineLabelPayload(labelId, name),
    });
  }

  for (const [etypeId, name] of tx.pendingNewEtypes) {
    records.push({
      type: WalRecordType.DEFINE_ETYPE,
      txid: tx.txid,
      payload: buildDefineEtypePayload(etypeId, name),
    });
  }

  for (const [propkeyId, name] of tx.pendingNewPropkeys) {
    records.push({
      type: WalRecordType.DEFINE_PROPKEY,
      txid: tx.txid,
      payload: buildDefinePropkeyPayload(propkeyId, name),
    });
  }

  // Node creations
  for (const [nodeId, nodeDelta] of tx.pendingCreatedNodes) {
    records.push({
      type: WalRecordType.CREATE_NODE,
      txid: tx.txid,
      payload: buildCreateNodePayload(nodeId, nodeDelta.key),
    });

    // Node properties
    const props = tx.pendingNodeProps.get(nodeId);
    if (props) {
      for (const [keyId, value] of props) {
        if (value !== null) {
          records.push({
            type: WalRecordType.SET_NODE_PROP,
            txid: tx.txid,
            payload: buildSetNodePropPayload(nodeId, keyId, value),
          });
        }
      }
    }
  }

  // Node deletions
  for (const nodeId of tx.pendingDeletedNodes) {
    records.push({
      type: WalRecordType.DELETE_NODE,
      txid: tx.txid,
      payload: buildDeleteNodePayload(nodeId),
    });
  }

  // Edge additions
  for (const [src, patches] of tx.pendingOutAdd) {
    for (const patch of patches) {
      records.push({
        type: WalRecordType.ADD_EDGE,
        txid: tx.txid,
        payload: buildAddEdgePayload(src, patch.etype, patch.other),
      });
    }
  }

  // Edge deletions
  for (const [src, patches] of tx.pendingOutDel) {
    for (const patch of patches) {
      records.push({
        type: WalRecordType.DELETE_EDGE,
        txid: tx.txid,
        payload: buildDeleteEdgePayload(src, patch.etype, patch.other),
      });
    }
  }

  // Existing node property changes
  for (const [nodeId, props] of tx.pendingNodeProps) {
    if (!tx.pendingCreatedNodes.has(nodeId)) {
      for (const [keyId, value] of props) {
        if (value !== null) {
          records.push({
            type: WalRecordType.SET_NODE_PROP,
            txid: tx.txid,
            payload: buildSetNodePropPayload(nodeId, keyId, value),
          });
        } else {
          records.push({
            type: WalRecordType.DEL_NODE_PROP,
            txid: tx.txid,
            payload: buildDelNodePropPayload(nodeId, keyId),
          });
        }
      }
    }
  }

  // Edge property changes
  for (const [edgeKey, props] of tx.pendingEdgeProps) {
    const [srcStr, etypeStr, dstStr] = edgeKey.split(":");
    const src = Number(srcStr!);
    const etype = Number.parseInt(etypeStr!, 10);
    const dst = Number(dstStr!);

    for (const [keyId, value] of props) {
      if (value !== null) {
        records.push({
          type: WalRecordType.SET_EDGE_PROP,
          txid: tx.txid,
          payload: buildSetEdgePropPayload(src, etype, dst, keyId, value),
        });
      } else {
        records.push({
          type: WalRecordType.DEL_EDGE_PROP,
          txid: tx.txid,
          payload: buildDelEdgePropPayload(src, etype, dst, keyId),
        });
      }
    }
  }

  // COMMIT
  records.push({
    type: WalRecordType.COMMIT,
    txid: tx.txid,
    payload: buildCommitPayload(),
  });

  // Append to WAL
  const walPath = join(
    db.path,
    WAL_DIR,
    walFilename(db._manifest.activeWalSeg),
  );
  db._walOffset = await appendToWal(walPath, records);

  // Apply to delta
  for (const [labelId, name] of tx.pendingNewLabels) {
    deltaDefineLabel(db._delta, labelId, name);
  }

  for (const [etypeId, name] of tx.pendingNewEtypes) {
    deltaDefineEtype(db._delta, etypeId, name);
  }

  for (const [propkeyId, name] of tx.pendingNewPropkeys) {
    deltaDefinePropkey(db._delta, propkeyId, name);
  }

  for (const [nodeId, nodeDelta] of tx.pendingCreatedNodes) {
    deltaCreateNode(db._delta, nodeId, nodeDelta.key);
  }

  for (const nodeId of tx.pendingDeletedNodes) {
    deltaDeleteNode(db._delta, nodeId);
  }

  for (const [src, patches] of tx.pendingOutAdd) {
    for (const patch of patches) {
      deltaAddEdge(db._delta, src, patch.etype, patch.other);
    }
  }

  for (const [src, patches] of tx.pendingOutDel) {
    for (const patch of patches) {
      deltaDeleteEdge(db._delta, src, patch.etype, patch.other);
    }
  }

  for (const [nodeId, props] of tx.pendingNodeProps) {
    const isNew = tx.pendingCreatedNodes.has(nodeId);
    for (const [keyId, value] of props) {
      if (value !== null) {
        deltaSetNodeProp(db._delta, nodeId, keyId, value, isNew);
      } else {
        deltaDeleteNodeProp(db._delta, nodeId, keyId, isNew);
      }
    }
  }

  for (const [edgeKey, props] of tx.pendingEdgeProps) {
    const [srcStr, etypeStr, dstStr] = edgeKey.split(":");
    const src = Number(srcStr!);
    const etype = Number.parseInt(etypeStr!, 10);
    const dst = Number(dstStr!);

    for (const [keyId, value] of props) {
      if (value !== null) {
        deltaSetEdgeProp(db._delta, src, etype, dst, keyId, value);
      } else {
        deltaDeleteEdgeProp(db._delta, src, etype, dst, keyId);
      }
    }
  }

  // Transaction-aware cache invalidation
  // Invalidate all affected nodes and edges
  const cache = getCache(db);
  if (cache) {
    // Invalidate all nodes that were created, deleted, or modified
    const affectedNodes = new Set<NodeID>();
    for (const nodeId of tx.pendingCreatedNodes.keys()) {
      affectedNodes.add(nodeId);
    }
    for (const nodeId of tx.pendingDeletedNodes) {
      affectedNodes.add(nodeId);
    }
    for (const nodeId of tx.pendingNodeProps.keys()) {
      affectedNodes.add(nodeId);
    }
    for (const nodeId of affectedNodes) {
      cache.invalidateNode(nodeId);
    }

    // Invalidate all edges that were added, deleted, or modified
    const affectedEdges = new Set<string>(); // Format: "src:etype:dst"
    for (const [src, patches] of tx.pendingOutAdd) {
      for (const patch of patches) {
        affectedEdges.add(`${src}:${patch.etype}:${patch.other}`);
      }
    }
    for (const [src, patches] of tx.pendingOutDel) {
      for (const patch of patches) {
        affectedEdges.add(`${src}:${patch.etype}:${patch.other}`);
      }
    }
    for (const edgeKey of tx.pendingEdgeProps.keys()) {
      affectedEdges.add(edgeKey);
    }
    for (const edgeKey of affectedEdges) {
      const [srcStr, etypeStr, dstStr] = edgeKey.split(":");
      const src = Number(srcStr!);
      const etype = Number.parseInt(etypeStr!, 10);
      const dst = Number(dstStr!);
      cache.invalidateEdge(src, etype, dst);
    }
  }

  // Clear current transaction only in non-MVCC mode
  if (!isMvccEnabled(db)) {
    db._currentTx = null;
  }
}

/**
 * Rollback a transaction
 */
export function rollback(handle: TxHandle): void {
  const { _db: db, _tx: tx } = handle;

  // MVCC mode: check transaction validity differently
  // Non-MVCC mode: ensure it's the current transaction
  if (!isMvccEnabled(db)) {
    if (db._currentTx !== tx) {
      throw new Error("Transaction is not current");
    }
  }

  // Abort in MVCC manager if enabled
  const mvcc = getMvccManager(db);
  if (mvcc) {
    mvcc.txManager.abortTx(tx.txid);
  }

  // Clear current transaction only in non-MVCC mode
  if (!isMvccEnabled(db)) {
    db._currentTx = null;
  }
}

/**
 * Get the old value of a node property from delta or snapshot
 * Used to initialize version chains with baseline values
 */
function getOldNodePropValue(
  db: GraphDB,
  nodeId: NodeID,
  keyId: PropKeyID,
): PropValue | null | undefined {
  // Check delta first
  const nodeDelta = getNodeDelta(db._delta, nodeId);
  if (nodeDelta?.props) {
    const deltaValue = nodeDelta.props.get(keyId);
    if (deltaValue !== undefined) {
      return deltaValue; // null means explicitly deleted
    }
  }
  
  // Fall back to snapshot
  if (db._snapshot) {
    const phys = getPhysNode(db._snapshot, nodeId);
    if (phys >= 0) {
      return snapshotGetNodeProp(db._snapshot, phys, keyId);
    }
  }
  
  return undefined; // Property never existed
}

/**
 * Get the old value of an edge property from delta or snapshot
 * Used to initialize version chains with baseline values
 */
function getOldEdgePropValue(
  db: GraphDB,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
  keyId: PropKeyID,
): PropValue | null | undefined {
  // Check delta first
  const edgeKey = `${src}:${etype}:${dst}`;
  const edgePropsMap = db._delta.edgeProps.get(edgeKey);
  if (edgePropsMap) {
    const deltaValue = edgePropsMap.get(keyId);
    if (deltaValue !== undefined) {
      return deltaValue; // null means explicitly deleted
    }
  }
  
  // Snapshot edge property lookup is complex (requires edge index lookup)
  // For simplicity, if the property isn't in delta, we return undefined
  // This means edge properties that only exist in snapshot won't be preserved
  // in version chains. This is an acceptable trade-off since most active
  // edge property modifications will go through delta first.
  return undefined;
}

