/**
 * MVCC Visibility Rules
 * 
 * Determines which versions are visible to a transaction based on snapshot isolation
 */

import type {
  VersionedRecord,
  MvccTransaction,
} from "../types.ts";

/**
 * Check if a version is visible to a transaction
 * 
 * A version is visible if:
 * 1. It was committed before the transaction's snapshot timestamp (commitTs < startTs)
 * 2. OR it was created by the transaction itself (own writes)
 * 3. AND it's not deleted (unless it's the transaction's own deletion)
 */
export function isVisible<T>(
  version: VersionedRecord<T> | null,
  txSnapshotTs: bigint,
  txid: bigint,
): boolean {
  if (!version) {
    return false;
  }

  // Own writes are always visible (even if uncommitted)
  if (version.txid === txid) {
    return true;
  }

  // Must be committed before snapshot
  if (version.commitTs >= txSnapshotTs) {
    return false;
  }

  // Check if there's a newer visible version that supersedes this one
  // (This is handled by walking the chain in getVisibleVersion)
  return true;
}

/**
 * Get the visible version from a version chain
 * Walks the chain to find the newest version visible to the transaction
 */
export function getVisibleVersion<T>(
  head: VersionedRecord<T> | null,
  txSnapshotTs: bigint,
  txid: bigint,
): VersionedRecord<T> | null {
  if (!head) {
    return null;
  }

  let current: VersionedRecord<T> | null = head;
  let visible: VersionedRecord<T> | null = null;

  // Walk the chain from newest to oldest
  while (current) {
    if (isVisible(current, txSnapshotTs, txid)) {
      // Found a visible version
      visible = current;
      
      // If it's our own write, we can stop (own writes are always newest for us)
      if (current.txid === txid) {
        break;
      }
      
      // If it's deleted and not our own deletion, we need to check older versions
      // to see if the item existed before
      if (current.deleted && current.txid !== txid) {
        current = current.prev;
        continue;
      }
      
      // Found a non-deleted visible version, this is what we want
      break;
    }
    
    // This version is not visible, check older versions
    current = current.prev;
  }

  return visible;
}

/**
 * Check if a node exists (is visible and not deleted)
 */
export function nodeExists(
  version: VersionedRecord<any> | null,
  txSnapshotTs: bigint,
  txid: bigint,
): boolean {
  const visible = getVisibleVersion(version, txSnapshotTs, txid);
  if (!visible) {
    return false;
  }
  
  // If it's our own deletion, it doesn't exist
  if (visible.deleted && visible.txid === txid) {
    return false;
  }
  
  // If it's deleted by another transaction, check if it existed before
  if (visible.deleted) {
    // Walk back to find a non-deleted version
    let current = visible.prev;
    while (current) {
      if (isVisible(current, txSnapshotTs, txid) && !current.deleted) {
        return true;
      }
      current = current.prev;
    }
    return false;
  }
  
  return true;
}

/**
 * Check if an edge exists (is visible and was added, not deleted)
 */
export function edgeExists(
  version: VersionedRecord<any> | null,
  txSnapshotTs: bigint,
  txid: bigint,
): boolean {
  const visible = getVisibleVersion(version, txSnapshotTs, txid);
  if (!visible) {
    return false;
  }
  
  // For edges, check the 'added' flag in the data
  if (visible.data && typeof visible.data === 'object' && 'added' in visible.data) {
    const edgeData = visible.data as { added: boolean };
    
    // If it's our own write, use the current state
    if (visible.txid === txid) {
      return edgeData.added;
    }
    
    // For other transactions, check if it was added
    if (edgeData.added) {
      // Check if there's a newer deletion
      if (visible.deleted) {
        // Walk back to see if it was added before
        let current = visible.prev;
        while (current) {
          if (isVisible(current, txSnapshotTs, txid)) {
            if (current.deleted) {
              return false;
            }
            if (current.data && typeof current.data === 'object' && 'added' in current.data) {
              return (current.data as { added: boolean }).added;
            }
          }
          current = current.prev;
        }
        return false;
      }
      return true;
    }
    
    // Was deleted, check if it existed before
    let current = visible.prev;
    while (current) {
      if (isVisible(current, txSnapshotTs, txid)) {
        if (current.deleted) {
          return false;
        }
        if (current.data && typeof current.data === 'object' && 'added' in current.data) {
          return (current.data as { added: boolean }).added;
        }
      }
      current = current.prev;
    }
    return false;
  }
  
  return false;
}

