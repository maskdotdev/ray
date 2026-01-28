import type { GraphDB, NodeID, ETypeID } from "../../types.js";
import { getCache } from "./cache-helper.js";

/**
 * Invalidate all caches for a node
 */
export function invalidateNodeCache(db: GraphDB, nodeId: NodeID): void {
  const cache = getCache(db);
  if (cache) {
    cache.invalidateNode(nodeId);
  }
}

/**
 * Invalidate caches for a specific edge
 */
export function invalidateEdgeCache(
  db: GraphDB,
  src: NodeID,
  etype: ETypeID,
  dst: NodeID,
): void {
  const cache = getCache(db);
  if (cache) {
    cache.invalidateEdge(src, etype, dst);
  }
}

/**
 * Clear all caches
 */
export function clearCache(db: GraphDB): void {
  const cache = getCache(db);
  if (cache) {
    cache.clear();
  }
}

/**
 * Get cache statistics
 */
export function getCacheStats(db: GraphDB): import("../../types.js").CacheStats | null {
  const cache = getCache(db);
  if (!cache || !cache.isEnabled()) {
    return null;
  }
  return cache.getStats();
}

