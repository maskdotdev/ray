/**
 * Main GraphDB handle with transaction logic
 * 
 * This module re-exports all public APIs from the graph-db submodules
 * to maintain backward compatibility with existing imports.
 */

// Lifecycle
export { openGraphDB, closeGraphDB } from "./lifecycle.js";

// Transactions
export { beginTx, commit, rollback } from "./tx.js";

// Node operations
export {
  createNode,
  deleteNode,
  getNodeByKey,
  nodeExists,
  setNodeProp,
  delNodeProp,
  getNodeProp,
  getNodeProps,
  listNodes,
  countNodes,
} from "./nodes.js";

// Edge operations
export {
  addEdge,
  deleteEdge,
  getNeighborsOut,
  getNeighborsIn,
  edgeExists,
  setEdgeProp,
  delEdgeProp,
  getEdgeProp,
  getEdgeProps,
  listEdges,
  countEdges,
} from "./edges.js";

// Schema definitions
export { defineLabel, defineEtype, definePropkey } from "./definitions.js";

// Cache API
export {
  invalidateNodeCache,
  invalidateEdgeCache,
  clearCache,
  getCacheStats,
} from "./cache-api.js";

// Stats and maintenance
export { stats, check } from "./stats.js";

// Vector operations
export {
  setNodeVector,
  getNodeVector,
  delNodeVector,
  hasNodeVector,
  getVectorStore,
  getVectorStats,
} from "./vectors.js";

