/**
 * Embedded Graph Database
 *
 * A high-performance embedded graph database with:
 * - Fast reads via mmap CSR snapshots
 * - Reliable writes via WAL + delta overlay
 * - Stable node IDs
 * - Periodic compaction for maintenance
 */

// ============================================================================
// Core types
// ============================================================================

export type {
	NodeID,
	ETypeID,
	LabelID,
	PropKeyID,
	PropValue,
	Edge,
	GraphDB,
	TxHandle,
	OpenOptions,
	NodeOpts,
	DbStats,
	CheckResult,
} from "./types.ts";

export { PropValueTag } from "./types.ts";

// ============================================================================
// Database lifecycle
// ============================================================================

export {
	openGraphDB,
	closeGraphDB,
} from "./nero/graph-db.ts";

// ============================================================================
// Transactions
// ============================================================================

export {
	beginTx,
	commit,
	rollback,
} from "./nero/graph-db.ts";

// ============================================================================
// Node operations
// ============================================================================

export {
	createNode,
	deleteNode,
	getNodeByKey,
	nodeExists,
} from "./nero/graph-db.ts";

// ============================================================================
// Edge operations
// ============================================================================

export {
	addEdge,
	deleteEdge,
	getNeighborsOut,
	getNeighborsIn,
	edgeExists,
} from "./nero/graph-db.ts";

// ============================================================================
// Property operations
// ============================================================================

export {
	setNodeProp,
	delNodeProp,
	setEdgeProp,
	delEdgeProp,
	getNodeProp,
	getNodeProps,
	getEdgeProp,
	getEdgeProps,
} from "./nero/graph-db.ts";

// ============================================================================
// Schema definitions
// ============================================================================

export {
	defineLabel,
	defineEtype,
	definePropkey,
} from "./nero/graph-db.ts";

// ============================================================================
// Maintenance
// ============================================================================

export {
	stats,
	check,
} from "./nero/graph-db.ts";

export { optimize, type OptimizeOptions } from "./core/compactor.ts";

// ============================================================================
// Utilities for advanced use
// ============================================================================

export { checkSnapshot } from "./check/checker.ts";

// ============================================================================
// Compression
// ============================================================================

export {
	CompressionType,
	type CompressionOptions,
	DEFAULT_COMPRESSION_OPTIONS,
} from "./util/compression.ts";

// ============================================================================
// High-Level API (Drizzle-style)
// ============================================================================

export {
	// Main entry
	nero,
	Nero,
	type NeroOptions,
	type TransactionContext,
	// Schema builders
	defineNode,
	defineEdge,
	prop,
	optional,
	type NodeDef,
	type EdgeDef,
	type PropDef,
	type PropBuilder,
	type OptionalPropDef,
	type PropsSchema,
	type EdgePropsSchema,
	type InferNode,
	type InferNodeInsert,
	type InferEdge,
	type InferEdgeProps,
	type NeroSchema,
	// Query builders
	type InsertBuilder,
	type InsertExecutor,
	type UpdateBuilder,
	type UpdateExecutor,
	type UpdateByRefBuilder,
	type UpdateByRefExecutor,
	type DeleteBuilder,
	type DeleteExecutor,
	type LinkExecutor,
	type UpdateEdgeBuilder,
	type UpdateEdgeExecutor,
	type NodeRef,
	type WhereCondition,
	// Traversal
	type TraversalBuilder,
	type TraverseOptions,
	type TraversalDirection,
	type AsyncTraversalResult,
	type EdgeResult,
} from "./api/index.ts";
