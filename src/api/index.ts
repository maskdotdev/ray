/**
 * Nero High-Level API
 *
 * Drizzle-style, type-safe API for the Nero graph database.
 * Provides schema definition, query building, and graph traversal with
 * full TypeScript type inference.
 *
 * @module
 *
 * @example
 * ```ts
 * import { nero, defineNode, defineEdge, prop } from './src/api';
 *
 * const user = defineNode('user', {
 *   key: (id: string) => `user:${id}`,
 *   props: { name: prop.string('name') },
 * });
 *
 * const knows = defineEdge('knows');
 *
 * const db = await nero('./my-db', {
 *   nodes: [user],
 *   edges: [knows],
 * });
 * ```
 */

/**
 * Main database context
 * @see {@link nero}
 * @see {@link Nero}
 */
export {
	nero,
	Nero,
	type NeroOptions,
	type TransactionContext,
} from "./nero.ts";

/**
 * Schema definition builders
 * Define node and edge types with properties and type inference
 * @see {@link defineNode}
 * @see {@link defineEdge}
 * @see {@link prop}
 */
export {
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
} from "./schema.ts";

/**
 * Query builders for CRUD operations
 * Fluent API for insert, update, delete with where conditions
 * @see {@link InsertBuilder}
 * @see {@link UpdateBuilder}
 * @see {@link DeleteBuilder}
 */
export type {
	InsertBuilder,
	InsertExecutor,
	UpdateBuilder,
	UpdateExecutor,
	UpdateByRefBuilder,
	UpdateByRefExecutor,
	DeleteBuilder,
	DeleteExecutor,
	LinkExecutor,
	UpdateEdgeBuilder,
	UpdateEdgeExecutor,
	NodeRef,
	WhereCondition,
} from "./builders.ts";

/**
 * Graph traversal with filtering and aggregation
 * Chain traversal steps (out, in, both) with lazy async iteration
 * @see {@link TraversalBuilder}
 */
export type {
	TraversalBuilder,
	TraverseOptions,
	TraversalDirection,
	AsyncTraversalResult,
	EdgeResult,
} from "./traversal.ts";
