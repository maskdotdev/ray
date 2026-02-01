/**
 * KiteDB - A fast, lightweight, embedded graph database for Node.js
 *
 * @example
 * ```typescript
 * import { kite, defineNode, defineEdge, prop, optional } from 'kitedb-core'
 *
 * // Define schema
 * const User = defineNode('user', {
 *   key: (id: string) => `user:${id}`,
 *   props: {
 *     name: prop.string('name'),
 *     email: prop.string('email'),
 *   },
 * })
 *
 * const knows = defineEdge('knows', {
 *   since: prop.int('since'),
 * })
 *
 * // Open database
 * const db = await kite('./my.kitedb', {
 *   nodes: [User],
 *   edges: [knows],
 * })
 *
 * // Insert nodes
 * const alice = db.insert('user').values('alice', { name: 'Alice' }).returning()
 *
 * // Close when done
 * db.close()
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Schema Builders (clean API)
// =============================================================================

export { node, edge, prop, optional, withDefault, defineNode, defineEdge } from './schema'
export type { PropType, PropSpec, KeySpec, NodeSpec, NodeConfig, EdgeSpec } from './schema'

// =============================================================================
// Native Bindings
// =============================================================================

// Import native bindings
import { kite as nativeKite, kiteSync as nativeKiteSync, Kite as NativeKite } from '../index'

import type { JsKiteOptions, JsNodeSpec, JsEdgeSpec, JsPropSpec, JsPropValue } from '../index'

import type { NodeSpec, EdgeSpec, PropSpec } from './schema'

// =============================================================================
// Clean Type Aliases (no Js prefix)
// =============================================================================

// =============================================================================
// Kite Wrapper (transactions + batch)
// =============================================================================

export class Kite extends NativeKite {
  static open(path: string, options: JsKiteOptions): Kite {
    const native = NativeKite.open(path, options)
    Object.setPrototypeOf(native, Kite.prototype)
    return native as Kite
  }

  transaction<T>(fn: (ctx: Kite) => T | Promise<T>): T | Promise<T> {
    if (this.hasTransaction()) {
      return fn(this)
    }

    this.begin()
    try {
      const result = fn(this)
      if (result && typeof (result as Promise<T>).then === 'function') {
        return (result as Promise<T>).then(
          (value) => {
            this.commit()
            return value
          },
          (err) => {
            this.rollback()
            throw err
          },
        )
      }
      this.commit()
      return result
    } catch (err) {
      this.rollback()
      throw err
    }
  }

  batch(operations: Array<any>): any[] {
    if (operations.length === 0) {
      return []
    }

    const nativeOps = new Set(['createNode', 'deleteNode', 'link', 'unlink', 'setProp', 'delProp'])
    const isNativeBatch = operations.every((op) => {
      if (!op || typeof op !== 'object') {
        return false
      }
      const opName = (op as { op?: string }).op ?? (op as { type?: string }).type
      return typeof opName === 'string' && nativeOps.has(opName)
    })

    if (isNativeBatch) {
      return super.batch(operations as Array<object>) as unknown as any[]
    }

    const inTransaction = this.hasTransaction()
    if (!inTransaction) {
      this.begin()
    }

    try {
      const results: any[] = []
      for (const op of operations) {
        let value
        if (typeof op === 'function') {
          value = op(this)
        } else if (op && typeof op.returning === 'function') {
          value = op.returning()
        } else if (op && typeof op.execute === 'function') {
          value = op.execute()
        } else {
          throw new Error('Unsupported batch operation')
        }

        if (value && typeof (value as Promise<unknown>).then === 'function') {
          if (!inTransaction) {
            this.rollback()
          }
          throw new Error('Batch operations must be synchronous')
        }

        results.push(value)
      }

      if (!inTransaction) {
        this.commit()
      }

      return results
    } catch (err) {
      if (!inTransaction) {
        this.rollback()
      }
      throw err
    }
  }
}

// Re-export other classes with clean names
export {
  Database,
  VectorIndex,
  KiteInsertBuilder,
  KiteInsertExecutorSingle,
  KiteInsertExecutorMany,
  KiteUpdateBuilder,
  KiteUpdateEdgeBuilder,
  KiteTraversal,
  KitePath,
  KiteUpsertBuilder,
  KiteUpsertExecutorSingle,
  KiteUpsertExecutorMany,
  KiteUpsertByIdBuilder,
  KiteUpsertEdgeBuilder,
} from '../index'

// Re-export enums with clean names
export {
  JsTraversalDirection as TraversalDirection,
  JsDistanceMetric as DistanceMetric,
  JsAggregation as Aggregation,
  JsSyncMode as SyncMode,
  JsCompressionType as CompressionType,
  PropType as PropValueType,
} from '../index'

// Re-export utility functions
export {
  openDatabase,
  createBackup,
  restoreBackup,
  getBackupInfo,
  createOfflineBackup,
  collectMetrics,
  healthCheck,
  createVectorIndex,
  bruteForceSearch,
  pathConfig,
  traversalStep,
  version,
} from '../index'

// Re-export common types with clean names
export type {
  // Database
  DbStats,
  CheckResult,
  OpenOptions,
  // Export/Import
  ExportOptions,
  ExportResult,
  ImportOptions,
  ImportResult,
  // Backup
  BackupOptions,
  BackupResult,
  RestoreOptions,
  OfflineBackupOptions,
  // Streaming
  StreamOptions,
  PaginationOptions,
  NodePage,
  EdgePage,
  NodeWithProps,
  EdgeWithProps,
  // Metrics
  DatabaseMetrics,
  DataMetrics,
  CacheMetrics,
  CacheLayerMetrics,
  MemoryMetrics,
  MvccMetrics,
  MvccStats,
  HealthCheckResult,
  HealthCheckEntry,
  // Traversal
  JsTraverseOptions as TraverseOptions,
  JsTraversalStep as TraversalStep,
  JsTraversalResult as TraversalResult,
  // Pathfinding
  JsPathConfig as PathConfig,
  JsPathResult as PathResult,
  JsPathEdge as PathEdge,
  // Vectors
  VectorIndexOptions,
  VectorIndexStats,
  VectorSearchHit,
  SimilarOptions,
  JsIvfConfig as IvfConfig,
  JsIvfStats as IvfStats,
  JsPqConfig as PqConfig,
  JsSearchOptions as SearchOptions,
  JsSearchResult as SearchResult,
  JsBruteForceResult as BruteForceResult,
  // Compression
  CompressionOptions,
  SingleFileOptimizeOptions,
  VacuumOptions,
  // Cache
  JsCacheStats as CacheStats,
  // Low-level (for advanced use)
  JsEdge as Edge,
  JsFullEdge as FullEdge,
  JsNodeProp as NodeProp,
  JsPropValue as PropValue,
  JsEdgeInput as EdgeInput,
} from '../index'

// =============================================================================
// Kite Options (clean API)
// =============================================================================

/** Options for opening a Kite database */
export interface KiteOptions {
  /** Node type definitions */
  nodes: NodeSpec[]
  /** Edge type definitions */
  edges: EdgeSpec[]
  /** Open in read-only mode (default: false) */
  readOnly?: boolean
  /** Create database if it doesn't exist (default: true) */
  createIfMissing?: boolean
  /** Acquire file lock (default: true) */
  lockFile?: boolean
}

// =============================================================================
// Type Conversion Helpers
// =============================================================================

function propSpecToNative(spec: PropSpec): JsPropSpec {
  return {
    type: spec.type,
    optional: spec.optional,
    default: spec.default as JsPropValue | undefined,
  }
}

function nodeSpecToNative(spec: NodeSpec): JsNodeSpec {
  let props: Record<string, JsPropSpec> | undefined

  if (spec.props) {
    props = {}
    for (const [k, v] of Object.entries(spec.props)) {
      props[k] = propSpecToNative(v)
    }
  }

  return {
    name: spec.name,
    key: spec.key,
    props,
  }
}

function edgeSpecToNative(spec: EdgeSpec): JsEdgeSpec {
  let props: Record<string, JsPropSpec> | undefined

  if (spec.props) {
    props = {}
    for (const [k, v] of Object.entries(spec.props)) {
      props[k] = propSpecToNative(v)
    }
  }

  return {
    name: spec.name,
    props,
  }
}

function optionsToNative(options: KiteOptions): JsKiteOptions {
  return {
    nodes: options.nodes.map(nodeSpecToNative),
    edges: options.edges.map(edgeSpecToNative),
    readOnly: options.readOnly,
    createIfMissing: options.createIfMissing,
    lockFile: options.lockFile,
  }
}

// =============================================================================
// Main Entry Points
// =============================================================================

/**
 * Open a Kite database asynchronously.
 *
 * This is the recommended way to open a database as it doesn't block
 * the Node.js event loop during file I/O.
 *
 * @param path - Path to the database file
 * @param options - Database options including schema
 * @returns Promise resolving to a Kite database instance
 *
 * @example
 * ```typescript
 * const db = await kite('./my.kitedb', {
 *   nodes: [User, Post],
 *   edges: [follows, authored],
 * })
 * ```
 */
export async function kite(path: string, options: KiteOptions): Promise<Kite> {
  const nativeOptions = optionsToNative(options)
  // Cast through unknown because NAPI-RS generates Promise<unknown> for async tasks
  const native = (await nativeKite(path, nativeOptions)) as NativeKite
  Object.setPrototypeOf(native, Kite.prototype)
  return native as Kite
}

/**
 * Open a Kite database synchronously.
 *
 * Use this when you need synchronous initialization (e.g., at module load time).
 * For most cases, prefer the async `kite()` function.
 *
 * @param path - Path to the database file
 * @param options - Database options including schema
 * @returns A Kite database instance
 *
 * @example
 * ```typescript
 * const db = kiteSync('./my.kitedb', {
 *   nodes: [User],
 *   edges: [knows],
 * })
 * ```
 */
export function kiteSync(path: string, options: KiteOptions): Kite {
  const nativeOptions = optionsToNative(options)
  const native = nativeKiteSync(path, nativeOptions)
  Object.setPrototypeOf(native, Kite.prototype)
  return native as Kite
}
