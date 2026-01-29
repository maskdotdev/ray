//! Python bindings for SingleFileDB
//!
//! Provides Python access to the single-file database format.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use super::traversal::{PyPathEdge, PyPathResult, PyTraversalResult};
use crate::api::pathfinding::{bfs, dijkstra, PathConfig};
use crate::api::traversal::{
  TraversalBuilder as RustTraversalBuilder, TraversalDirection, TraverseOptions,
};
use crate::cache::manager::CacheManagerStats;
use crate::constants::{EXT_RAYDB, MANIFEST_FILENAME, SNAPSHOTS_DIR, WAL_DIR};
use crate::core::single_file::{
  close_single_file, is_single_file_path, open_single_file, SingleFileDB as RustSingleFileDB,
  SingleFileOpenOptions as RustOpenOptions, SyncMode as RustSyncMode,
};
use crate::export as ray_export;
use crate::graph::db::{
  close_graph_db, open_graph_db as open_multi_file, GraphDB as RustGraphDB,
  OpenOptions as GraphOpenOptions, TxState as GraphTxState,
};
use crate::graph::definitions::define_label as graph_define_label;
use crate::graph::edges::{
  add_edge as graph_add_edge, del_edge_prop as graph_del_edge_prop,
  delete_edge as graph_delete_edge, edge_exists_db, get_edge_prop_db, get_edge_props_db,
  set_edge_prop as graph_set_edge_prop,
};
use crate::graph::iterators::{
  count_edges as graph_count_edges, count_nodes as graph_count_nodes,
  list_edges as graph_list_edges, list_in_edges, list_nodes as graph_list_nodes, list_out_edges,
  ListEdgesOptions,
};
use crate::graph::key_index::get_node_key as graph_get_node_key;
use crate::graph::nodes::{
  add_node_label as graph_add_node_label, create_node as graph_create_node,
  del_node_prop as graph_del_node_prop, delete_node as graph_delete_node, get_node_by_key_db,
  get_node_labels_db, get_node_prop_db, get_node_props_db, node_exists_db, node_has_label_db,
  remove_node_label as graph_remove_node_label, set_node_prop as graph_set_node_prop, NodeOpts,
};
use crate::graph::tx::{
  begin_read_tx as graph_begin_read_tx, begin_tx as graph_begin_tx, commit as graph_commit,
  rollback as graph_rollback, TxHandle as GraphTxHandle,
};
use crate::graph::vectors::{
  delete_node_vector as graph_delete_node_vector, get_node_vector_db as graph_get_node_vector_db,
  has_node_vector_db as graph_has_node_vector_db, set_node_vector as graph_set_node_vector,
};
use crate::streaming;
use crate::types::{
  CheckResult as RustCheckResult, DeltaState, ETypeId, Edge, NodeId, PropKeyId, PropValue,
};
use serde_json;

// ============================================================================
// Open Options
// ============================================================================

/// Synchronization mode for WAL writes
///
/// Controls the durability vs performance trade-off for commits.
/// - "full": Fsync on every commit (durable to OS, slowest)
/// - "normal": Fsync only on checkpoint (~1000x faster, safe from app crash)
/// - "off": No fsync (fastest, data may be lost on any crash)
#[pyclass(name = "SyncMode")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PySyncMode {
  mode: RustSyncMode,
}

#[pymethods]
impl PySyncMode {
  /// Full durability: fsync on every commit
  #[staticmethod]
  fn full() -> Self {
    Self {
      mode: RustSyncMode::Full,
    }
  }

  /// Normal: fsync on checkpoint only (~1000x faster)
  /// Safe from application crashes, but not OS crashes.
  #[staticmethod]
  fn normal() -> Self {
    Self {
      mode: RustSyncMode::Normal,
    }
  }

  /// No fsync (fastest, for testing only)
  #[staticmethod]
  fn off() -> Self {
    Self {
      mode: RustSyncMode::Off,
    }
  }

  fn __repr__(&self) -> String {
    match self.mode {
      RustSyncMode::Full => "SyncMode.full()".to_string(),
      RustSyncMode::Normal => "SyncMode.normal()".to_string(),
      RustSyncMode::Off => "SyncMode.off()".to_string(),
    }
  }
}

/// Options for opening a database
#[pyclass(name = "OpenOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyOpenOptions {
  /// Open in read-only mode
  #[pyo3(get, set)]
  pub read_only: Option<bool>,
  /// Create database if it doesn't exist
  #[pyo3(get, set)]
  pub create_if_missing: Option<bool>,
  /// Acquire file lock (multi-file only)
  #[pyo3(get, set)]
  pub lock_file: Option<bool>,
  /// Require locking support (multi-file only)
  #[pyo3(get, set)]
  pub require_locking: Option<bool>,
  /// Enable MVCC (multi-file only)
  #[pyo3(get, set)]
  pub mvcc: Option<bool>,
  /// MVCC GC interval in ms (multi-file only)
  #[pyo3(get, set)]
  pub mvcc_gc_interval_ms: Option<i64>,
  /// MVCC retention in ms (multi-file only)
  #[pyo3(get, set)]
  pub mvcc_retention_ms: Option<i64>,
  /// MVCC max version chain depth (multi-file only)
  #[pyo3(get, set)]
  pub mvcc_max_chain_depth: Option<u32>,
  /// Page size in bytes (default 4096)
  #[pyo3(get, set)]
  pub page_size: Option<u32>,
  /// WAL size in bytes (default 1MB)
  #[pyo3(get, set)]
  pub wal_size: Option<u32>,
  /// Enable auto-checkpoint when WAL usage exceeds threshold
  #[pyo3(get, set)]
  pub auto_checkpoint: Option<bool>,
  /// WAL usage threshold (0.0-1.0) to trigger auto-checkpoint
  #[pyo3(get, set)]
  pub checkpoint_threshold: Option<f64>,
  /// Use background (non-blocking) checkpoint
  #[pyo3(get, set)]
  pub background_checkpoint: Option<bool>,
  /// Cache parsed snapshot in memory (single-file only)
  #[pyo3(get, set)]
  pub cache_snapshot: Option<bool>,
  /// Enable caching
  #[pyo3(get, set)]
  pub cache_enabled: Option<bool>,
  /// Max node properties in cache
  #[pyo3(get, set)]
  pub cache_max_node_props: Option<i64>,
  /// Max edge properties in cache
  #[pyo3(get, set)]
  pub cache_max_edge_props: Option<i64>,
  /// Max traversal cache entries
  #[pyo3(get, set)]
  pub cache_max_traversal_entries: Option<i64>,
  /// Max query cache entries
  #[pyo3(get, set)]
  pub cache_max_query_entries: Option<i64>,
  /// Query cache TTL in milliseconds
  #[pyo3(get, set)]
  pub cache_query_ttl_ms: Option<i64>,
  /// Sync mode: "full", "normal", or "off"
  pub sync_mode: Option<PySyncMode>,
}

#[pymethods]
impl PyOpenOptions {
  #[new]
  #[pyo3(signature = (
    read_only=None,
    create_if_missing=None,
    lock_file=None,
    require_locking=None,
    mvcc=None,
    mvcc_gc_interval_ms=None,
    mvcc_retention_ms=None,
    mvcc_max_chain_depth=None,
    page_size=None,
    wal_size=None,
    auto_checkpoint=None,
    checkpoint_threshold=None,
    background_checkpoint=None,
    cache_snapshot=None,
    cache_enabled=None,
    cache_max_node_props=None,
    cache_max_edge_props=None,
    cache_max_traversal_entries=None,
    cache_max_query_entries=None,
    cache_query_ttl_ms=None,
    sync_mode=None
  ))]
  fn new(
    read_only: Option<bool>,
    create_if_missing: Option<bool>,
    lock_file: Option<bool>,
    require_locking: Option<bool>,
    mvcc: Option<bool>,
    mvcc_gc_interval_ms: Option<i64>,
    mvcc_retention_ms: Option<i64>,
    mvcc_max_chain_depth: Option<u32>,
    page_size: Option<u32>,
    wal_size: Option<u32>,
    auto_checkpoint: Option<bool>,
    checkpoint_threshold: Option<f64>,
    background_checkpoint: Option<bool>,
    cache_snapshot: Option<bool>,
    cache_enabled: Option<bool>,
    cache_max_node_props: Option<i64>,
    cache_max_edge_props: Option<i64>,
    cache_max_traversal_entries: Option<i64>,
    cache_max_query_entries: Option<i64>,
    cache_query_ttl_ms: Option<i64>,
    sync_mode: Option<PySyncMode>,
  ) -> Self {
    Self {
      read_only,
      create_if_missing,
      lock_file,
      require_locking,
      mvcc,
      mvcc_gc_interval_ms,
      mvcc_retention_ms,
      mvcc_max_chain_depth,
      page_size,
      wal_size,
      auto_checkpoint,
      checkpoint_threshold,
      background_checkpoint,
      cache_snapshot,
      cache_enabled,
      cache_max_node_props,
      cache_max_edge_props,
      cache_max_traversal_entries,
      cache_max_query_entries,
      cache_query_ttl_ms,
      sync_mode,
    }
  }
}

impl From<PyOpenOptions> for RustOpenOptions {
  fn from(opts: PyOpenOptions) -> Self {
    use crate::types::{CacheOptions, PropertyCacheConfig, QueryCacheConfig, TraversalCacheConfig};

    let mut rust_opts = RustOpenOptions::new();
    if let Some(v) = opts.read_only {
      rust_opts = rust_opts.read_only(v);
    }
    if let Some(v) = opts.create_if_missing {
      rust_opts = rust_opts.create_if_missing(v);
    }
    if let Some(v) = opts.page_size {
      rust_opts = rust_opts.page_size(v as usize);
    }
    if let Some(v) = opts.wal_size {
      rust_opts = rust_opts.wal_size(v as usize);
    }
    if let Some(v) = opts.auto_checkpoint {
      rust_opts = rust_opts.auto_checkpoint(v);
    }
    if let Some(v) = opts.checkpoint_threshold {
      rust_opts = rust_opts.checkpoint_threshold(v);
    }
    if let Some(v) = opts.background_checkpoint {
      rust_opts = rust_opts.background_checkpoint(v);
    }

    // Cache options
    if opts.cache_enabled == Some(true) {
      let property_cache = Some(PropertyCacheConfig {
        max_node_props: opts.cache_max_node_props.unwrap_or(10000) as usize,
        max_edge_props: opts.cache_max_edge_props.unwrap_or(10000) as usize,
      });

      let traversal_cache = Some(TraversalCacheConfig {
        max_entries: opts.cache_max_traversal_entries.unwrap_or(5000) as usize,
        max_neighbors_per_entry: 100,
      });

      let query_cache = Some(QueryCacheConfig {
        max_entries: opts.cache_max_query_entries.unwrap_or(1000) as usize,
        ttl_ms: opts.cache_query_ttl_ms.map(|v| v as u64),
      });

      rust_opts = rust_opts.cache(Some(CacheOptions {
        enabled: true,
        property_cache,
        traversal_cache,
        query_cache,
      }));
    }

    // Sync mode
    if let Some(sync) = opts.sync_mode {
      rust_opts = rust_opts.sync_mode(sync.mode);
    }

    rust_opts
  }
}

impl PyOpenOptions {
  fn to_graph_options(&self) -> GraphOpenOptions {
    let mut opts = GraphOpenOptions::new();

    if let Some(v) = self.read_only {
      opts.read_only = v;
    }
    if let Some(v) = self.create_if_missing {
      opts.create_if_missing = v;
    }
    if let Some(v) = self.lock_file {
      opts.lock_file = v;
    }
    if let Some(v) = self.mvcc {
      opts.mvcc = v;
    }

    opts
  }
}

// ============================================================================
// Database Statistics
// ============================================================================

/// Database statistics
#[pyclass(name = "DbStats")]
#[derive(Debug, Clone)]
pub struct PyDbStats {
  #[pyo3(get)]
  pub snapshot_gen: i64,
  #[pyo3(get)]
  pub snapshot_nodes: i64,
  #[pyo3(get)]
  pub snapshot_edges: i64,
  #[pyo3(get)]
  pub snapshot_max_node_id: i64,
  #[pyo3(get)]
  pub delta_nodes_created: i64,
  #[pyo3(get)]
  pub delta_nodes_deleted: i64,
  #[pyo3(get)]
  pub delta_edges_added: i64,
  #[pyo3(get)]
  pub delta_edges_deleted: i64,
  #[pyo3(get)]
  pub wal_bytes: i64,
  #[pyo3(get)]
  pub recommend_compact: bool,
}

#[pymethods]
impl PyDbStats {
  fn __repr__(&self) -> String {
    format!(
      "DbStats(nodes={}, edges={}, wal_bytes={}, recommend_compact={})",
      self.snapshot_nodes + self.delta_nodes_created - self.delta_nodes_deleted,
      self.snapshot_edges + self.delta_edges_added - self.delta_edges_deleted,
      self.wal_bytes,
      self.recommend_compact
    )
  }
}

/// Database integrity check result
#[pyclass(name = "CheckResult")]
#[derive(Debug, Clone)]
pub struct PyCheckResult {
  #[pyo3(get)]
  pub valid: bool,
  #[pyo3(get)]
  pub errors: Vec<String>,
  #[pyo3(get)]
  pub warnings: Vec<String>,
}

#[pymethods]
impl PyCheckResult {
  fn __repr__(&self) -> String {
    format!(
      "CheckResult(valid={}, errors={}, warnings={})",
      self.valid,
      self.errors.len(),
      self.warnings.len()
    )
  }
}

impl From<RustCheckResult> for PyCheckResult {
  fn from(result: RustCheckResult) -> Self {
    PyCheckResult {
      valid: result.valid,
      errors: result.errors,
      warnings: result.warnings,
    }
  }
}

/// Cache statistics
#[pyclass(name = "CacheStats")]
#[derive(Debug, Clone)]
pub struct PyCacheStats {
  #[pyo3(get)]
  pub property_cache_hits: i64,
  #[pyo3(get)]
  pub property_cache_misses: i64,
  #[pyo3(get)]
  pub property_cache_size: i64,
  #[pyo3(get)]
  pub traversal_cache_hits: i64,
  #[pyo3(get)]
  pub traversal_cache_misses: i64,
  #[pyo3(get)]
  pub traversal_cache_size: i64,
  #[pyo3(get)]
  pub query_cache_hits: i64,
  #[pyo3(get)]
  pub query_cache_misses: i64,
  #[pyo3(get)]
  pub query_cache_size: i64,
}

#[pymethods]
impl PyCacheStats {
  fn __repr__(&self) -> String {
    format!(
      "CacheStats(property_hits={}, traversal_hits={}, query_hits={})",
      self.property_cache_hits, self.traversal_cache_hits, self.query_cache_hits
    )
  }
}

// ============================================================================
// Export / Import Options
// ============================================================================

/// Options for export
#[pyclass(name = "ExportOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyExportOptions {
  #[pyo3(get, set)]
  pub include_nodes: Option<bool>,
  #[pyo3(get, set)]
  pub include_edges: Option<bool>,
  #[pyo3(get, set)]
  pub include_schema: Option<bool>,
  #[pyo3(get, set)]
  pub pretty: Option<bool>,
}

#[pymethods]
impl PyExportOptions {
  #[new]
  #[pyo3(signature = (include_nodes=None, include_edges=None, include_schema=None, pretty=None))]
  fn new(
    include_nodes: Option<bool>,
    include_edges: Option<bool>,
    include_schema: Option<bool>,
    pretty: Option<bool>,
  ) -> Self {
    Self {
      include_nodes,
      include_edges,
      include_schema,
      pretty,
    }
  }
}

impl PyExportOptions {
  fn to_rust(self) -> ray_export::ExportOptions {
    let mut opts = ray_export::ExportOptions::default();
    if let Some(v) = self.include_nodes {
      opts.include_nodes = v;
    }
    if let Some(v) = self.include_edges {
      opts.include_edges = v;
    }
    if let Some(v) = self.include_schema {
      opts.include_schema = v;
    }
    if let Some(v) = self.pretty {
      opts.pretty = v;
    }
    opts
  }
}

/// Options for import
#[pyclass(name = "ImportOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyImportOptions {
  #[pyo3(get, set)]
  pub skip_existing: Option<bool>,
  #[pyo3(get, set)]
  pub batch_size: Option<i64>,
}

#[pymethods]
impl PyImportOptions {
  #[new]
  #[pyo3(signature = (skip_existing=None, batch_size=None))]
  fn new(skip_existing: Option<bool>, batch_size: Option<i64>) -> Self {
    Self {
      skip_existing,
      batch_size,
    }
  }
}

impl PyImportOptions {
  fn to_rust(self) -> ray_export::ImportOptions {
    let mut opts = ray_export::ImportOptions::default();
    if let Some(v) = self.skip_existing {
      opts.skip_existing = v;
    }
    if let Some(v) = self.batch_size {
      if v > 0 {
        opts.batch_size = v as usize;
      }
    }
    opts
  }
}

/// Export result
#[pyclass(name = "ExportResult")]
#[derive(Debug, Clone)]
pub struct PyExportResult {
  #[pyo3(get)]
  pub node_count: i64,
  #[pyo3(get)]
  pub edge_count: i64,
}

/// Import result
#[pyclass(name = "ImportResult")]
#[derive(Debug, Clone)]
pub struct PyImportResult {
  #[pyo3(get)]
  pub node_count: i64,
  #[pyo3(get)]
  pub edge_count: i64,
  #[pyo3(get)]
  pub skipped: i64,
}

// =============================================================================
// Streaming / Pagination Options
// =============================================================================

/// Options for streaming node/edge batches
#[pyclass(name = "StreamOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyStreamOptions {
  #[pyo3(get, set)]
  pub batch_size: Option<i64>,
}

#[pymethods]
impl PyStreamOptions {
  #[new]
  #[pyo3(signature = (batch_size=None))]
  fn new(batch_size: Option<i64>) -> Self {
    Self { batch_size }
  }
}

impl PyStreamOptions {
  fn to_rust(self) -> PyResult<streaming::StreamOptions> {
    let batch_size = self.batch_size.unwrap_or(0);
    if batch_size < 0 {
      return Err(PyRuntimeError::new_err("batch_size must be non-negative"));
    }
    Ok(streaming::StreamOptions {
      batch_size: batch_size as usize,
    })
  }
}

/// Options for cursor-based pagination
#[pyclass(name = "PaginationOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyPaginationOptions {
  #[pyo3(get, set)]
  pub limit: Option<i64>,
  #[pyo3(get, set)]
  pub cursor: Option<String>,
}

#[pymethods]
impl PyPaginationOptions {
  #[new]
  #[pyo3(signature = (limit=None, cursor=None))]
  fn new(limit: Option<i64>, cursor: Option<String>) -> Self {
    Self { limit, cursor }
  }
}

impl PyPaginationOptions {
  fn to_rust(self) -> PyResult<streaming::PaginationOptions> {
    let limit = self.limit.unwrap_or(0);
    if limit < 0 {
      return Err(PyRuntimeError::new_err("limit must be non-negative"));
    }
    Ok(streaming::PaginationOptions {
      limit: limit as usize,
      cursor: self.cursor,
    })
  }
}

/// Node entry with properties
#[pyclass(name = "NodeWithProps")]
#[derive(Debug, Clone)]
pub struct PyNodeWithProps {
  #[pyo3(get)]
  pub id: i64,
  #[pyo3(get)]
  pub key: Option<String>,
  #[pyo3(get)]
  pub props: Vec<PyNodeProp>,
}

/// Edge entry with properties
#[pyclass(name = "EdgeWithProps")]
#[derive(Debug, Clone)]
pub struct PyEdgeWithProps {
  #[pyo3(get)]
  pub src: i64,
  #[pyo3(get)]
  pub etype: u32,
  #[pyo3(get)]
  pub dst: i64,
  #[pyo3(get)]
  pub props: Vec<PyNodeProp>,
}

/// Page of node IDs
#[pyclass(name = "NodePage")]
#[derive(Debug, Clone)]
pub struct PyNodePage {
  #[pyo3(get)]
  pub items: Vec<i64>,
  #[pyo3(get)]
  pub next_cursor: Option<String>,
  #[pyo3(get)]
  pub has_more: bool,
  #[pyo3(get)]
  pub total: Option<i64>,
}

/// Page of edges
#[pyclass(name = "EdgePage")]
#[derive(Debug, Clone)]
pub struct PyEdgePage {
  #[pyo3(get)]
  pub items: Vec<PyFullEdge>,
  #[pyo3(get)]
  pub next_cursor: Option<String>,
  #[pyo3(get)]
  pub has_more: bool,
  #[pyo3(get)]
  pub total: Option<i64>,
}

// =============================================================================
// Metrics and Health
// =============================================================================

/// Cache layer metrics
#[pyclass(name = "CacheLayerMetrics")]
#[derive(Debug, Clone)]
pub struct PyCacheLayerMetrics {
  #[pyo3(get)]
  pub hits: i64,
  #[pyo3(get)]
  pub misses: i64,
  #[pyo3(get)]
  pub hit_rate: f64,
  #[pyo3(get)]
  pub size: i64,
  #[pyo3(get)]
  pub max_size: i64,
  #[pyo3(get)]
  pub utilization_percent: f64,
}

/// Cache metrics
#[pyclass(name = "CacheMetrics")]
#[derive(Debug, Clone)]
pub struct PyCacheMetrics {
  #[pyo3(get)]
  pub enabled: bool,
  #[pyo3(get)]
  pub property_cache: PyCacheLayerMetrics,
  #[pyo3(get)]
  pub traversal_cache: PyCacheLayerMetrics,
  #[pyo3(get)]
  pub query_cache: PyCacheLayerMetrics,
}

/// Data metrics
#[pyclass(name = "DataMetrics")]
#[derive(Debug, Clone)]
pub struct PyDataMetrics {
  #[pyo3(get)]
  pub node_count: i64,
  #[pyo3(get)]
  pub edge_count: i64,
  #[pyo3(get)]
  pub delta_nodes_created: i64,
  #[pyo3(get)]
  pub delta_nodes_deleted: i64,
  #[pyo3(get)]
  pub delta_edges_added: i64,
  #[pyo3(get)]
  pub delta_edges_deleted: i64,
  #[pyo3(get)]
  pub snapshot_generation: i64,
  #[pyo3(get)]
  pub max_node_id: i64,
  #[pyo3(get)]
  pub schema_labels: i64,
  #[pyo3(get)]
  pub schema_etypes: i64,
  #[pyo3(get)]
  pub schema_prop_keys: i64,
}

/// MVCC metrics
#[pyclass(name = "MvccMetrics")]
#[derive(Debug, Clone)]
pub struct PyMvccMetrics {
  #[pyo3(get)]
  pub enabled: bool,
  #[pyo3(get)]
  pub active_transactions: i64,
  #[pyo3(get)]
  pub versions_pruned: i64,
  #[pyo3(get)]
  pub gc_runs: i64,
  #[pyo3(get)]
  pub min_active_timestamp: i64,
}

/// Memory metrics
#[pyclass(name = "MemoryMetrics")]
#[derive(Debug, Clone)]
pub struct PyMemoryMetrics {
  #[pyo3(get)]
  pub delta_estimate_bytes: i64,
  #[pyo3(get)]
  pub cache_estimate_bytes: i64,
  #[pyo3(get)]
  pub snapshot_bytes: i64,
  #[pyo3(get)]
  pub total_estimate_bytes: i64,
}

/// Database metrics
#[pyclass(name = "DatabaseMetrics")]
#[derive(Debug, Clone)]
pub struct PyDatabaseMetrics {
  #[pyo3(get)]
  pub path: String,
  #[pyo3(get)]
  pub is_single_file: bool,
  #[pyo3(get)]
  pub read_only: bool,
  #[pyo3(get)]
  pub data: PyDataMetrics,
  #[pyo3(get)]
  pub cache: PyCacheMetrics,
  #[pyo3(get)]
  pub mvcc: Option<PyMvccMetrics>,
  #[pyo3(get)]
  pub memory: PyMemoryMetrics,
  #[pyo3(get)]
  pub collected_at: i64,
}

/// Health check entry
#[pyclass(name = "HealthCheckEntry")]
#[derive(Debug, Clone)]
pub struct PyHealthCheckEntry {
  #[pyo3(get)]
  pub name: String,
  #[pyo3(get)]
  pub passed: bool,
  #[pyo3(get)]
  pub message: String,
}

/// Health check result
#[pyclass(name = "HealthCheckResult")]
#[derive(Debug, Clone)]
pub struct PyHealthCheckResult {
  #[pyo3(get)]
  pub healthy: bool,
  #[pyo3(get)]
  pub checks: Vec<PyHealthCheckEntry>,
}

// =============================================================================
// Backup / Restore
// =============================================================================

/// Options for creating a backup
#[pyclass(name = "BackupOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyBackupOptions {
  #[pyo3(get, set)]
  pub checkpoint: Option<bool>,
  #[pyo3(get, set)]
  pub overwrite: Option<bool>,
}

#[pymethods]
impl PyBackupOptions {
  #[new]
  #[pyo3(signature = (checkpoint=None, overwrite=None))]
  fn new(checkpoint: Option<bool>, overwrite: Option<bool>) -> Self {
    Self {
      checkpoint,
      overwrite,
    }
  }
}

/// Options for restoring a backup
#[pyclass(name = "RestoreOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyRestoreOptions {
  #[pyo3(get, set)]
  pub overwrite: Option<bool>,
}

#[pymethods]
impl PyRestoreOptions {
  #[new]
  #[pyo3(signature = (overwrite=None))]
  fn new(overwrite: Option<bool>) -> Self {
    Self { overwrite }
  }
}

/// Options for offline backup
#[pyclass(name = "OfflineBackupOptions")]
#[derive(Debug, Clone, Default)]
pub struct PyOfflineBackupOptions {
  #[pyo3(get, set)]
  pub overwrite: Option<bool>,
}

#[pymethods]
impl PyOfflineBackupOptions {
  #[new]
  #[pyo3(signature = (overwrite=None))]
  fn new(overwrite: Option<bool>) -> Self {
    Self { overwrite }
  }
}

/// Backup result
#[pyclass(name = "BackupResult")]
#[derive(Debug, Clone)]
pub struct PyBackupResult {
  #[pyo3(get)]
  pub path: String,
  #[pyo3(get)]
  pub size: i64,
  #[pyo3(get)]
  pub timestamp: i64,
  #[pyo3(get)]
  pub r#type: String,
}

// ============================================================================
// Property Value
// ============================================================================

/// Property value types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropType {
  Null,
  Bool,
  Int,
  Float,
  String,
  Vector,
}

/// Property value wrapper for Python
#[pyclass(name = "PropValue")]
#[derive(Debug, Clone)]
pub struct PyPropValue {
  #[pyo3(get)]
  pub prop_type: String,
  #[pyo3(get)]
  pub bool_value: Option<bool>,
  #[pyo3(get)]
  pub int_value: Option<i64>,
  #[pyo3(get)]
  pub float_value: Option<f64>,
  #[pyo3(get)]
  pub string_value: Option<String>,
  #[pyo3(get)]
  pub vector_value: Option<Vec<f64>>,
}

#[pymethods]
impl PyPropValue {
  /// Create a null value
  #[staticmethod]
  fn null() -> Self {
    PyPropValue {
      prop_type: "null".to_string(),
      bool_value: None,
      int_value: None,
      float_value: None,
      string_value: None,
      vector_value: None,
    }
  }

  /// Create a boolean value
  #[staticmethod]
  fn bool(value: bool) -> Self {
    PyPropValue {
      prop_type: "bool".to_string(),
      bool_value: Some(value),
      int_value: None,
      float_value: None,
      string_value: None,
      vector_value: None,
    }
  }

  /// Create an integer value
  #[staticmethod]
  fn int(value: i64) -> Self {
    PyPropValue {
      prop_type: "int".to_string(),
      bool_value: None,
      int_value: Some(value),
      float_value: None,
      string_value: None,
      vector_value: None,
    }
  }

  /// Create a float value
  #[staticmethod]
  #[pyo3(name = "float")]
  fn float_val(value: f64) -> Self {
    PyPropValue {
      prop_type: "float".to_string(),
      bool_value: None,
      int_value: None,
      float_value: Some(value),
      string_value: None,
      vector_value: None,
    }
  }

  /// Create a string value
  #[staticmethod]
  fn string(value: String) -> Self {
    PyPropValue {
      prop_type: "string".to_string(),
      bool_value: None,
      int_value: None,
      float_value: None,
      string_value: Some(value),
      vector_value: None,
    }
  }

  /// Create a vector value
  #[staticmethod]
  fn vector(value: Vec<f64>) -> Self {
    PyPropValue {
      prop_type: "vector".to_string(),
      bool_value: None,
      int_value: None,
      float_value: None,
      string_value: None,
      vector_value: Some(value),
    }
  }

  /// Get the Python value
  fn value(&self, py: Python<'_>) -> PyObject {
    use pyo3::ToPyObject;
    match self.prop_type.as_str() {
      "null" => py.None(),
      "bool" => self.bool_value.unwrap_or(false).to_object(py),
      "int" => self.int_value.unwrap_or(0).to_object(py),
      "float" => self.float_value.unwrap_or(0.0).to_object(py),
      "string" => self.string_value.clone().unwrap_or_default().to_object(py),
      "vector" => self.vector_value.clone().unwrap_or_default().to_object(py),
      _ => py.None(),
    }
  }

  fn __repr__(&self) -> String {
    match self.prop_type.as_str() {
      "null" => "PropValue(null)".to_string(),
      "bool" => format!("PropValue({})", self.bool_value.unwrap_or(false)),
      "int" => format!("PropValue({})", self.int_value.unwrap_or(0)),
      "float" => format!("PropValue({})", self.float_value.unwrap_or(0.0)),
      "string" => format!(
        "PropValue(\"{}\")",
        self.string_value.clone().unwrap_or_default()
      ),
      "vector" => format!(
        "PropValue(vector, len={})",
        self.vector_value.as_ref().map(|v| v.len()).unwrap_or(0)
      ),
      _ => "PropValue(unknown)".to_string(),
    }
  }
}

impl From<PropValue> for PyPropValue {
  fn from(value: PropValue) -> Self {
    match value {
      PropValue::Null => PyPropValue::null(),
      PropValue::Bool(v) => PyPropValue::bool(v),
      PropValue::I64(v) => PyPropValue::int(v),
      PropValue::F64(v) => PyPropValue::float_val(v),
      PropValue::String(v) => PyPropValue::string(v),
      PropValue::VectorF32(v) => PyPropValue::vector(v.iter().map(|&x| x as f64).collect()),
    }
  }
}

impl From<PyPropValue> for PropValue {
  fn from(value: PyPropValue) -> Self {
    match value.prop_type.as_str() {
      "null" => PropValue::Null,
      "bool" => PropValue::Bool(value.bool_value.unwrap_or(false)),
      "int" => PropValue::I64(value.int_value.unwrap_or(0)),
      "float" => PropValue::F64(value.float_value.unwrap_or(0.0)),
      "string" => PropValue::String(value.string_value.unwrap_or_default()),
      "vector" => {
        let vector = value.vector_value.unwrap_or_default();
        PropValue::VectorF32(vector.iter().map(|&x| x as f32).collect())
      }
      _ => PropValue::Null,
    }
  }
}

// ============================================================================
// Edge Result
// ============================================================================

/// Edge representation (neighbor style)
#[pyclass(name = "Edge")]
#[derive(Debug, Clone)]
pub struct PyEdge {
  #[pyo3(get)]
  pub etype: u32,
  #[pyo3(get)]
  pub node_id: i64,
}

#[pymethods]
impl PyEdge {
  fn __repr__(&self) -> String {
    format!("Edge(etype={}, node_id={})", self.etype, self.node_id)
  }
}

/// Full edge representation (src, etype, dst)
#[pyclass(name = "FullEdge")]
#[derive(Debug, Clone)]
pub struct PyFullEdge {
  #[pyo3(get)]
  pub src: i64,
  #[pyo3(get)]
  pub etype: u32,
  #[pyo3(get)]
  pub dst: i64,
}

#[pymethods]
impl PyFullEdge {
  fn __repr__(&self) -> String {
    format!(
      "FullEdge(src={}, etype={}, dst={})",
      self.src, self.etype, self.dst
    )
  }
}

// ============================================================================
// Node Property Result
// ============================================================================

/// Node property key-value pair
#[pyclass(name = "NodeProp")]
#[derive(Debug, Clone)]
pub struct PyNodeProp {
  #[pyo3(get)]
  pub key_id: u32,
  #[pyo3(get)]
  pub value: PyPropValue,
}

#[pymethods]
impl PyNodeProp {
  fn __repr__(&self) -> String {
    format!("NodeProp(key_id={}, value={:?})", self.key_id, self.value)
  }
}

// ============================================================================
// SingleFileDB Python Wrapper
// ============================================================================

enum DatabaseInner {
  SingleFile(RustSingleFileDB),
  Graph(RustGraphDB),
}

/// Graph database handle (single-file or multi-file)
#[pyclass(name = "Database")]
pub struct PyDatabase {
  inner: Mutex<Option<DatabaseInner>>,
  graph_tx: Mutex<Option<GraphTxState>>,
}

#[pymethods]
impl PyDatabase {
  /// Open a database file
  #[new]
  #[pyo3(signature = (path, options=None))]
  fn new(path: String, options: Option<PyOpenOptions>) -> PyResult<Self> {
    let options = options.unwrap_or_default();
    let path_buf = PathBuf::from(&path);

    if path_buf.exists() {
      if path_buf.is_dir() {
        let graph_opts = options.to_graph_options();
        let db = open_multi_file(&path_buf, graph_opts)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to open database: {e}")))?;
        return Ok(PyDatabase {
          inner: Mutex::new(Some(DatabaseInner::Graph(db))),
          graph_tx: Mutex::new(None),
        });
      }
    }

    let mut db_path = path_buf;
    if !is_single_file_path(&db_path) {
      db_path = PathBuf::from(format!("{path}.raydb"));
    }

    let opts: RustOpenOptions = options.into();
    let db = open_single_file(&db_path, opts)
      .map_err(|e| PyRuntimeError::new_err(format!("Failed to open database: {e}")))?;
    Ok(PyDatabase {
      inner: Mutex::new(Some(DatabaseInner::SingleFile(db))),
      graph_tx: Mutex::new(None),
    })
  }

  /// Close the database
  fn close(&self) -> PyResult<()> {
    let mut guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    if let Some(db) = guard.take() {
      match db {
        DatabaseInner::SingleFile(db) => {
          close_single_file(db)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to close database: {e}")))?;
        }
        DatabaseInner::Graph(db) => {
          close_graph_db(db)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to close database: {e}")))?;
        }
      }
    }
    let _ = self.graph_tx.lock().map(|mut tx| tx.take());
    Ok(())
  }

  /// Check if database is open
  #[getter]
  fn is_open(&self) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(guard.is_some())
  }

  /// Get database path
  #[getter]
  fn path(&self) -> PyResult<String> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.path.to_string_lossy().to_string()),
      Some(DatabaseInner::Graph(db)) => Ok(db.path.to_string_lossy().to_string()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if database is read-only
  #[getter]
  fn read_only(&self) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.read_only),
      Some(DatabaseInner::Graph(db)) => Ok(db.read_only),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Transaction Methods
  // ========================================================================

  /// Begin a transaction
  #[pyo3(signature = (read_only=None))]
  fn begin(&self, read_only: Option<bool>) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let read_only = read_only.unwrap_or(false);
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let txid = db
          .begin(read_only)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to begin transaction: {e}")))?;
        Ok(txid as i64)
      }
      Some(DatabaseInner::Graph(db)) => {
        let mut tx_guard = self
          .graph_tx
          .lock()
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        if tx_guard.is_some() {
          return Err(PyRuntimeError::new_err("Transaction already active"));
        }

        let handle = if read_only {
          graph_begin_read_tx(db)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to begin transaction: {e}")))?
        } else {
          graph_begin_tx(db)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to begin transaction: {e}")))?
        };
        let txid = handle.tx.txid as i64;
        *tx_guard = Some(handle.tx);
        Ok(txid)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Commit the current transaction
  fn commit(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .commit()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to commit: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let mut tx_guard = self
          .graph_tx
          .lock()
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let tx_state = tx_guard
          .take()
          .ok_or_else(|| PyRuntimeError::new_err("No active transaction"))?;
        let mut handle = GraphTxHandle::new(db, tx_state);
        graph_commit(&mut handle)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to commit: {e}")))?;
        Ok(())
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Rollback the current transaction
  fn rollback(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .rollback()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to rollback: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let mut tx_guard = self
          .graph_tx
          .lock()
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let tx_state = tx_guard
          .take()
          .ok_or_else(|| PyRuntimeError::new_err("No active transaction"))?;
        let mut handle = GraphTxHandle::new(db, tx_state);
        graph_rollback(&mut handle)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to rollback: {e}")))?;
        Ok(())
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if there's an active transaction
  fn has_transaction(&self) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.has_transaction()),
      Some(DatabaseInner::Graph(_)) => Ok(
        self
          .graph_tx
          .lock()
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
          .is_some(),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Node Operations
  // ========================================================================

  /// Create a new node
  #[pyo3(signature = (key=None))]
  fn create_node(&self, key: Option<String>) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let node_id = db
          .create_node(key.as_deref())
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to create node: {e}")))?;
        Ok(node_id as i64)
      }
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        let mut opts = NodeOpts::new();
        if let Some(key) = key {
          opts = opts.with_key(key);
        }
        let node_id = graph_create_node(handle, opts)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to create node: {e}")))?;
        Ok(node_id as i64)
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Batch create multiple nodes with properties in a single transaction
  ///
  /// This is optimized for bulk inserts - creates all nodes and sets all properties
  /// in a single transaction with minimal FFI overhead.
  ///
  /// Args:
  ///   nodes: List of (key, props) tuples where props is a list of (prop_key_id, PropValue)
  ///
  /// Returns:
  ///   List of created node IDs
  fn batch_create_nodes(
    &self,
    nodes: Vec<(String, Vec<(u32, PyPropValue)>)>,
  ) -> PyResult<Vec<i64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let mut node_ids = Vec::with_capacity(nodes.len());

        db.begin(false)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to begin transaction: {e}")))?;

        let result: Result<(), pyo3::PyErr> = (|| {
          for (key, props) in nodes {
            let node_id = db
              .create_node(Some(&key))
              .map_err(|e| PyRuntimeError::new_err(format!("Failed to create node: {e}")))?;

            for (prop_key_id, value) in props {
              db.set_node_prop(node_id, prop_key_id as PropKeyId, value.into())
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
            }

            node_ids.push(node_id as i64);
          }
          Ok(())
        })();

        match result {
          Ok(()) => {
            db.commit()
              .map_err(|e| PyRuntimeError::new_err(format!("Failed to commit: {e}")))?;
            Ok(node_ids)
          }
          Err(e) => {
            let _ = db.rollback();
            Err(e)
          }
        }
      }
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        let mut node_ids = Vec::with_capacity(nodes.len());
        for (key, props) in nodes {
          let opts = NodeOpts::new().with_key(key);
          let node_id = graph_create_node(handle, opts)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create node: {e}")))?;

          for (prop_key_id, value) in props {
            graph_set_node_prop(handle, node_id, prop_key_id as PropKeyId, value.into())
              .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
          }

          node_ids.push(node_id as i64);
        }
        Ok(node_ids)
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Delete a node
  fn delete_node(&self, node_id: i64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_node(node_id as NodeId)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete node: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_delete_node(handle, node_id as NodeId)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete node: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if a node exists
  fn node_exists(&self, node_id: i64) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.node_exists(node_id as NodeId)),
      Some(DatabaseInner::Graph(db)) => Ok(node_exists_db(db, node_id as NodeId)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get node by key
  fn get_node_by_key(&self, key: String) -> PyResult<Option<i64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_node_by_key(&key).map(|id| id as i64)),
      Some(DatabaseInner::Graph(db)) => Ok(get_node_by_key_db(db, &key).map(|id| id as i64)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get the key for a node
  fn get_node_key(&self, node_id: i64) -> PyResult<Option<String>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_node_key(node_id as NodeId)),
      Some(DatabaseInner::Graph(db)) => Ok(get_graph_node_key(db, node_id as NodeId)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// List all node IDs
  fn list_nodes(&self) -> PyResult<Vec<i64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.list_nodes().into_iter().map(|id| id as i64).collect())
      }
      Some(DatabaseInner::Graph(db)) => Ok(
        graph_list_nodes(db)
          .into_iter()
          .map(|id| id as i64)
          .collect(),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// List node IDs with keys matching a prefix
  ///
  /// This is optimized for filtering by node type (e.g., "user:" prefix).
  fn list_nodes_with_prefix(&self, prefix: String) -> PyResult<Vec<i64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let results: Vec<i64> = db
          .list_nodes()
          .into_iter()
          .filter(|&id| {
            if let Some(key) = db.get_node_key(id) {
              key.starts_with(&prefix)
            } else {
              false
            }
          })
          .map(|id| id as i64)
          .collect();
        Ok(results)
      }
      Some(DatabaseInner::Graph(db)) => {
        let results: Vec<i64> = graph_list_nodes(db)
          .into_iter()
          .filter(|&id| {
            if let Some(key) = get_graph_node_key(db, id) {
              key.starts_with(&prefix)
            } else {
              false
            }
          })
          .map(|id| id as i64)
          .collect();
        Ok(results)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Count nodes with keys matching a prefix
  ///
  /// This is optimized for counting by node type (e.g., "user:" prefix).
  fn count_nodes_with_prefix(&self, prefix: String) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let count = db
          .list_nodes()
          .into_iter()
          .filter(|&id| {
            if let Some(key) = db.get_node_key(id) {
              key.starts_with(&prefix)
            } else {
              false
            }
          })
          .count();
        Ok(count as i64)
      }
      Some(DatabaseInner::Graph(db)) => {
        let count = graph_list_nodes(db)
          .into_iter()
          .filter(|&id| {
            if let Some(key) = get_graph_node_key(db, id) {
              key.starts_with(&prefix)
            } else {
              false
            }
          })
          .count();
        Ok(count as i64)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Count all nodes
  fn count_nodes(&self) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.count_nodes() as i64),
      Some(DatabaseInner::Graph(db)) => Ok(graph_count_nodes(db) as i64),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Edge Operations
  // ========================================================================

  /// Add an edge
  fn add_edge(&self, src: i64, etype: u32, dst: i64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_edge(src as NodeId, etype as ETypeId, dst as NodeId)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to add edge: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_add_edge(handle, src as NodeId, etype as ETypeId, dst as NodeId)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to add edge: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Add an edge by type name
  fn add_edge_by_name(&self, src: i64, etype_name: String, dst: i64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_edge_by_name(src as NodeId, &etype_name, dst as NodeId)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to add edge: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown edge type: {etype_name}")))?;
        self.with_graph_tx(db, |handle| {
          graph_add_edge(handle, src as NodeId, etype, dst as NodeId)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to add edge: {e}")))?;
          Ok(())
        })
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Delete an edge
  fn delete_edge(&self, src: i64, etype: u32, dst: i64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_edge(src as NodeId, etype as ETypeId, dst as NodeId)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete edge: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_delete_edge(handle, src as NodeId, etype as ETypeId, dst as NodeId)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete edge: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if an edge exists
  fn edge_exists(&self, src: i64, etype: u32, dst: i64) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.edge_exists(src as NodeId, etype as ETypeId, dst as NodeId))
      }
      Some(DatabaseInner::Graph(db)) => Ok(edge_exists_db(
        db,
        src as NodeId,
        etype as ETypeId,
        dst as NodeId,
      )),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get outgoing edges for a node
  fn get_out_edges(&self, node_id: i64) -> PyResult<Vec<PyEdge>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_out_edges(node_id as NodeId)
          .into_iter()
          .map(|(etype, dst)| PyEdge {
            etype,
            node_id: dst as i64,
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        list_out_edges(db, node_id as NodeId)
          .into_iter()
          .map(|edge| PyEdge {
            etype: edge.etype,
            node_id: edge.dst as i64,
          })
          .collect(),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get incoming edges for a node
  fn get_in_edges(&self, node_id: i64) -> PyResult<Vec<PyEdge>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_in_edges(node_id as NodeId)
          .into_iter()
          .map(|(etype, src)| PyEdge {
            etype,
            node_id: src as i64,
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        list_in_edges(db, node_id as NodeId)
          .into_iter()
          .map(|edge| PyEdge {
            etype: edge.etype,
            node_id: edge.dst as i64,
          })
          .collect(),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get out-degree for a node
  fn get_out_degree(&self, node_id: i64) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_out_degree(node_id as NodeId) as i64),
      Some(DatabaseInner::Graph(db)) => Ok(list_out_edges(db, node_id as NodeId).len() as i64),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get in-degree for a node
  fn get_in_degree(&self, node_id: i64) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_in_degree(node_id as NodeId) as i64),
      Some(DatabaseInner::Graph(db)) => Ok(list_in_edges(db, node_id as NodeId).len() as i64),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Count all edges
  fn count_edges(&self) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.count_edges() as i64),
      Some(DatabaseInner::Graph(db)) => Ok(graph_count_edges(db, None) as i64),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// List all edges in the database
  #[pyo3(signature = (etype=None))]
  fn list_edges(&self, etype: Option<u32>) -> PyResult<Vec<PyFullEdge>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.list_edges(etype)
          .into_iter()
          .map(|e| PyFullEdge {
            src: e.src as i64,
            etype: e.etype,
            dst: e.dst as i64,
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => {
        let options = ListEdgesOptions { etype };
        Ok(
          graph_list_edges(db, options)
            .into_iter()
            .map(|e| PyFullEdge {
              src: e.src as i64,
              etype: e.etype,
              dst: e.dst as i64,
            })
            .collect(),
        )
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// List edges by type name
  fn list_edges_by_name(&self, etype_name: String) -> PyResult<Vec<PyFullEdge>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown edge type: {etype_name}")))?;
        Ok(
          db.list_edges(Some(etype))
            .into_iter()
            .map(|e| PyFullEdge {
              src: e.src as i64,
              etype: e.etype,
              dst: e.dst as i64,
            })
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown edge type: {etype_name}")))?;
        let options = ListEdgesOptions { etype: Some(etype) };
        Ok(
          graph_list_edges(db, options)
            .into_iter()
            .map(|e| PyFullEdge {
              src: e.src as i64,
              etype: e.etype,
              dst: e.dst as i64,
            })
            .collect(),
        )
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Count edges by type
  fn count_edges_by_type(&self, etype: u32) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.count_edges_by_type(etype) as i64),
      Some(DatabaseInner::Graph(db)) => Ok(graph_count_edges(db, Some(etype)) as i64),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Count edges by type name
  fn count_edges_by_name(&self, etype_name: String) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown edge type: {etype_name}")))?;
        Ok(db.count_edges_by_type(etype) as i64)
      }
      Some(DatabaseInner::Graph(db)) => {
        let etype = db
          .get_etype_id(&etype_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown edge type: {etype_name}")))?;
        Ok(graph_count_edges(db, Some(etype)) as i64)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Streaming and Pagination
  // ========================================================================

  /// Stream nodes in batches
  #[pyo3(signature = (options=None))]
  fn stream_nodes(&self, options: Option<PyStreamOptions>) -> PyResult<Vec<Vec<i64>>> {
    let options = options.unwrap_or_default().to_rust()?;
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        streaming::stream_nodes_single(db, options)
          .into_iter()
          .map(|batch| batch.into_iter().map(|id| id as i64).collect())
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        streaming::stream_nodes_graph(db, options)
          .into_iter()
          .map(|batch| batch.into_iter().map(|id| id as i64).collect())
          .collect(),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Stream nodes with properties in batches
  #[pyo3(signature = (options=None))]
  fn stream_nodes_with_props(
    &self,
    options: Option<PyStreamOptions>,
  ) -> PyResult<Vec<Vec<PyNodeWithProps>>> {
    let options = options.unwrap_or_default().to_rust()?;
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let batches = streaming::stream_nodes_single(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|node_id| {
                  let key = db.get_node_key(node_id as NodeId);
                  let props = db.get_node_props(node_id as NodeId).unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| PyNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  PyNodeWithProps {
                    id: node_id as i64,
                    key,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let batches = streaming::stream_nodes_graph(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|node_id| {
                  let key = get_graph_node_key(db, node_id);
                  let props = get_node_props_db(db, node_id).unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| PyNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  PyNodeWithProps {
                    id: node_id as i64,
                    key,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Stream edges in batches
  #[pyo3(signature = (options=None))]
  fn stream_edges(&self, options: Option<PyStreamOptions>) -> PyResult<Vec<Vec<PyFullEdge>>> {
    let options = options.unwrap_or_default().to_rust()?;
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        streaming::stream_edges_single(db, options)
          .into_iter()
          .map(|batch| {
            batch
              .into_iter()
              .map(|edge| PyFullEdge {
                src: edge.src as i64,
                etype: edge.etype,
                dst: edge.dst as i64,
              })
              .collect()
          })
          .collect(),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        streaming::stream_edges_graph(db, options)
          .into_iter()
          .map(|batch| {
            batch
              .into_iter()
              .map(|edge| PyFullEdge {
                src: edge.src as i64,
                etype: edge.etype,
                dst: edge.dst as i64,
              })
              .collect()
          })
          .collect(),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Stream edges with properties in batches
  #[pyo3(signature = (options=None))]
  fn stream_edges_with_props(
    &self,
    options: Option<PyStreamOptions>,
  ) -> PyResult<Vec<Vec<PyEdgeWithProps>>> {
    let options = options.unwrap_or_default().to_rust()?;
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let batches = streaming::stream_edges_single(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|edge| {
                  let props = db
                    .get_edge_props(edge.src, edge.etype, edge.dst)
                    .unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| PyNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  PyEdgeWithProps {
                    src: edge.src as i64,
                    etype: edge.etype,
                    dst: edge.dst as i64,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      Some(DatabaseInner::Graph(db)) => {
        let batches = streaming::stream_edges_graph(db, options);
        Ok(
          batches
            .into_iter()
            .map(|batch| {
              batch
                .into_iter()
                .map(|edge| {
                  let props =
                    get_edge_props_db(db, edge.src, edge.etype, edge.dst).unwrap_or_default();
                  let props = props
                    .into_iter()
                    .map(|(k, v)| PyNodeProp {
                      key_id: k,
                      value: v.into(),
                    })
                    .collect();
                  PyEdgeWithProps {
                    src: edge.src as i64,
                    etype: edge.etype,
                    dst: edge.dst as i64,
                    props,
                  }
                })
                .collect()
            })
            .collect(),
        )
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get a page of node IDs
  #[pyo3(signature = (options=None))]
  fn get_nodes_page(&self, options: Option<PyPaginationOptions>) -> PyResult<PyNodePage> {
    let options = options.unwrap_or_default().to_rust()?;
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let page = streaming::get_nodes_page_single(db, options);
        Ok(PyNodePage {
          items: page.items.into_iter().map(|id| id as i64).collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(db.count_nodes() as i64),
        })
      }
      Some(DatabaseInner::Graph(db)) => {
        let page = streaming::get_nodes_page_graph(db, options);
        Ok(PyNodePage {
          items: page.items.into_iter().map(|id| id as i64).collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(graph_count_nodes(db) as i64),
        })
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get a page of edges
  #[pyo3(signature = (options=None))]
  fn get_edges_page(&self, options: Option<PyPaginationOptions>) -> PyResult<PyEdgePage> {
    let options = options.unwrap_or_default().to_rust()?;
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let page = streaming::get_edges_page_single(db, options);
        Ok(PyEdgePage {
          items: page
            .items
            .into_iter()
            .map(|edge| PyFullEdge {
              src: edge.src as i64,
              etype: edge.etype,
              dst: edge.dst as i64,
            })
            .collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(db.count_edges() as i64),
        })
      }
      Some(DatabaseInner::Graph(db)) => {
        let page = streaming::get_edges_page_graph(db, options);
        Ok(PyEdgePage {
          items: page
            .items
            .into_iter()
            .map(|edge| PyFullEdge {
              src: edge.src as i64,
              etype: edge.etype,
              dst: edge.dst as i64,
            })
            .collect(),
          next_cursor: page.next_cursor,
          has_more: page.has_more,
          total: Some(graph_count_edges(db, None) as i64),
        })
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Property Operations
  // ========================================================================

  /// Set a node property
  fn set_node_prop(&self, node_id: i64, key_id: u32, value: PyPropValue) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop(node_id as NodeId, key_id as PropKeyId, value.into())
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_set_node_prop(handle, node_id as NodeId, key_id as PropKeyId, value.into())
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Set a node property by key name
  fn set_node_prop_by_name(
    &self,
    node_id: i64,
    key_name: String,
    value: PyPropValue,
  ) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop_by_name(node_id as NodeId, &key_name, value.into())
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        let key_id = handle
          .db
          .get_propkey_id(&key_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown property key: {key_name}")))?;
        graph_set_node_prop(handle, node_id as NodeId, key_id, value.into())
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Delete a node property
  fn delete_node_prop(&self, node_id: i64, key_id: u32) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_node_prop(node_id as NodeId, key_id as PropKeyId)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_del_node_prop(handle, node_id as NodeId, key_id as PropKeyId)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get a specific node property
  fn get_node_prop(&self, node_id: i64, key_id: u32) -> PyResult<Option<PyPropValue>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_prop(node_id as NodeId, key_id as PropKeyId)
          .map(|v| v.into()),
      ),
      Some(DatabaseInner::Graph(db)) => {
        Ok(get_node_prop_db(db, node_id as NodeId, key_id as PropKeyId).map(|v| v.into()))
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Direct Property Type Methods (skip PropValue wrapper for performance)
  // ========================================================================

  /// Get a string property directly (faster than get_node_prop)
  fn get_node_prop_string(&self, node_id: i64, key_id: u32) -> PyResult<Option<String>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_prop(node_id as NodeId, key_id as PropKeyId)
          .and_then(|v| match v {
            PropValue::String(s) => Some(s),
            _ => None,
          }),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_node_prop_db(db, node_id as NodeId, key_id as PropKeyId).and_then(|v| match v {
          PropValue::String(s) => Some(s),
          _ => None,
        }),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get an integer property directly (faster than get_node_prop)
  fn get_node_prop_int(&self, node_id: i64, key_id: u32) -> PyResult<Option<i64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_prop(node_id as NodeId, key_id as PropKeyId)
          .and_then(|v| match v {
            PropValue::I64(i) => Some(i),
            _ => None,
          }),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_node_prop_db(db, node_id as NodeId, key_id as PropKeyId).and_then(|v| match v {
          PropValue::I64(i) => Some(i),
          _ => None,
        }),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get a float property directly (faster than get_node_prop)
  fn get_node_prop_float(&self, node_id: i64, key_id: u32) -> PyResult<Option<f64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_prop(node_id as NodeId, key_id as PropKeyId)
          .and_then(|v| match v {
            PropValue::F64(f) => Some(f),
            _ => None,
          }),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_node_prop_db(db, node_id as NodeId, key_id as PropKeyId).and_then(|v| match v {
          PropValue::F64(f) => Some(f),
          _ => None,
        }),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get a bool property directly (faster than get_node_prop)
  fn get_node_prop_bool(&self, node_id: i64, key_id: u32) -> PyResult<Option<bool>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_prop(node_id as NodeId, key_id as PropKeyId)
          .and_then(|v| match v {
            PropValue::Bool(b) => Some(b),
            _ => None,
          }),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_node_prop_db(db, node_id as NodeId, key_id as PropKeyId).and_then(|v| match v {
          PropValue::Bool(b) => Some(b),
          _ => None,
        }),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Set a string property directly (faster than set_node_prop)
  fn set_node_prop_string(&self, node_id: i64, key_id: u32, value: String) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop(
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::String(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_set_node_prop(
          handle,
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::String(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Set an integer property directly (faster than set_node_prop)
  fn set_node_prop_int(&self, node_id: i64, key_id: u32, value: i64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop(
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::I64(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_set_node_prop(
          handle,
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::I64(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Set a float property directly (faster than set_node_prop)
  fn set_node_prop_float(&self, node_id: i64, key_id: u32, value: f64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop(
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::F64(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_set_node_prop(
          handle,
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::F64(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Set a bool property directly (faster than set_node_prop)
  fn set_node_prop_bool(&self, node_id: i64, key_id: u32, value: bool) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_prop(
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::Bool(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_set_node_prop(
          handle,
          node_id as NodeId,
          key_id as PropKeyId,
          PropValue::Bool(value),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get all properties for a node
  fn get_node_props(&self, node_id: i64) -> PyResult<Option<Vec<PyNodeProp>>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.get_node_props(node_id as NodeId).map(|props| {
          props
            .into_iter()
            .map(|(k, v)| PyNodeProp {
              key_id: k,
              value: v.into(),
            })
            .collect()
        }))
      }
      Some(DatabaseInner::Graph(db)) => Ok(get_node_props_db(db, node_id as NodeId).map(|props| {
        props
          .into_iter()
          .map(|(k, v)| PyNodeProp {
            key_id: k,
            value: v.into(),
          })
          .collect()
      })),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Edge Property Operations
  // ========================================================================

  /// Set an edge property
  fn set_edge_prop(
    &self,
    src: i64,
    etype: u32,
    dst: i64,
    key_id: u32,
    value: PyPropValue,
  ) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_edge_prop(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
          value.into(),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set edge property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_set_edge_prop(
          handle,
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
          value.into(),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set edge property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Set an edge property by key name
  fn set_edge_prop_by_name(
    &self,
    src: i64,
    etype: u32,
    dst: i64,
    key_name: String,
    value: PyPropValue,
  ) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_edge_prop_by_name(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          &key_name,
          value.into(),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set edge property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        let key_id = handle
          .db
          .get_propkey_id(&key_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown property key: {key_name}")))?;
        graph_set_edge_prop(
          handle,
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id,
          value.into(),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set edge property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Delete an edge property
  fn delete_edge_prop(&self, src: i64, etype: u32, dst: i64, key_id: u32) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_edge_prop(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete edge property: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_del_edge_prop(
          handle,
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete edge property: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get a specific edge property
  fn get_edge_prop(
    &self,
    src: i64,
    etype: u32,
    dst: i64,
    key_id: u32,
  ) -> PyResult<Option<PyPropValue>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_edge_prop(
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map(|v| v.into()),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_edge_prop_db(
          db,
          src as NodeId,
          etype as ETypeId,
          dst as NodeId,
          key_id as PropKeyId,
        )
        .map(|v| v.into()),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get all properties for an edge
  fn get_edge_props(&self, src: i64, etype: u32, dst: i64) -> PyResult<Option<Vec<PyNodeProp>>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_edge_props(src as NodeId, etype as ETypeId, dst as NodeId)
          .map(|props| {
            props
              .into_iter()
              .map(|(k, v)| PyNodeProp {
                key_id: k,
                value: v.into(),
              })
              .collect()
          }),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        get_edge_props_db(db, src as NodeId, etype as ETypeId, dst as NodeId).map(|props| {
          props
            .into_iter()
            .map(|(k, v)| PyNodeProp {
              key_id: k,
              value: v.into(),
            })
            .collect()
        }),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Vector Operations
  // ========================================================================

  /// Set a vector embedding for a node
  fn set_node_vector(&self, node_id: i64, prop_key_id: u32, vector: Vec<f64>) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let vector_f32: Vec<f32> = vector.iter().map(|&v| v as f32).collect();
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .set_node_vector(node_id as NodeId, prop_key_id as PropKeyId, &vector_f32)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set vector: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_set_node_vector(
          handle,
          node_id as NodeId,
          prop_key_id as PropKeyId,
          &vector_f32,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to set vector: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get a vector embedding for a node
  fn get_node_vector(&self, node_id: i64, prop_key_id: u32) -> PyResult<Option<Vec<f64>>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(
        db.get_node_vector(node_id as NodeId, prop_key_id as PropKeyId)
          .map(|v| v.iter().map(|&f| f as f64).collect()),
      ),
      Some(DatabaseInner::Graph(db)) => Ok(
        graph_get_node_vector_db(db, node_id as NodeId, prop_key_id as PropKeyId)
          .map(|v| v.iter().map(|&f| f as f64).collect()),
      ),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Delete a vector embedding for a node
  fn delete_node_vector(&self, node_id: i64, prop_key_id: u32) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .delete_node_vector(node_id as NodeId, prop_key_id as PropKeyId)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete vector: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_delete_node_vector(handle, node_id as NodeId, prop_key_id as PropKeyId)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to delete vector: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if a node has a vector embedding
  fn has_node_vector(&self, node_id: i64, prop_key_id: u32) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        Ok(db.has_node_vector(node_id as NodeId, prop_key_id as PropKeyId))
      }
      Some(DatabaseInner::Graph(db)) => Ok(graph_has_node_vector_db(
        db,
        node_id as NodeId,
        prop_key_id as PropKeyId,
      )),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Schema Operations
  // ========================================================================

  /// Get or create a label ID
  fn get_or_create_label(&self, name: String) -> PyResult<u32> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_or_create_label(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_or_create_label(&name)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get label ID by name
  fn get_label_id(&self, name: String) -> PyResult<Option<u32>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_label_id(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_label_id(&name)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get label name by ID
  fn get_label_name(&self, id: u32) -> PyResult<Option<String>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_label_name(id)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_label_name(id)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get or create an edge type ID
  fn get_or_create_etype(&self, name: String) -> PyResult<u32> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_or_create_etype(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_or_create_etype(&name)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get edge type ID by name
  fn get_etype_id(&self, name: String) -> PyResult<Option<u32>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_etype_id(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_etype_id(&name)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get edge type name by ID
  fn get_etype_name(&self, id: u32) -> PyResult<Option<String>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_etype_name(id)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_etype_name(id)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get or create a property key ID
  fn get_or_create_propkey(&self, name: String) -> PyResult<u32> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_or_create_propkey(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_or_create_propkey(&name)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get property key ID by name
  fn get_propkey_id(&self, name: String) -> PyResult<Option<u32>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_propkey_id(&name)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_propkey_id(&name)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get property key name by ID
  fn get_propkey_name(&self, id: u32) -> PyResult<Option<String>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_propkey_name(id)),
      Some(DatabaseInner::Graph(db)) => Ok(db.get_propkey_name(id)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Node Label Operations
  // ========================================================================

  /// Define a new label (requires transaction)
  fn define_label(&self, name: String) -> PyResult<u32> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .define_label(&name)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to define label: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        let label_id = graph_define_label(handle, &name)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to define label: {e}")))?;
        Ok(label_id)
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Add a label to a node
  fn add_node_label(&self, node_id: i64, label_id: u32) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_node_label(node_id as NodeId, label_id)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to add label: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_add_node_label(handle, node_id as NodeId, label_id)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to add label: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Add a label to a node by name
  fn add_node_label_by_name(&self, node_id: i64, label_name: String) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .add_node_label_by_name(node_id as NodeId, &label_name)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to add label: {e}"))),
      Some(DatabaseInner::Graph(db)) => {
        let label_id = db
          .get_label_id(&label_name)
          .ok_or_else(|| PyRuntimeError::new_err(format!("Unknown label: {label_name}")))?;
        self.with_graph_tx(db, |handle| {
          graph_add_node_label(handle, node_id as NodeId, label_id)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to add label: {e}")))?;
          Ok(())
        })
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Remove a label from a node
  fn remove_node_label(&self, node_id: i64, label_id: u32) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .remove_node_label(node_id as NodeId, label_id)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to remove label: {e}"))),
      Some(DatabaseInner::Graph(db)) => self.with_graph_tx(db, |handle| {
        graph_remove_node_label(handle, node_id as NodeId, label_id)
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to remove label: {e}")))?;
        Ok(())
      }),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if a node has a label
  fn node_has_label(&self, node_id: i64, label_id: u32) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.node_has_label(node_id as NodeId, label_id)),
      Some(DatabaseInner::Graph(db)) => Ok(node_has_label_db(db, node_id as NodeId, label_id)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get all labels for a node
  fn get_node_labels(&self, node_id: i64) -> PyResult<Vec<u32>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.get_node_labels(node_id as NodeId)),
      Some(DatabaseInner::Graph(db)) => Ok(get_node_labels_db(db, node_id as NodeId)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Checkpoint / Maintenance
  // ========================================================================

  /// Perform a checkpoint (compact WAL into snapshot)
  fn checkpoint(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .checkpoint()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to checkpoint: {e}"))),
      Some(DatabaseInner::Graph(_)) => Err(PyRuntimeError::new_err(
        "checkpoint() only supports single-file databases",
      )),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Perform a background (non-blocking) checkpoint
  fn background_checkpoint(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => db
        .background_checkpoint()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to background checkpoint: {e}"))),
      Some(DatabaseInner::Graph(_)) => Err(PyRuntimeError::new_err(
        "background_checkpoint() only supports single-file databases",
      )),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if checkpoint is recommended
  #[pyo3(signature = (threshold=None))]
  fn should_checkpoint(&self, threshold: Option<f64>) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.should_checkpoint(threshold.unwrap_or(0.8))),
      Some(DatabaseInner::Graph(_)) => Ok(false),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Optimize (compact) the database
  fn optimize(&self) -> PyResult<()> {
    let mut guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_mut() {
      Some(DatabaseInner::SingleFile(db)) => db
        .checkpoint()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to optimize: {e}"))),
      Some(DatabaseInner::Graph(db)) => db
        .optimize()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to optimize: {e}"))),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get database statistics
  fn stats(&self) -> PyResult<PyDbStats> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let s = db.stats();
        Ok(PyDbStats {
          snapshot_gen: s.snapshot_gen as i64,
          snapshot_nodes: s.snapshot_nodes as i64,
          snapshot_edges: s.snapshot_edges as i64,
          snapshot_max_node_id: s.snapshot_max_node_id as i64,
          delta_nodes_created: s.delta_nodes_created as i64,
          delta_nodes_deleted: s.delta_nodes_deleted as i64,
          delta_edges_added: s.delta_edges_added as i64,
          delta_edges_deleted: s.delta_edges_deleted as i64,
          wal_bytes: s.wal_bytes as i64,
          recommend_compact: s.recommend_compact,
        })
      }
      Some(DatabaseInner::Graph(db)) => Ok(graph_stats(db)),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check database integrity
  fn check(&self) -> PyResult<PyCheckResult> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(PyCheckResult::from(db.check())),
      Some(DatabaseInner::Graph(db)) => Ok(PyCheckResult::from(graph_check(db))),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Export / Import
  // ========================================================================

  /// Export database to a JSON object
  #[pyo3(signature = (options=None))]
  fn export_to_object(
    &self,
    py: Python<'_>,
    options: Option<PyExportOptions>,
  ) -> PyResult<PyObject> {
    let opts = options.unwrap_or_default().to_rust();
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let data = match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => ray_export::export_to_object_single(db, opts)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      Some(DatabaseInner::Graph(db)) => ray_export::export_to_object_graph(db, opts)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      None => return Err(PyRuntimeError::new_err("Database is closed")),
    };

    let json_str =
      serde_json::to_string(&data).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let json = py.import_bound("json")?;
    let obj = json.call_method1("loads", (json_str,))?;
    Ok(obj.to_object(py))
  }

  /// Export database to a JSON file
  #[pyo3(signature = (path, options=None))]
  fn export_to_json(
    &self,
    path: String,
    options: Option<PyExportOptions>,
  ) -> PyResult<PyExportResult> {
    let opts = options.unwrap_or_default().to_rust();
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let data = match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => ray_export::export_to_object_single(db, opts.clone())
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      Some(DatabaseInner::Graph(db)) => ray_export::export_to_object_graph(db, opts.clone())
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      None => return Err(PyRuntimeError::new_err("Database is closed")),
    };

    let result = ray_export::export_to_json(&data, path, opts.pretty)
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyExportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
    })
  }

  /// Export database to JSONL
  #[pyo3(signature = (path, options=None))]
  fn export_to_jsonl(
    &self,
    path: String,
    options: Option<PyExportOptions>,
  ) -> PyResult<PyExportResult> {
    let opts = options.unwrap_or_default().to_rust();
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let data = match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => ray_export::export_to_object_single(db, opts)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      Some(DatabaseInner::Graph(db)) => ray_export::export_to_object_graph(db, opts)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      None => return Err(PyRuntimeError::new_err("Database is closed")),
    };

    let result = ray_export::export_to_jsonl(&data, path)
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyExportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
    })
  }

  /// Import database from a JSON object
  #[pyo3(signature = (data, options=None))]
  fn import_from_object(
    &self,
    py: Python<'_>,
    data: PyObject,
    options: Option<PyImportOptions>,
  ) -> PyResult<PyImportResult> {
    let opts = options.unwrap_or_default().to_rust();
    let json = py.import_bound("json")?;
    let json_str = json.call_method1("dumps", (data,))?.extract::<String>()?;
    let parsed: ray_export::ExportedDatabase =
      serde_json::from_str(&json_str).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let result = match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        ray_export::import_from_object_single(db, &parsed, opts)
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
      }
      Some(DatabaseInner::Graph(db)) => ray_export::import_from_object_graph(db, &parsed, opts)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      None => return Err(PyRuntimeError::new_err("Database is closed")),
    };

    Ok(PyImportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
      skipped: result.skipped as i64,
    })
  }

  /// Import database from a JSON file
  #[pyo3(signature = (path, options=None))]
  fn import_from_json(
    &self,
    path: String,
    options: Option<PyImportOptions>,
  ) -> PyResult<PyImportResult> {
    let opts = options.unwrap_or_default().to_rust();
    let parsed =
      ray_export::import_from_json(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let result = match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        ray_export::import_from_object_single(db, &parsed, opts)
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
      }
      Some(DatabaseInner::Graph(db)) => ray_export::import_from_object_graph(db, &parsed, opts)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
      None => return Err(PyRuntimeError::new_err("Database is closed")),
    };

    Ok(PyImportResult {
      node_count: result.node_count as i64,
      edge_count: result.edge_count as i64,
      skipped: result.skipped as i64,
    })
  }

  // ========================================================================
  // Cache Operations
  // ========================================================================

  /// Check if caching is enabled
  fn cache_is_enabled(&self) -> PyResult<bool> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.cache_is_enabled()),
      Some(DatabaseInner::Graph(_)) => Ok(false),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Invalidate all caches for a node
  fn cache_invalidate_node(&self, node_id: i64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_invalidate_node(node_id as NodeId);
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Invalidate caches for a specific edge
  fn cache_invalidate_edge(&self, src: i64, etype: u32, dst: i64) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_invalidate_edge(src as NodeId, etype as ETypeId, dst as NodeId);
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Invalidate a cached key lookup
  fn cache_invalidate_key(&self, key: String) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_invalidate_key(&key);
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Clear all caches
  fn cache_clear(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Clear only the query cache
  fn cache_clear_query(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_query();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Clear only the key cache
  fn cache_clear_key(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_key();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Clear only the property cache
  fn cache_clear_property(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_property();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Clear only the traversal cache
  fn cache_clear_traversal(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_clear_traversal();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Get cache statistics
  fn cache_stats(&self) -> PyResult<Option<PyCacheStats>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(db.cache_stats().map(|s| PyCacheStats {
        property_cache_hits: s.property_cache_hits as i64,
        property_cache_misses: s.property_cache_misses as i64,
        property_cache_size: s.property_cache_size as i64,
        traversal_cache_hits: s.traversal_cache_hits as i64,
        traversal_cache_misses: s.traversal_cache_misses as i64,
        traversal_cache_size: s.traversal_cache_size as i64,
        query_cache_hits: s.query_cache_hits as i64,
        query_cache_misses: s.query_cache_misses as i64,
        query_cache_size: s.query_cache_size as i64,
      })),
      Some(DatabaseInner::Graph(_)) => Ok(None),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Reset cache statistics
  fn cache_reset_stats(&self) -> PyResult<()> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        db.cache_reset_stats();
        Ok(())
      }
      Some(DatabaseInner::Graph(_)) => Ok(()),
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Graph Traversal
  // ========================================================================

  /// Traverse outgoing edges from a node
  ///
  /// Args:
  ///   node_id: Starting node ID
  ///   etype: Edge type ID (optional, None = all types)
  ///
  /// Returns:
  ///   List of neighboring node IDs
  #[pyo3(signature = (node_id, etype=None))]
  fn traverse_out(&self, node_id: i64, etype: Option<u32>) -> PyResult<Vec<i64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let edges = db.get_out_edges(node_id as NodeId);
        let results: Vec<i64> = edges
          .into_iter()
          .filter(|(e, _)| etype.is_none() || etype == Some(*e))
          .map(|(_, dst)| dst as i64)
          .collect();
        Ok(results)
      }
      Some(DatabaseInner::Graph(db)) => {
        let edges = list_out_edges(db, node_id as NodeId);
        let results: Vec<i64> = edges
          .into_iter()
          .filter(|e| etype.is_none() || etype == Some(e.etype))
          .map(|e| e.dst as i64)
          .collect();
        Ok(results)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Traverse outgoing edges and return (node_id, key) pairs
  ///
  /// This is optimized for the fluent API - gets both ID and key in one FFI call.
  ///
  /// Args:
  ///   node_id: Starting node ID
  ///   etype: Edge type ID (optional, None = all types)
  ///
  /// Returns:
  ///   List of (node_id, key) tuples
  #[pyo3(signature = (node_id, etype=None))]
  fn traverse_out_with_keys(
    &self,
    node_id: i64,
    etype: Option<u32>,
  ) -> PyResult<Vec<(i64, Option<String>)>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let edges = db.get_out_edges(node_id as NodeId);
        let results: Vec<(i64, Option<String>)> = edges
          .into_iter()
          .filter(|(e, _)| etype.is_none() || etype == Some(*e))
          .map(|(_, dst)| {
            let key = db.get_node_key(dst);
            (dst as i64, key)
          })
          .collect();
        Ok(results)
      }
      Some(DatabaseInner::Graph(db)) => {
        let edges = list_out_edges(db, node_id as NodeId);
        let results: Vec<(i64, Option<String>)> = edges
          .into_iter()
          .filter(|e| etype.is_none() || etype == Some(e.etype))
          .map(|e| {
            let key = get_graph_node_key(db, e.dst);
            (e.dst as i64, key)
          })
          .collect();
        Ok(results)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Count outgoing edges from a node
  ///
  /// This is optimized for counting - doesn't allocate a list.
  ///
  /// Args:
  ///   node_id: Starting node ID
  ///   etype: Edge type ID (optional, None = all types)
  ///
  /// Returns:
  ///   Number of outgoing edges
  #[pyo3(signature = (node_id, etype=None))]
  fn traverse_out_count(&self, node_id: i64, etype: Option<u32>) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        if let Some(et) = etype {
          let count = db
            .get_out_edges(node_id as NodeId)
            .into_iter()
            .filter(|(e, _)| *e == et)
            .count();
          Ok(count as i64)
        } else {
          Ok(db.get_out_degree(node_id as NodeId) as i64)
        }
      }
      Some(DatabaseInner::Graph(db)) => {
        let edges = list_out_edges(db, node_id as NodeId);
        if let Some(et) = etype {
          let count = edges.into_iter().filter(|e| e.etype == et).count();
          Ok(count as i64)
        } else {
          Ok(edges.len() as i64)
        }
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Traverse incoming edges to a node
  ///
  /// Args:
  ///   node_id: Target node ID
  ///   etype: Edge type ID (optional, None = all types)
  ///
  /// Returns:
  ///   List of source node IDs
  #[pyo3(signature = (node_id, etype=None))]
  fn traverse_in(&self, node_id: i64, etype: Option<u32>) -> PyResult<Vec<i64>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let edges = db.get_in_edges(node_id as NodeId);
        let results: Vec<i64> = edges
          .into_iter()
          .filter(|(e, _)| etype.is_none() || etype == Some(*e))
          .map(|(_, src)| src as i64)
          .collect();
        Ok(results)
      }
      Some(DatabaseInner::Graph(db)) => {
        let edges = list_in_edges(db, node_id as NodeId);
        let results: Vec<i64> = edges
          .into_iter()
          .filter(|e| etype.is_none() || etype == Some(e.etype))
          .map(|e| e.dst as i64)
          .collect();
        Ok(results)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Traverse incoming edges and return (node_id, key) pairs
  ///
  /// This is optimized for the fluent API - gets both ID and key in one FFI call.
  ///
  /// Args:
  ///   node_id: Target node ID
  ///   etype: Edge type ID (optional, None = all types)
  ///
  /// Returns:
  ///   List of (node_id, key) tuples
  #[pyo3(signature = (node_id, etype=None))]
  fn traverse_in_with_keys(
    &self,
    node_id: i64,
    etype: Option<u32>,
  ) -> PyResult<Vec<(i64, Option<String>)>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let edges = db.get_in_edges(node_id as NodeId);
        let results: Vec<(i64, Option<String>)> = edges
          .into_iter()
          .filter(|(e, _)| etype.is_none() || etype == Some(*e))
          .map(|(_, src)| {
            let key = db.get_node_key(src);
            (src as i64, key)
          })
          .collect();
        Ok(results)
      }
      Some(DatabaseInner::Graph(db)) => {
        let edges = list_in_edges(db, node_id as NodeId);
        let results: Vec<(i64, Option<String>)> = edges
          .into_iter()
          .filter(|e| etype.is_none() || etype == Some(e.etype))
          .map(|e| {
            let key = get_graph_node_key(db, e.dst);
            (e.dst as i64, key)
          })
          .collect();
        Ok(results)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Count incoming edges to a node
  ///
  /// This is optimized for counting - doesn't allocate a list.
  ///
  /// Args:
  ///   node_id: Target node ID
  ///   etype: Edge type ID (optional, None = all types)
  ///
  /// Returns:
  ///   Number of incoming edges
  #[pyo3(signature = (node_id, etype=None))]
  fn traverse_in_count(&self, node_id: i64, etype: Option<u32>) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        if let Some(et) = etype {
          let count = db
            .get_in_edges(node_id as NodeId)
            .into_iter()
            .filter(|(e, _)| *e == et)
            .count();
          Ok(count as i64)
        } else {
          Ok(db.get_in_degree(node_id as NodeId) as i64)
        }
      }
      Some(DatabaseInner::Graph(db)) => {
        let edges = list_in_edges(db, node_id as NodeId);
        if let Some(et) = etype {
          let count = edges.into_iter().filter(|e| e.etype == et).count();
          Ok(count as i64)
        } else {
          Ok(edges.len() as i64)
        }
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Execute a multi-step traversal entirely in Rust
  ///
  /// This is optimized for the fluent API - executes all steps in a single FFI call.
  /// Each step is a tuple of (direction, etype) where direction is "out", "in", or "both".
  ///
  /// Args:
  ///   start_ids: Starting node IDs
  ///   steps: List of (direction, etype) tuples
  ///
  /// Returns:
  ///   List of (node_id, key) tuples for final results
  #[pyo3(signature = (start_ids, steps))]
  fn traverse_multi(
    &self,
    start_ids: Vec<i64>,
    steps: Vec<(String, Option<u32>)>,
  ) -> PyResult<Vec<(i64, Option<String>)>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let mut current_ids: Vec<NodeId> = start_ids.iter().map(|&id| id as NodeId).collect();

        for (direction, etype) in steps {
          let mut next_ids: Vec<NodeId> = Vec::new();
          let mut visited: HashSet<NodeId> = HashSet::new();

          for node_id in &current_ids {
            let neighbors: Vec<NodeId> = match direction.as_str() {
              "out" => db
                .get_out_edges(*node_id)
                .into_iter()
                .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                .map(|(_, dst)| dst)
                .collect(),
              "in" => db
                .get_in_edges(*node_id)
                .into_iter()
                .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                .map(|(_, src)| src)
                .collect(),
              _ => {
                let mut out: Vec<NodeId> = db
                  .get_out_edges(*node_id)
                  .into_iter()
                  .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                  .map(|(_, dst)| dst)
                  .collect();
                let in_edges: Vec<NodeId> = db
                  .get_in_edges(*node_id)
                  .into_iter()
                  .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                  .map(|(_, src)| src)
                  .collect();
                out.extend(in_edges);
                out
              }
            };

            for neighbor_id in neighbors {
              if !visited.contains(&neighbor_id) {
                visited.insert(neighbor_id);
                next_ids.push(neighbor_id);
              }
            }
          }

          current_ids = next_ids;
        }

        let results: Vec<(i64, Option<String>)> = current_ids
          .into_iter()
          .map(|id| {
            let key = db.get_node_key(id);
            (id as i64, key)
          })
          .collect();

        Ok(results)
      }
      Some(DatabaseInner::Graph(db)) => {
        let mut current_ids: Vec<NodeId> = start_ids.iter().map(|&id| id as NodeId).collect();

        for (direction, etype) in steps {
          let mut next_ids: Vec<NodeId> = Vec::new();
          let mut visited: HashSet<NodeId> = HashSet::new();

          for node_id in &current_ids {
            let neighbors: Vec<NodeId> = match direction.as_str() {
              "out" => list_out_edges(db, *node_id)
                .into_iter()
                .filter(|e| etype.is_none() || etype == Some(e.etype))
                .map(|e| e.dst)
                .collect(),
              "in" => list_in_edges(db, *node_id)
                .into_iter()
                .filter(|e| etype.is_none() || etype == Some(e.etype))
                .map(|e| e.dst)
                .collect(),
              _ => {
                let mut out: Vec<NodeId> = list_out_edges(db, *node_id)
                  .into_iter()
                  .filter(|e| etype.is_none() || etype == Some(e.etype))
                  .map(|e| e.dst)
                  .collect();
                let in_edges: Vec<NodeId> = list_in_edges(db, *node_id)
                  .into_iter()
                  .filter(|e| etype.is_none() || etype == Some(e.etype))
                  .map(|e| e.dst)
                  .collect();
                out.extend(in_edges);
                out
              }
            };

            for neighbor_id in neighbors {
              if !visited.contains(&neighbor_id) {
                visited.insert(neighbor_id);
                next_ids.push(neighbor_id);
              }
            }
          }

          current_ids = next_ids;
        }

        let results: Vec<(i64, Option<String>)> = current_ids
          .into_iter()
          .map(|id| {
            let key = get_graph_node_key(db, id);
            (id as i64, key)
          })
          .collect();

        Ok(results)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Execute a multi-step traversal and return just the count
  ///
  /// This is the fastest option when you only need the count.
  #[pyo3(signature = (start_ids, steps))]
  fn traverse_multi_count(
    &self,
    start_ids: Vec<i64>,
    steps: Vec<(String, Option<u32>)>,
  ) -> PyResult<i64> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let mut current_ids: Vec<NodeId> = start_ids.iter().map(|&id| id as NodeId).collect();

        for (direction, etype) in steps {
          let mut next_ids: Vec<NodeId> = Vec::new();
          let mut visited: HashSet<NodeId> = HashSet::new();

          for node_id in &current_ids {
            let neighbors: Vec<NodeId> = match direction.as_str() {
              "out" => db
                .get_out_edges(*node_id)
                .into_iter()
                .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                .map(|(_, dst)| dst)
                .collect(),
              "in" => db
                .get_in_edges(*node_id)
                .into_iter()
                .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                .map(|(_, src)| src)
                .collect(),
              _ => {
                let mut out: Vec<NodeId> = db
                  .get_out_edges(*node_id)
                  .into_iter()
                  .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                  .map(|(_, dst)| dst)
                  .collect();
                let in_edges: Vec<NodeId> = db
                  .get_in_edges(*node_id)
                  .into_iter()
                  .filter(|(e, _)| etype.is_none() || etype == Some(*e))
                  .map(|(_, src)| src)
                  .collect();
                out.extend(in_edges);
                out
              }
            };

            for neighbor_id in neighbors {
              if !visited.contains(&neighbor_id) {
                visited.insert(neighbor_id);
                next_ids.push(neighbor_id);
              }
            }
          }

          current_ids = next_ids;
        }

        Ok(current_ids.len() as i64)
      }
      Some(DatabaseInner::Graph(db)) => {
        let mut current_ids: Vec<NodeId> = start_ids.iter().map(|&id| id as NodeId).collect();

        for (direction, etype) in steps {
          let mut next_ids: Vec<NodeId> = Vec::new();
          let mut visited: HashSet<NodeId> = HashSet::new();

          for node_id in &current_ids {
            let neighbors: Vec<NodeId> = match direction.as_str() {
              "out" => list_out_edges(db, *node_id)
                .into_iter()
                .filter(|e| etype.is_none() || etype == Some(e.etype))
                .map(|e| e.dst)
                .collect(),
              "in" => list_in_edges(db, *node_id)
                .into_iter()
                .filter(|e| etype.is_none() || etype == Some(e.etype))
                .map(|e| e.dst)
                .collect(),
              _ => {
                let mut out: Vec<NodeId> = list_out_edges(db, *node_id)
                  .into_iter()
                  .filter(|e| etype.is_none() || etype == Some(e.etype))
                  .map(|e| e.dst)
                  .collect();
                let in_edges: Vec<NodeId> = list_in_edges(db, *node_id)
                  .into_iter()
                  .filter(|e| etype.is_none() || etype == Some(e.etype))
                  .map(|e| e.dst)
                  .collect();
                out.extend(in_edges);
                out
              }
            };

            for neighbor_id in neighbors {
              if !visited.contains(&neighbor_id) {
                visited.insert(neighbor_id);
                next_ids.push(neighbor_id);
              }
            }
          }

          current_ids = next_ids;
        }

        Ok(current_ids.len() as i64)
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Variable-depth traversal from a node
  ///
  /// Similar to TypeScript: db.from(node).traverse(etype, { maxDepth: 3 })
  ///
  /// Args:
  ///   node_id: Starting node ID
  ///   max_depth: Maximum number of hops
  ///   etype: Edge type ID (optional, None = all types)
  ///   min_depth: Minimum depth (default 1)
  ///   direction: "out", "in", or "both" (default "out")
  ///   unique: Only visit each node once (default True)
  ///
  /// Returns:
  ///   List of TraversalResult objects
  #[pyo3(signature = (node_id, max_depth, etype=None, min_depth=None, direction=None, unique=None))]
  fn traverse(
    &self,
    node_id: i64,
    max_depth: u32,
    etype: Option<u32>,
    min_depth: Option<u32>,
    direction: Option<String>,
    unique: Option<bool>,
  ) -> PyResult<Vec<PyTraversalResult>> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let dir = match direction.as_deref() {
      Some("in") => TraversalDirection::In,
      Some("both") => TraversalDirection::Both,
      _ => TraversalDirection::Out,
    };

    let opts = TraverseOptions {
      direction: dir,
      min_depth: min_depth.unwrap_or(1) as usize,
      max_depth: max_depth as usize,
      unique: unique.unwrap_or(true),
      where_edge: None,
      where_node: None,
    };

    let to_py_results = |iter: Vec<crate::api::traversal::TraversalResult>| {
      iter
        .into_iter()
        .map(|r| {
          let (edge_src, edge_dst, edge_type) = match r.edge {
            Some(e) => (Some(e.src as i64), Some(e.dst as i64), Some(e.etype)),
            None => (None, None, None),
          };
          PyTraversalResult {
            node_id: r.node_id as i64,
            depth: r.depth as u32,
            edge_src,
            edge_dst,
            edge_type,
          }
        })
        .collect::<Vec<_>>()
    };

    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let get_neighbors =
          |nid: NodeId, d: TraversalDirection, et: Option<ETypeId>| -> Vec<Edge> {
            get_neighbors_from_single_file(db, nid, d, et)
          };
        let results: Vec<crate::api::traversal::TraversalResult> =
          RustTraversalBuilder::new(vec![node_id as NodeId])
            .traverse(etype, opts)
            .execute(get_neighbors)
            .collect();
        Ok(to_py_results(results))
      }
      Some(DatabaseInner::Graph(db)) => {
        let get_neighbors = |nid: NodeId,
                             d: TraversalDirection,
                             et: Option<ETypeId>|
         -> Vec<Edge> { get_neighbors_from_graph_db(db, nid, d, et) };
        let results: Vec<crate::api::traversal::TraversalResult> =
          RustTraversalBuilder::new(vec![node_id as NodeId])
            .traverse(etype, opts)
            .execute(get_neighbors)
            .collect();
        Ok(to_py_results(results))
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  // ========================================================================
  // Pathfinding
  // ========================================================================

  /// Find shortest path using BFS (unweighted)
  ///
  /// Similar to TypeScript: db.shortestPath(src).to(dst).bfs()
  ///
  /// Args:
  ///   source: Source node ID
  ///   target: Target node ID
  ///   etype: Edge type to traverse (optional, None = all)
  ///   max_depth: Maximum search depth (default 100)
  ///   direction: "out", "in", or "both" (default "out")
  ///
  /// Returns:
  ///   PathResult with path nodes and edges
  #[pyo3(signature = (source, target, etype=None, max_depth=None, direction=None))]
  fn find_path_bfs(
    &self,
    source: i64,
    target: i64,
    etype: Option<u32>,
    max_depth: Option<u32>,
    direction: Option<String>,
  ) -> PyResult<PyPathResult> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let dir = match direction.as_deref() {
      Some("in") => TraversalDirection::In,
      Some("both") => TraversalDirection::Both,
      _ => TraversalDirection::Out,
    };

    let mut targets = HashSet::new();
    targets.insert(target as NodeId);

    let mut allowed_etypes = HashSet::new();
    if let Some(e) = etype {
      allowed_etypes.insert(e);
    }

    let config = PathConfig {
      source: source as NodeId,
      targets,
      allowed_etypes,
      direction: dir,
      max_depth: max_depth.unwrap_or(100) as usize,
    };

    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let get_neighbors =
          |nid: NodeId, d: TraversalDirection, et: Option<ETypeId>| -> Vec<Edge> {
            get_neighbors_from_single_file(db, nid, d, et)
          };
        let result = bfs(config, get_neighbors);
        Ok(result.into())
      }
      Some(DatabaseInner::Graph(db)) => {
        let get_neighbors = |nid: NodeId,
                             d: TraversalDirection,
                             et: Option<ETypeId>|
         -> Vec<Edge> { get_neighbors_from_graph_db(db, nid, d, et) };
        let result = bfs(config, get_neighbors);
        Ok(result.into())
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Find shortest path using Dijkstra's algorithm (weighted)
  ///
  /// Similar to TypeScript: db.shortestPath(src).to(dst).dijkstra()
  ///
  /// Note: All edges have weight 1.0 by default. For custom weights,
  /// use find_path_dijkstra_weighted with edge property name.
  ///
  /// Args:
  ///   source: Source node ID
  ///   target: Target node ID
  ///   etype: Edge type to traverse (optional, None = all)
  ///   max_depth: Maximum search depth (default 100)
  ///   direction: "out", "in", or "both" (default "out")
  ///
  /// Returns:
  ///   PathResult with path nodes, edges, and total weight
  #[pyo3(signature = (source, target, etype=None, max_depth=None, direction=None))]
  fn find_path_dijkstra(
    &self,
    source: i64,
    target: i64,
    etype: Option<u32>,
    max_depth: Option<u32>,
    direction: Option<String>,
  ) -> PyResult<PyPathResult> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let dir = match direction.as_deref() {
      Some("in") => TraversalDirection::In,
      Some("both") => TraversalDirection::Both,
      _ => TraversalDirection::Out,
    };

    let mut targets = HashSet::new();
    targets.insert(target as NodeId);

    let mut allowed_etypes = HashSet::new();
    if let Some(e) = etype {
      allowed_etypes.insert(e);
    }

    let config = PathConfig {
      source: source as NodeId,
      targets,
      allowed_etypes,
      direction: dir,
      max_depth: max_depth.unwrap_or(100) as usize,
    };

    let get_weight = |_src: NodeId, _etype: ETypeId, _dst: NodeId| -> f64 { 1.0 };

    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => {
        let get_neighbors =
          |nid: NodeId, d: TraversalDirection, et: Option<ETypeId>| -> Vec<Edge> {
            get_neighbors_from_single_file(db, nid, d, et)
          };
        let result = dijkstra(config, get_neighbors, get_weight);
        Ok(result.into())
      }
      Some(DatabaseInner::Graph(db)) => {
        let get_neighbors = |nid: NodeId,
                             d: TraversalDirection,
                             et: Option<ETypeId>|
         -> Vec<Edge> { get_neighbors_from_graph_db(db, nid, d, et) };
        let result = dijkstra(config, get_neighbors, get_weight);
        Ok(result.into())
      }
      None => Err(PyRuntimeError::new_err("Database is closed")),
    }
  }

  /// Check if a path exists between two nodes
  ///
  /// Args:
  ///   source: Source node ID
  ///   target: Target node ID
  ///   etype: Edge type (optional)
  ///   max_depth: Maximum search depth (default 100)
  ///
  /// Returns:
  ///   True if path exists
  #[pyo3(signature = (source, target, etype=None, max_depth=None))]
  fn has_path(
    &self,
    source: i64,
    target: i64,
    etype: Option<u32>,
    max_depth: Option<u32>,
  ) -> PyResult<bool> {
    let result = self.find_path_bfs(source, target, etype, max_depth, None)?;
    Ok(result.found)
  }

  /// Get all nodes reachable from a source within a certain depth
  ///
  /// Args:
  ///   source: Source node ID
  ///   max_depth: Maximum depth to traverse
  ///   etype: Edge type (optional)
  ///
  /// Returns:
  ///   List of reachable node IDs
  #[pyo3(signature = (source, max_depth, etype=None))]
  fn reachable_nodes(&self, source: i64, max_depth: u32, etype: Option<u32>) -> PyResult<Vec<i64>> {
    let results = self.traverse(
      source,
      max_depth,
      etype,
      Some(1),
      Some("out".to_string()),
      Some(true),
    )?;
    Ok(results.into_iter().map(|r| r.node_id).collect())
  }

  // ========================================================================
  // Context Manager Support
  // ========================================================================

  fn __enter__(slf: Py<Self>) -> Py<Self> {
    slf
  }

  #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
  fn __exit__(
    &self,
    _exc_type: Option<PyObject>,
    _exc_value: Option<PyObject>,
    _traceback: Option<PyObject>,
  ) -> PyResult<bool> {
    self.close()?;
    Ok(false)
  }

  fn __repr__(&self) -> PyResult<String> {
    let guard = self
      .inner
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match guard.as_ref() {
      Some(DatabaseInner::SingleFile(db)) => Ok(format!(
        "Database(path=\"{}\", read_only={})",
        db.path.display(),
        db.read_only
      )),
      Some(DatabaseInner::Graph(db)) => Ok(format!(
        "Database(path=\"{}\", read_only={})",
        db.path.display(),
        db.read_only
      )),
      None => Ok("Database(closed)".to_string()),
    }
  }
}

impl PyDatabase {
  fn with_graph_tx<F, R>(&self, db: &RustGraphDB, f: F) -> PyResult<R>
  where
    F: FnOnce(&mut GraphTxHandle) -> PyResult<R>,
  {
    let mut guard = self
      .graph_tx
      .lock()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let tx_state = guard
      .take()
      .ok_or_else(|| PyRuntimeError::new_err("No active transaction"))?;
    let mut handle = GraphTxHandle::new(db, tx_state);
    let result = f(&mut handle);
    *guard = Some(handle.tx);
    result
  }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Open a database file (standalone function)
#[pyfunction]
#[pyo3(signature = (path, options=None))]
pub fn open_database(path: String, options: Option<PyOpenOptions>) -> PyResult<PyDatabase> {
  PyDatabase::new(path, options)
}

// ============================================================================
// Metrics / Health
// ============================================================================

#[pyfunction]
pub fn collect_metrics(db: &PyDatabase) -> PyResult<PyDatabaseMetrics> {
  let guard = db
    .inner
    .lock()
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  match guard.as_ref() {
    Some(DatabaseInner::SingleFile(db)) => Ok(collect_metrics_single_file(db)),
    Some(DatabaseInner::Graph(db)) => Ok(collect_metrics_graph(db)),
    None => Err(PyRuntimeError::new_err("Database is closed")),
  }
}

#[pyfunction]
pub fn health_check(db: &PyDatabase) -> PyResult<PyHealthCheckResult> {
  let guard = db
    .inner
    .lock()
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  match guard.as_ref() {
    Some(DatabaseInner::SingleFile(db)) => Ok(health_check_single_file(db)),
    Some(DatabaseInner::Graph(db)) => Ok(health_check_graph(db)),
    None => Err(PyRuntimeError::new_err("Database is closed")),
  }
}

// ============================================================================
// Backup / Restore
// ============================================================================

/// Create a backup from an open database handle
#[pyfunction]
#[pyo3(signature = (db, backup_path, options=None))]
pub fn create_backup(
  db: &PyDatabase,
  backup_path: String,
  options: Option<PyBackupOptions>,
) -> PyResult<PyBackupResult> {
  let options = options.unwrap_or_default();
  let do_checkpoint = options.checkpoint.unwrap_or(true);
  let overwrite = options.overwrite.unwrap_or(false);
  let mut backup_path = PathBuf::from(backup_path);

  if backup_path.exists() && !overwrite {
    return Err(PyRuntimeError::new_err(
      "Backup already exists at path (use overwrite: true)",
    ));
  }

  let guard = db
    .inner
    .lock()
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

  match guard.as_ref() {
    Some(DatabaseInner::SingleFile(db)) => {
      if !backup_path.to_string_lossy().ends_with(EXT_RAYDB) {
        backup_path = PathBuf::from(format!("{}{}", backup_path.to_string_lossy(), EXT_RAYDB));
      }

      if do_checkpoint && !db.read_only {
        db.checkpoint()
          .map_err(|e| PyRuntimeError::new_err(format!("Failed to checkpoint: {e}")))?;
      }

      ensure_parent_dir(&backup_path)?;

      if overwrite && backup_path.exists() {
        remove_existing(&backup_path)?;
      }

      copy_file_with_size(&db.path, &backup_path)?;
      let size = fs::metadata(&backup_path)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        .len();

      Ok(backup_result(
        &backup_path,
        size,
        "single-file",
        SystemTime::now(),
      ))
    }
    Some(DatabaseInner::Graph(db)) => {
      if overwrite && backup_path.exists() {
        remove_existing(&backup_path)?;
      }

      fs::create_dir_all(&backup_path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
      fs::create_dir_all(backup_path.join(SNAPSHOTS_DIR))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
      fs::create_dir_all(backup_path.join(WAL_DIR))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

      let mut total_size = 0u64;
      let manifest_src = db.path.join(MANIFEST_FILENAME);
      if manifest_src.exists() {
        total_size += copy_file_with_size(&manifest_src, &backup_path.join(MANIFEST_FILENAME))?;
      }

      let snapshots_dir = db.path.join(SNAPSHOTS_DIR);
      if snapshots_dir.exists() {
        for entry in
          fs::read_dir(&snapshots_dir).map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        {
          let entry = entry.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
          let src = entry.path();
          if entry
            .file_type()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .is_file()
          {
            let dst = backup_path.join(SNAPSHOTS_DIR).join(entry.file_name());
            total_size += copy_file_with_size(&src, &dst)?;
          }
        }
      }

      let wal_dir = db.path.join(WAL_DIR);
      if wal_dir.exists() {
        for entry in fs::read_dir(&wal_dir).map_err(|e| PyRuntimeError::new_err(e.to_string()))? {
          let entry = entry.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
          let src = entry.path();
          if entry
            .file_type()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .is_file()
          {
            let dst = backup_path.join(WAL_DIR).join(entry.file_name());
            total_size += copy_file_with_size(&src, &dst)?;
          }
        }
      }

      Ok(backup_result(
        &backup_path,
        total_size,
        "multi-file",
        SystemTime::now(),
      ))
    }
    None => Err(PyRuntimeError::new_err("Database is closed")),
  }
}

/// Restore a backup into a target path
#[pyfunction]
#[pyo3(signature = (backup_path, restore_path, options=None))]
pub fn restore_backup(
  backup_path: String,
  restore_path: String,
  options: Option<PyRestoreOptions>,
) -> PyResult<String> {
  let options = options.unwrap_or_default();
  let overwrite = options.overwrite.unwrap_or(false);
  let backup_path = PathBuf::from(backup_path);
  let mut restore_path = PathBuf::from(restore_path);

  if !backup_path.exists() {
    return Err(PyRuntimeError::new_err("Backup not found at path"));
  }

  if restore_path.exists() && !overwrite {
    return Err(PyRuntimeError::new_err(
      "Database already exists at restore path (use overwrite: true)",
    ));
  }

  let metadata = fs::metadata(&backup_path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  if metadata.is_file() {
    if !restore_path.to_string_lossy().ends_with(EXT_RAYDB) {
      restore_path = PathBuf::from(format!("{}{}", restore_path.to_string_lossy(), EXT_RAYDB));
    }

    ensure_parent_dir(&restore_path)?;

    if overwrite && restore_path.exists() {
      remove_existing(&restore_path)?;
    }

    copy_file_with_size(&backup_path, &restore_path)?;
    Ok(restore_path.to_string_lossy().to_string())
  } else if metadata.is_dir() {
    if overwrite && restore_path.exists() {
      remove_existing(&restore_path)?;
    }

    fs::create_dir_all(&restore_path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    fs::create_dir_all(restore_path.join(SNAPSHOTS_DIR))
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    fs::create_dir_all(restore_path.join(WAL_DIR))
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let manifest_src = backup_path.join(MANIFEST_FILENAME);
    if manifest_src.exists() {
      copy_file_with_size(&manifest_src, &restore_path.join(MANIFEST_FILENAME))?;
    }

    let snapshots_dir = backup_path.join(SNAPSHOTS_DIR);
    if snapshots_dir.exists() {
      for entry in
        fs::read_dir(&snapshots_dir).map_err(|e| PyRuntimeError::new_err(e.to_string()))?
      {
        let entry = entry.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let src = entry.path();
        if entry
          .file_type()
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
          .is_file()
        {
          let dst = restore_path.join(SNAPSHOTS_DIR).join(entry.file_name());
          copy_file_with_size(&src, &dst)?;
        }
      }
    }

    let wal_dir = backup_path.join(WAL_DIR);
    if wal_dir.exists() {
      for entry in fs::read_dir(&wal_dir).map_err(|e| PyRuntimeError::new_err(e.to_string()))? {
        let entry = entry.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let src = entry.path();
        if entry
          .file_type()
          .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
          .is_file()
        {
          let dst = restore_path.join(WAL_DIR).join(entry.file_name());
          copy_file_with_size(&src, &dst)?;
        }
      }
    }

    Ok(restore_path.to_string_lossy().to_string())
  } else {
    Err(PyRuntimeError::new_err(
      "Backup path is not a file or directory",
    ))
  }
}

/// Inspect a backup without restoring it
#[pyfunction]
pub fn get_backup_info(backup_path: String) -> PyResult<PyBackupResult> {
  let backup_path = PathBuf::from(backup_path);
  if !backup_path.exists() {
    return Err(PyRuntimeError::new_err("Backup not found at path"));
  }

  let metadata = fs::metadata(&backup_path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  let timestamp = metadata.modified().unwrap_or_else(|_| SystemTime::now());

  if metadata.is_file() {
    Ok(backup_result(
      &backup_path,
      metadata.len(),
      "single-file",
      timestamp,
    ))
  } else if metadata.is_dir() {
    let size = dir_size(&backup_path)?;
    Ok(backup_result(&backup_path, size, "multi-file", timestamp))
  } else {
    Err(PyRuntimeError::new_err(
      "Backup path is not a file or directory",
    ))
  }
}

/// Create a backup from a database path without opening it
#[pyfunction]
#[pyo3(signature = (db_path, backup_path, options=None))]
pub fn create_offline_backup(
  db_path: String,
  backup_path: String,
  options: Option<PyOfflineBackupOptions>,
) -> PyResult<PyBackupResult> {
  let options = options.unwrap_or_default();
  let overwrite = options.overwrite.unwrap_or(false);
  let db_path = PathBuf::from(db_path);
  let backup_path = PathBuf::from(backup_path);

  if !db_path.exists() {
    return Err(PyRuntimeError::new_err("Database not found at path"));
  }

  if backup_path.exists() && !overwrite {
    return Err(PyRuntimeError::new_err(
      "Backup already exists at path (use overwrite: true)",
    ));
  }

  let metadata = fs::metadata(&db_path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  if metadata.is_file() {
    ensure_parent_dir(&backup_path)?;
    if overwrite && backup_path.exists() {
      remove_existing(&backup_path)?;
    }
    copy_file_with_size(&db_path, &backup_path)?;
    let size = fs::metadata(&backup_path)
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
      .len();
    Ok(backup_result(
      &backup_path,
      size,
      "single-file",
      SystemTime::now(),
    ))
  } else if metadata.is_dir() {
    if overwrite && backup_path.exists() {
      remove_existing(&backup_path)?;
    }
    let size = copy_dir_recursive(&db_path, &backup_path)?;
    Ok(backup_result(
      &backup_path,
      size,
      "multi-file",
      SystemTime::now(),
    ))
  } else {
    Err(PyRuntimeError::new_err(
      "Database path is not a file or directory",
    ))
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn get_graph_node_key(db: &RustGraphDB, node_id: NodeId) -> Option<String> {
  let delta = db.delta.read();
  graph_get_node_key(db.snapshot.as_ref(), &delta, node_id)
}

fn system_time_to_millis(time: SystemTime) -> i64 {
  match time.duration_since(UNIX_EPOCH) {
    Ok(duration) => duration.as_millis() as i64,
    Err(_) => 0,
  }
}

fn ensure_parent_dir(path: &Path) -> PyResult<()> {
  if let Some(parent) = path.parent() {
    if !parent.as_os_str().is_empty() {
      fs::create_dir_all(parent).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }
  }
  Ok(())
}

fn remove_existing(path: &Path) -> PyResult<()> {
  if path.is_dir() {
    fs::remove_dir_all(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  } else {
    fs::remove_file(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  }
  Ok(())
}

fn copy_file_with_size(src: &Path, dst: &Path) -> PyResult<u64> {
  fs::copy(src, dst).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  let size = fs::metadata(src)
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
    .len();
  Ok(size)
}

fn dir_size(path: &Path) -> PyResult<u64> {
  let mut total = 0u64;
  for entry in fs::read_dir(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))? {
    let entry = entry.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let entry_path = entry.path();
    let metadata = entry
      .metadata()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    if metadata.is_dir() {
      total += dir_size(&entry_path)?;
    } else {
      total += metadata.len();
    }
  }
  Ok(total)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> PyResult<u64> {
  fs::create_dir_all(dst).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
  let mut total = 0u64;
  for entry in fs::read_dir(src).map_err(|e| PyRuntimeError::new_err(e.to_string()))? {
    let entry = entry.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let src_path = entry.path();
    let dst_path = dst.join(entry.file_name());
    let metadata = entry
      .metadata()
      .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    if metadata.is_dir() {
      total += copy_dir_recursive(&src_path, &dst_path)?;
    } else {
      total += copy_file_with_size(&src_path, &dst_path)?;
    }
  }
  Ok(total)
}

fn backup_result(path: &Path, size: u64, kind: &str, timestamp: SystemTime) -> PyBackupResult {
  PyBackupResult {
    path: path.to_string_lossy().to_string(),
    size: size as i64,
    timestamp: system_time_to_millis(timestamp),
    r#type: kind.to_string(),
  }
}

fn graph_stats(db: &RustGraphDB) -> PyDbStats {
  let node_count = graph_count_nodes(db);
  let edge_count = graph_count_edges(db, None);

  let delta = db.delta.read();
  let delta_nodes_created = delta.created_nodes.len();
  let delta_nodes_deleted = delta.deleted_nodes.len();
  let delta_edges_added = delta.total_edges_added();
  let delta_edges_deleted = delta.total_edges_deleted();
  drop(delta);

  let (snapshot_gen, snapshot_nodes, snapshot_edges, snapshot_max_node_id) =
    if let Some(ref snapshot) = db.snapshot {
      (
        snapshot.header.generation,
        snapshot.header.num_nodes,
        snapshot.header.num_edges,
        snapshot.header.max_node_id,
      )
    } else {
      (0, 0, 0, 0)
    };

  let total_changes =
    delta_nodes_created + delta_nodes_deleted + delta_edges_added + delta_edges_deleted;
  let recommend_compact = total_changes > 10_000;

  PyDbStats {
    snapshot_gen: snapshot_gen as i64,
    snapshot_nodes: snapshot_nodes.max(node_count) as i64,
    snapshot_edges: snapshot_edges.max(edge_count) as i64,
    snapshot_max_node_id: snapshot_max_node_id as i64,
    delta_nodes_created: delta_nodes_created as i64,
    delta_nodes_deleted: delta_nodes_deleted as i64,
    delta_edges_added: delta_edges_added as i64,
    delta_edges_deleted: delta_edges_deleted as i64,
    wal_bytes: db.wal_bytes() as i64,
    recommend_compact,
  }
}

fn graph_check(db: &RustGraphDB) -> RustCheckResult {
  let mut errors = Vec::new();
  let mut warnings = Vec::new();

  let all_nodes = graph_list_nodes(db);
  let node_count = all_nodes.len();

  if node_count == 0 {
    warnings.push("No nodes in database".to_string());
    return RustCheckResult {
      valid: true,
      errors,
      warnings,
    };
  }

  let all_edges = graph_list_edges(db, ListEdgesOptions::default());
  let edge_count = all_edges.len();

  for edge in &all_edges {
    if !node_exists_db(db, edge.src) {
      errors.push(format!(
        "Edge references non-existent source node: {} -[{}]-> {}",
        edge.src, edge.etype, edge.dst
      ));
    }

    if !node_exists_db(db, edge.dst) {
      errors.push(format!(
        "Edge references non-existent destination node: {} -[{}]-> {}",
        edge.src, edge.etype, edge.dst
      ));
    }
  }

  for edge in &all_edges {
    let exists = edge_exists_db(db, edge.src, edge.etype, edge.dst);
    if !exists {
      errors.push(format!(
        "Edge inconsistency: edge {} -[{}]-> {} listed but not found via edge_exists",
        edge.src, edge.etype, edge.dst
      ));
    }
  }

  let counted_nodes = graph_count_nodes(db);
  let counted_edges = graph_count_edges(db, None);

  if counted_nodes as usize != node_count {
    warnings.push(format!(
      "Node count mismatch: list_nodes returned {node_count} but count_nodes returned {counted_nodes}"
    ));
  }

  if counted_edges as usize != edge_count {
    warnings.push(format!(
      "Edge count mismatch: list_edges returned {edge_count} but count_edges returned {counted_edges}"
    ));
  }

  RustCheckResult {
    valid: errors.is_empty(),
    errors,
    warnings,
  }
}

fn calc_hit_rate(hits: u64, misses: u64) -> f64 {
  let total = hits + misses;
  if total > 0 {
    hits as f64 / total as f64
  } else {
    0.0
  }
}

fn build_cache_layer_metrics(
  hits: u64,
  misses: u64,
  size: usize,
  max_size: usize,
) -> PyCacheLayerMetrics {
  PyCacheLayerMetrics {
    hits: hits as i64,
    misses: misses as i64,
    hit_rate: calc_hit_rate(hits, misses),
    size: size as i64,
    max_size: max_size as i64,
    utilization_percent: if max_size > 0 {
      (size as f64 / max_size as f64) * 100.0
    } else {
      0.0
    },
  }
}

fn empty_cache_layer_metrics() -> PyCacheLayerMetrics {
  PyCacheLayerMetrics {
    hits: 0,
    misses: 0,
    hit_rate: 0.0,
    size: 0,
    max_size: 0,
    utilization_percent: 0.0,
  }
}

fn build_cache_metrics(stats: Option<&CacheManagerStats>) -> PyCacheMetrics {
  match stats {
    Some(stats) => PyCacheMetrics {
      enabled: true,
      property_cache: build_cache_layer_metrics(
        stats.property_cache_hits,
        stats.property_cache_misses,
        stats.property_cache_size,
        stats.property_cache_max_size,
      ),
      traversal_cache: build_cache_layer_metrics(
        stats.traversal_cache_hits,
        stats.traversal_cache_misses,
        stats.traversal_cache_size,
        stats.traversal_cache_max_size,
      ),
      query_cache: build_cache_layer_metrics(
        stats.query_cache_hits,
        stats.query_cache_misses,
        stats.query_cache_size,
        stats.query_cache_max_size,
      ),
    },
    None => PyCacheMetrics {
      enabled: false,
      property_cache: empty_cache_layer_metrics(),
      traversal_cache: empty_cache_layer_metrics(),
      query_cache: empty_cache_layer_metrics(),
    },
  }
}

fn estimate_delta_memory(delta: &DeltaState) -> i64 {
  let mut bytes = 0i64;

  bytes += delta.created_nodes.len() as i64 * 100;
  bytes += delta.deleted_nodes.len() as i64 * 8;
  bytes += delta.modified_nodes.len() as i64 * 100;

  for patches in delta.out_add.values() {
    bytes += patches.len() as i64 * 24;
  }
  for patches in delta.out_del.values() {
    bytes += patches.len() as i64 * 24;
  }
  for patches in delta.in_add.values() {
    bytes += patches.len() as i64 * 24;
  }
  for patches in delta.in_del.values() {
    bytes += patches.len() as i64 * 24;
  }

  bytes += delta.edge_props.len() as i64 * 50;
  bytes += delta.key_index.len() as i64 * 40;

  bytes
}

fn estimate_cache_memory(stats: Option<&CacheManagerStats>) -> i64 {
  match stats {
    Some(stats) => {
      (stats.property_cache_size as i64 * 100)
        + (stats.traversal_cache_size as i64 * 200)
        + (stats.query_cache_size as i64 * 500)
    }
    None => 0,
  }
}

fn delta_health_size(delta: &DeltaState) -> usize {
  delta.created_nodes.len()
    + delta.deleted_nodes.len()
    + delta.modified_nodes.len()
    + delta.out_add.len()
    + delta.in_add.len()
}

fn collect_metrics_single_file(db: &RustSingleFileDB) -> PyDatabaseMetrics {
  let stats = db.stats();
  let delta = db.delta.read();
  let cache_stats = db.cache.read().as_ref().map(|cache| cache.stats());

  let node_count = stats.snapshot_nodes as i64 + stats.delta_nodes_created as i64
    - stats.delta_nodes_deleted as i64;
  let edge_count =
    stats.snapshot_edges as i64 + stats.delta_edges_added as i64 - stats.delta_edges_deleted as i64;

  let data = PyDataMetrics {
    node_count,
    edge_count,
    delta_nodes_created: stats.delta_nodes_created as i64,
    delta_nodes_deleted: stats.delta_nodes_deleted as i64,
    delta_edges_added: stats.delta_edges_added as i64,
    delta_edges_deleted: stats.delta_edges_deleted as i64,
    snapshot_generation: stats.snapshot_gen as i64,
    max_node_id: stats.snapshot_max_node_id as i64,
    schema_labels: delta.new_labels.len() as i64,
    schema_etypes: delta.new_etypes.len() as i64,
    schema_prop_keys: delta.new_propkeys.len() as i64,
  };

  let cache = build_cache_metrics(cache_stats.as_ref());
  let delta_bytes = estimate_delta_memory(&delta);
  let cache_bytes = estimate_cache_memory(cache_stats.as_ref());
  let snapshot_bytes = (stats.snapshot_nodes as i64 * 50) + (stats.snapshot_edges as i64 * 20);

  PyDatabaseMetrics {
    path: db.path.to_string_lossy().to_string(),
    is_single_file: true,
    read_only: db.read_only,
    data,
    cache,
    mvcc: None,
    memory: PyMemoryMetrics {
      delta_estimate_bytes: delta_bytes,
      cache_estimate_bytes: cache_bytes,
      snapshot_bytes,
      total_estimate_bytes: delta_bytes + cache_bytes + snapshot_bytes,
    },
    collected_at: system_time_to_millis(SystemTime::now()),
  }
}

fn collect_metrics_graph(db: &RustGraphDB) -> PyDatabaseMetrics {
  let stats = graph_stats(db);
  let delta = db.delta.read();

  let node_count = stats.snapshot_nodes + stats.delta_nodes_created - stats.delta_nodes_deleted;
  let edge_count = stats.snapshot_edges + stats.delta_edges_added - stats.delta_edges_deleted;

  let data = PyDataMetrics {
    node_count,
    edge_count,
    delta_nodes_created: stats.delta_nodes_created,
    delta_nodes_deleted: stats.delta_nodes_deleted,
    delta_edges_added: stats.delta_edges_added,
    delta_edges_deleted: stats.delta_edges_deleted,
    snapshot_generation: stats.snapshot_gen,
    max_node_id: stats.snapshot_max_node_id,
    schema_labels: delta.new_labels.len() as i64,
    schema_etypes: delta.new_etypes.len() as i64,
    schema_prop_keys: delta.new_propkeys.len() as i64,
  };

  let cache = build_cache_metrics(None);
  let delta_bytes = estimate_delta_memory(&delta);
  let snapshot_bytes = (stats.snapshot_nodes * 50) + (stats.snapshot_edges * 20);

  PyDatabaseMetrics {
    path: db.path.to_string_lossy().to_string(),
    is_single_file: false,
    read_only: db.read_only,
    data,
    cache,
    mvcc: None,
    memory: PyMemoryMetrics {
      delta_estimate_bytes: delta_bytes,
      cache_estimate_bytes: 0,
      snapshot_bytes,
      total_estimate_bytes: delta_bytes + snapshot_bytes,
    },
    collected_at: system_time_to_millis(SystemTime::now()),
  }
}

fn health_check_single_file(db: &RustSingleFileDB) -> PyHealthCheckResult {
  let mut checks = Vec::new();

  checks.push(PyHealthCheckEntry {
    name: "database_open".to_string(),
    passed: true,
    message: "Database handle is valid".to_string(),
  });

  let delta = db.delta.read();
  let delta_size = delta_health_size(&delta);
  let delta_ok = delta_size < 100000;
  checks.push(PyHealthCheckEntry {
    name: "delta_size".to_string(),
    passed: delta_ok,
    message: if delta_ok {
      format!("Delta size is reasonable ({delta_size} entries)")
    } else {
      format!("Delta is large ({delta_size} entries) - consider checkpointing")
    },
  });

  let cache_stats = db.cache.read().as_ref().map(|cache| cache.stats());
  if let Some(stats) = cache_stats {
    let total_hits = stats.property_cache_hits + stats.traversal_cache_hits;
    let total_misses = stats.property_cache_misses + stats.traversal_cache_misses;
    let total = total_hits + total_misses;
    let hit_rate = if total > 0 {
      total_hits as f64 / total as f64
    } else {
      1.0
    };
    let cache_ok = hit_rate > 0.5 || total < 100;
    checks.push(PyHealthCheckEntry {
      name: "cache_efficiency".to_string(),
      passed: cache_ok,
      message: if cache_ok {
        format!("Cache hit rate: {:.1}%", hit_rate * 100.0)
      } else {
        format!(
          "Low cache hit rate: {:.1}% - consider adjusting cache size",
          hit_rate * 100.0
        )
      },
    });
  }

  if db.read_only {
    checks.push(PyHealthCheckEntry {
      name: "write_access".to_string(),
      passed: true,
      message: "Database is read-only".to_string(),
    });
  }

  let healthy = checks.iter().all(|check| check.passed);
  PyHealthCheckResult { healthy, checks }
}

fn health_check_graph(db: &RustGraphDB) -> PyHealthCheckResult {
  let mut checks = Vec::new();

  checks.push(PyHealthCheckEntry {
    name: "database_open".to_string(),
    passed: true,
    message: "Database handle is valid".to_string(),
  });

  let delta = db.delta.read();
  let delta_size = delta_health_size(&delta);
  let delta_ok = delta_size < 100000;
  checks.push(PyHealthCheckEntry {
    name: "delta_size".to_string(),
    passed: delta_ok,
    message: if delta_ok {
      format!("Delta size is reasonable ({delta_size} entries)")
    } else {
      format!("Delta is large ({delta_size} entries) - consider checkpointing")
    },
  });

  if db.read_only {
    checks.push(PyHealthCheckEntry {
      name: "write_access".to_string(),
      passed: true,
      message: "Database is read-only".to_string(),
    });
  }

  let healthy = checks.iter().all(|check| check.passed);
  PyHealthCheckResult { healthy, checks }
}

/// Get neighbors from database for traversal
fn get_neighbors_from_single_file(
  db: &RustSingleFileDB,
  node_id: NodeId,
  direction: TraversalDirection,
  etype: Option<ETypeId>,
) -> Vec<Edge> {
  let mut edges = Vec::new();
  match direction {
    TraversalDirection::Out => {
      for (e, dst) in db.get_out_edges(node_id) {
        if etype.is_none() || etype == Some(e) {
          edges.push(Edge {
            src: node_id,
            etype: e,
            dst,
          });
        }
      }
    }
    TraversalDirection::In => {
      for (e, src) in db.get_in_edges(node_id) {
        if etype.is_none() || etype == Some(e) {
          edges.push(Edge {
            src,
            etype: e,
            dst: node_id,
          });
        }
      }
    }
    TraversalDirection::Both => {
      edges.extend(get_neighbors_from_single_file(
        db,
        node_id,
        TraversalDirection::Out,
        etype,
      ));
      edges.extend(get_neighbors_from_single_file(
        db,
        node_id,
        TraversalDirection::In,
        etype,
      ));
    }
  }
  edges
}

fn get_neighbors_from_graph_db(
  db: &RustGraphDB,
  node_id: NodeId,
  direction: TraversalDirection,
  etype: Option<ETypeId>,
) -> Vec<Edge> {
  let mut edges = Vec::new();
  match direction {
    TraversalDirection::Out => {
      for edge in list_out_edges(db, node_id) {
        if etype.is_none() || etype == Some(edge.etype) {
          edges.push(Edge {
            src: node_id,
            etype: edge.etype,
            dst: edge.dst,
          });
        }
      }
    }
    TraversalDirection::In => {
      for edge in list_in_edges(db, node_id) {
        if etype.is_none() || etype == Some(edge.etype) {
          edges.push(Edge {
            src: edge.dst,
            etype: edge.etype,
            dst: node_id,
          });
        }
      }
    }
    TraversalDirection::Both => {
      edges.extend(get_neighbors_from_graph_db(
        db,
        node_id,
        TraversalDirection::Out,
        etype,
      ));
      edges.extend(get_neighbors_from_graph_db(
        db,
        node_id,
        TraversalDirection::In,
        etype,
      ));
    }
  }
  edges
}

/// Convert Rust PathResult to Python PathResult
impl From<crate::api::pathfinding::PathResult> for PyPathResult {
  fn from(result: crate::api::pathfinding::PathResult) -> Self {
    Self {
      path: result.path.iter().map(|&id| id as i64).collect(),
      edges: result
        .edges
        .iter()
        .map(|&(src, etype, dst)| PyPathEdge {
          src: src as i64,
          etype,
          dst: dst as i64,
        })
        .collect(),
      total_weight: result.total_weight,
      found: result.found,
    }
  }
}
