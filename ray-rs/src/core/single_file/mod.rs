//! Single-file database format (.raydb)
//!
//! Provides open/close/read/write operations for single-file databases.
//! Layout: [Header (1 page)] [WAL (N pages)] [Snapshot (M pages)]
//!
//! Ported from src/ray/graph-db/single-file.ts

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::constants::*;
use crate::core::pager::FilePager;
use crate::core::snapshot::reader::SnapshotData;
use crate::core::wal::buffer::WalBuffer;
use crate::types::*;
use crate::vector::types::VectorManifest;

// Submodules
mod checkpoint;
mod iter;
mod open;
mod read;
mod recovery;
mod schema;
mod transaction;
mod vector;
mod write;

// Re-export everything for backward compatibility
pub use iter::*;
pub use open::*;

// Also re-export recovery items that are used externally
pub use recovery::replay_wal_record;

// ============================================================================
// Transaction State (for single-file DB)
// ============================================================================

/// Transaction state for SingleFileDB
///
/// This is simpler than the main TxState in types.rs, as SingleFileDB
/// handles operations differently.
#[derive(Debug)]
pub struct SingleFileTxState {
  pub txid: TxId,
  pub read_only: bool,
  pub snapshot_ts: u64,
}

impl SingleFileTxState {
  pub fn new(txid: TxId, read_only: bool, snapshot_ts: u64) -> Self {
    Self {
      txid,
      read_only,
      snapshot_ts,
    }
  }
}

// ============================================================================
// Single-File GraphDB
// ============================================================================

/// Single-file database handle
pub struct SingleFileDB {
  /// Database file path
  pub path: PathBuf,
  /// Read-only mode
  pub read_only: bool,
  /// Page-based I/O
  pub pager: Mutex<FilePager>,
  /// Database header
  pub header: RwLock<DbHeaderV1>,
  /// WAL circular buffer manager
  pub wal_buffer: Mutex<WalBuffer>,
  /// Memory-mapped snapshot data (if exists)
  pub snapshot: RwLock<Option<SnapshotData>>,
  /// Delta state (uncommitted changes)
  pub delta: RwLock<DeltaState>,

  // ID allocators
  pub(crate) next_node_id: AtomicU64,
  pub(crate) next_label_id: AtomicU32,
  pub(crate) next_etype_id: AtomicU32,
  pub(crate) next_propkey_id: AtomicU32,
  pub(crate) next_tx_id: AtomicU64,

  /// Current active transaction
  pub current_tx: Mutex<Option<SingleFileTxState>>,

  /// Label name -> ID mapping
  pub(crate) label_names: RwLock<HashMap<String, LabelId>>,
  /// ID -> label name mapping
  pub(crate) label_ids: RwLock<HashMap<LabelId, String>>,
  /// Edge type name -> ID mapping
  pub(crate) etype_names: RwLock<HashMap<String, ETypeId>>,
  /// ID -> edge type name mapping
  pub(crate) etype_ids: RwLock<HashMap<ETypeId, String>>,
  /// Property key name -> ID mapping
  pub(crate) propkey_names: RwLock<HashMap<String, PropKeyId>>,
  /// ID -> property key name mapping
  pub(crate) propkey_ids: RwLock<HashMap<PropKeyId, String>>,

  /// Enable auto-checkpoint when WAL usage exceeds threshold
  pub(crate) auto_checkpoint: bool,
  /// WAL usage threshold (0.0-1.0) to trigger auto-checkpoint
  pub(crate) checkpoint_threshold: f64,
  /// Use background (non-blocking) checkpoint instead of blocking
  pub(crate) background_checkpoint: bool,
  /// Current checkpoint state
  pub(crate) checkpoint_status: Mutex<CheckpointStatus>,

  /// Vector stores keyed by property key ID
  /// Each property key can have its own vector store with different dimensions
  pub(crate) vector_stores: RwLock<HashMap<PropKeyId, VectorManifest>>,
}

/// Checkpoint state for background checkpointing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStatus {
  /// No checkpoint in progress
  Idle,
  /// Background checkpoint is running (writes go to secondary WAL)
  Running,
  /// Completing checkpoint (brief lock for final updates)
  Completing,
}

// ============================================================================
// SingleFileDB Implementation - ID Allocators
// ============================================================================

impl SingleFileDB {
  /// Allocate a new node ID
  pub fn alloc_node_id(&self) -> NodeId {
    self.next_node_id.fetch_add(1, Ordering::SeqCst)
  }

  /// Allocate a new label ID
  pub fn alloc_label_id(&self) -> LabelId {
    self.next_label_id.fetch_add(1, Ordering::SeqCst)
  }

  /// Allocate a new edge type ID
  pub fn alloc_etype_id(&self) -> ETypeId {
    self.next_etype_id.fetch_add(1, Ordering::SeqCst)
  }

  /// Allocate a new property key ID
  pub fn alloc_propkey_id(&self) -> PropKeyId {
    self.next_propkey_id.fetch_add(1, Ordering::SeqCst)
  }

  /// Allocate a new transaction ID
  pub fn alloc_tx_id(&self) -> TxId {
    self.next_tx_id.fetch_add(1, Ordering::SeqCst)
  }

  /// Check if a node exists
  pub fn node_exists(&self, node_id: NodeId) -> bool {
    let delta = self.delta.read();

    if delta.is_node_deleted(node_id) {
      return false;
    }

    if delta.is_node_created(node_id) {
      return true;
    }

    // Check snapshot
    if let Some(ref snapshot) = *self.snapshot.read() {
      return snapshot.has_node(node_id);
    }

    false
  }

  /// Check if an edge exists
  pub fn edge_exists(&self, src: NodeId, etype: ETypeId, dst: NodeId) -> bool {
    let delta = self.delta.read();

    if delta.is_edge_deleted(src, etype, dst) {
      return false;
    }

    if delta.is_edge_added(src, etype, dst) {
      return true;
    }

    // Check snapshot
    if let Some(ref snapshot) = *self.snapshot.read() {
      if let (Some(src_phys), Some(dst_phys)) =
        (snapshot.get_phys_node(src), snapshot.get_phys_node(dst))
      {
        return snapshot.has_edge(src_phys, etype, dst_phys);
      }
    }

    false
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Check if a path is a single-file database
pub fn is_single_file_path<P: AsRef<Path>>(path: P) -> bool {
  path
    .as_ref()
    .extension()
    .map(|ext| ext == "raydb")
    .unwrap_or(false)
}

/// Get the single-file extension
pub fn single_file_extension() -> &'static str {
  EXT_RAYDB
}
