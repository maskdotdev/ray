//! Python bindings for RayDB using PyO3
//!
//! Exposes SingleFileDB and related types to Python.

#[cfg(feature = "python")]
pub mod database;
#[cfg(feature = "python")]
pub mod traversal;
#[cfg(feature = "python")]
pub mod vector;

#[cfg(feature = "python")]
pub use database::*;
#[cfg(feature = "python")]
pub use traversal::*;
#[cfg(feature = "python")]
pub use vector::*;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// RayDB Python module
#[cfg(feature = "python")]
#[pymodule]
#[pyo3(name = "_raydb")]
pub fn raydb(m: &Bound<'_, PyModule>) -> PyResult<()> {
  // Database classes
  m.add_class::<database::PyDatabase>()?;
  m.add_class::<database::PyOpenOptions>()?;
  m.add_class::<database::PySyncMode>()?;
  m.add_class::<database::PyDbStats>()?;
  m.add_class::<database::PyCheckResult>()?;
  m.add_class::<database::PyCacheStats>()?;
  m.add_class::<database::PyExportOptions>()?;
  m.add_class::<database::PyImportOptions>()?;
  m.add_class::<database::PyExportResult>()?;
  m.add_class::<database::PyImportResult>()?;
  m.add_class::<database::PyStreamOptions>()?;
  m.add_class::<database::PyPaginationOptions>()?;
  m.add_class::<database::PyNodeWithProps>()?;
  m.add_class::<database::PyEdgeWithProps>()?;
  m.add_class::<database::PyNodePage>()?;
  m.add_class::<database::PyEdgePage>()?;
  m.add_class::<database::PyCacheLayerMetrics>()?;
  m.add_class::<database::PyCacheMetrics>()?;
  m.add_class::<database::PyDataMetrics>()?;
  m.add_class::<database::PyMvccMetrics>()?;
  m.add_class::<database::PyMemoryMetrics>()?;
  m.add_class::<database::PyDatabaseMetrics>()?;
  m.add_class::<database::PyHealthCheckEntry>()?;
  m.add_class::<database::PyHealthCheckResult>()?;
  m.add_class::<database::PyBackupOptions>()?;
  m.add_class::<database::PyRestoreOptions>()?;
  m.add_class::<database::PyOfflineBackupOptions>()?;
  m.add_class::<database::PyBackupResult>()?;
  m.add_class::<database::PyPropValue>()?;
  m.add_class::<database::PyEdge>()?;
  m.add_class::<database::PyFullEdge>()?;
  m.add_class::<database::PyNodeProp>()?;

  // Traversal result classes
  m.add_class::<traversal::PyTraversalResult>()?;
  m.add_class::<traversal::PyPathResult>()?;
  m.add_class::<traversal::PyPathEdge>()?;

  // Vector search classes
  m.add_class::<vector::PyIvfIndex>()?;
  m.add_class::<vector::PyIvfPqIndex>()?;
  m.add_class::<vector::PyIvfConfig>()?;
  m.add_class::<vector::PyPqConfig>()?;
  m.add_class::<vector::PySearchOptions>()?;
  m.add_class::<vector::PySearchResult>()?;
  m.add_class::<vector::PyIvfStats>()?;

  // Standalone functions
  m.add_function(wrap_pyfunction!(database::open_database, m)?)?;
  m.add_function(wrap_pyfunction!(database::collect_metrics, m)?)?;
  m.add_function(wrap_pyfunction!(database::health_check, m)?)?;
  m.add_function(wrap_pyfunction!(database::create_backup, m)?)?;
  m.add_function(wrap_pyfunction!(database::restore_backup, m)?)?;
  m.add_function(wrap_pyfunction!(database::get_backup_info, m)?)?;
  m.add_function(wrap_pyfunction!(database::create_offline_backup, m)?)?;
  m.add_function(wrap_pyfunction!(version, m)?)?;
  m.add_function(wrap_pyfunction!(vector::brute_force_search, m)?)?;

  Ok(())
}

/// Get RayDB version
#[cfg(feature = "python")]
#[pyfunction]
pub fn version() -> String {
  env!("CARGO_PKG_VERSION").to_string()
}
