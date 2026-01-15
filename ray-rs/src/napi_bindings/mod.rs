//! NAPI bindings for RayDB
//!
//! Exposes SingleFileDB and related types to Node.js/Bun.

pub mod database;

pub use database::{
  open_database, Database, DbStats, JsEdge, JsNodeProp, JsPropValue, OpenOptions, PropType,
};
