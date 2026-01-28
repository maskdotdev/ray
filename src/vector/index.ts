/**
 * Vector embeddings module for RayDB
 *
 * Lance-style columnar storage with IVF index for approximate nearest neighbor search.
 */

// Types
export type {
  VectorStoreConfig,
  IvfConfig,
  RowGroup,
  Fragment,
  VectorManifest,
  IvfIndex,
  VectorSearchResult,
  SecondaryIndexType,
  SecondaryIndexConfig,
  BTreeNode,
  SecondaryIndex,
  FilterOperators,
  FastFilter,
  CompiledFilter,
  VectorSearchOptions,
  MultiVectorSearchOptions,
  BatchInsertOptions,
  VectorManifestHeader,
  FragmentHeader,
  IvfIndexHeader,
  SetNodeVectorPayload,
  DelNodeVectorPayload,
  BatchVectorsPayload,
  SealFragmentPayload,
  CompactFragmentsPayload,
  VectorDeltaState,
} from "./types.js";

export { DEFAULT_VECTOR_CONFIG, DEFAULT_IVF_CONFIG } from "./types.js";

// Normalization & Validation
export {
  validateVector,
  hasNaN,
  hasInfinity,
  isZeroVector,
  l2Norm,
  normalizeInPlace,
  normalize,
  isNormalized,
  normalizeRowGroup,
  normalizeVectorAt,
  isNormalizedAt,
} from "./normalize.js";
export type { VectorValidationResult } from "./normalize.js";

// Distance functions
export {
  dotProduct,
  cosineDistance,
  cosineSimilarity,
  squaredEuclidean,
  euclideanDistance,
  dotProductAt,
  squaredEuclideanAt,
  batchCosineDistance,
  batchSquaredEuclidean,
  batchDotProductDistance,
  getDistanceFunction,
  getBatchDistanceFunction,
  distanceToSimilarity,
  findKNearest,
  MinHeap,
  MaxHeap,
} from "./distance.js";

// Row group operations
export {
  createRowGroup,
  rowGroupAppend,
  rowGroupGet,
  rowGroupGetCopy,
  rowGroupIsFull,
  rowGroupRemainingCapacity,
  rowGroupByteSize,
  rowGroupUsedByteSize,
  rowGroupTrim,
  rowGroupFromData,
  rowGroupIterator,
  rowGroupCopy,
} from "./row-group.js";

// Fragment operations
export {
  createFragment,
  fragmentAppend,
  fragmentDelete,
  fragmentIsDeleted,
  fragmentUndelete,
  fragmentSeal,
  fragmentShouldSeal,
  fragmentGetVector,
  fragmentLiveCount,
  fragmentDeletionRatio,
  fragmentByteSize,
  fragmentIterator,
  fragmentFromData,
  fragmentClone,
} from "./fragment.js";

// Columnar store
export {
  createVectorStore,
  vectorStoreInsert,
  vectorStoreDelete,
  vectorStoreGet,
  vectorStoreGetById,
  vectorStoreHas,
  vectorStoreGetVectorId,
  vectorStoreGetNodeId,
  vectorStoreGetLocation,
  vectorStoreIterator,
  vectorStoreIteratorWithIds,
  vectorStoreBatchInsert,
  vectorStoreStats,
  vectorStoreFragmentStats,
  vectorStoreSealActive,
  vectorStoreGetAllVectors,
  vectorStoreClear,
  vectorStoreClone,
} from "./columnar-store.js";

// IVF index
export {
  createIvfIndex,
  ivfAddTrainingVectors,
  ivfTrain,
  ivfInsert,
  ivfDelete,
  ivfSearch,
  ivfSearchMulti,
  ivfBuildFromStore,
  ivfStats,
  ivfClear,
} from "./ivf-index.js";

// Compaction
export {
  findFragmentsToCompact,
  compactFragments,
  applyCompaction,
  runCompactionIfNeeded,
  getCompactionStats,
  forceFullCompaction,
  clearDeletedFragments,
  DEFAULT_COMPACTION_STRATEGY,
} from "./compaction.js";
export type { CompactionStrategy } from "./compaction.js";

// Serialization
export {
  ivfSerializedSize,
  serializeIvf,
  deserializeIvf,
  manifestSerializedSize,
  serializeManifest,
  deserializeManifest,
} from "./ivf-serialize.js";

// Product Quantization
export {
  createPQIndex,
  pqTrain,
  pqEncode,
  pqEncodeOne,
  pqBuildDistanceTable,
  pqDistanceADC,
  pqSearch,
  pqSearchWithTable,
  pqStats,
  DEFAULT_PQ_CONFIG,
} from "./pq.js";
export type { PQConfig, PQIndex } from "./pq.js";

// IVF-PQ Combined Index
export {
  createIvfPqIndex,
  ivfPqAddTrainingVectors,
  ivfPqTrain,
  ivfPqInsert,
  ivfPqSearch,
  ivfPqStats,
  DEFAULT_IVF_PQ_CONFIG,
} from "./ivf-pq.js";
export type { IvfPqConfig, IvfPqIndex } from "./ivf-pq.js";
