/**
 * Consistency Verification Helpers for Stress Tests
 *
 * Provides utilities to verify graph invariants and consistency.
 */

import type {
  GraphDB,
  NodeID,
  ETypeID,
  PropKeyID,
  PropValue,
} from "../../../src/types.ts";
import {
  nodeExists,
  edgeExists,
  getNeighborsOut,
  getNeighborsIn,
  getNodeProp,
  getNodeByKey,
} from "../../../src/index.ts";

export interface VerificationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  stats: {
    nodesChecked: number;
    edgesChecked: number;
    propsChecked: number;
  };
}

/**
 * Verify that all expected nodes exist
 */
export function verifyNodesExist(
  db: GraphDB,
  expectedNodeIds: NodeID[]
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  let nodesChecked = 0;

  for (const nodeId of expectedNodeIds) {
    nodesChecked++;
    if (!nodeExists(db, nodeId)) {
      errors.push(`Node ${nodeId} expected to exist but not found`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked, edgesChecked: 0, propsChecked: 0 },
  };
}

/**
 * Verify that all expected nodes do NOT exist
 */
export function verifyNodesDeleted(
  db: GraphDB,
  deletedNodeIds: NodeID[]
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  let nodesChecked = 0;

  for (const nodeId of deletedNodeIds) {
    nodesChecked++;
    if (nodeExists(db, nodeId)) {
      errors.push(`Node ${nodeId} expected to be deleted but still exists`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked, edgesChecked: 0, propsChecked: 0 },
  };
}

/**
 * Verify edge existence
 */
export function verifyEdgesExist(
  db: GraphDB,
  expectedEdges: Array<{ src: NodeID; etype: ETypeID; dst: NodeID }>
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  let edgesChecked = 0;

  for (const { src, etype, dst } of expectedEdges) {
    edgesChecked++;
    if (!edgeExists(db, src, etype, dst)) {
      errors.push(`Edge (${src})-[${etype}]->(${dst}) expected but not found`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked: 0, edgesChecked, propsChecked: 0 },
  };
}

/**
 * Verify edges do NOT exist
 */
export function verifyEdgesDeleted(
  db: GraphDB,
  deletedEdges: Array<{ src: NodeID; etype: ETypeID; dst: NodeID }>
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  let edgesChecked = 0;

  for (const { src, etype, dst } of deletedEdges) {
    edgesChecked++;
    if (edgeExists(db, src, etype, dst)) {
      errors.push(`Edge (${src})-[${etype}]->(${dst}) expected deleted but exists`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked: 0, edgesChecked, propsChecked: 0 },
  };
}

/**
 * Verify edge count for a node
 */
export function verifyOutDegree(
  db: GraphDB,
  nodeId: NodeID,
  expectedCount: number
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  
  let count = 0;
  for (const _ of getNeighborsOut(db, nodeId)) {
    count++;
  }

  if (count !== expectedCount) {
    errors.push(`Node ${nodeId} out-degree: expected ${expectedCount}, got ${count}`);
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked: 1, edgesChecked: count, propsChecked: 0 },
  };
}

/**
 * Verify in-degree for a node
 */
export function verifyInDegree(
  db: GraphDB,
  nodeId: NodeID,
  expectedCount: number
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  
  let count = 0;
  for (const _ of getNeighborsIn(db, nodeId)) {
    count++;
  }

  if (count !== expectedCount) {
    errors.push(`Node ${nodeId} in-degree: expected ${expectedCount}, got ${count}`);
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked: 1, edgesChecked: count, propsChecked: 0 },
  };
}

/**
 * Verify property value
 */
export function verifyNodeProp(
  db: GraphDB,
  nodeId: NodeID,
  propKey: PropKeyID,
  expectedValue: PropValue | null
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  
  const actual = getNodeProp(db, nodeId, propKey);

  if (expectedValue === null) {
    if (actual !== null) {
      errors.push(`Node ${nodeId} prop ${propKey}: expected null, got ${JSON.stringify(actual)}`);
    }
  } else {
    if (actual === null) {
      errors.push(`Node ${nodeId} prop ${propKey}: expected ${JSON.stringify(expectedValue)}, got null`);
    } else if (JSON.stringify(actual) !== JSON.stringify(expectedValue)) {
      errors.push(`Node ${nodeId} prop ${propKey}: expected ${JSON.stringify(expectedValue)}, got ${JSON.stringify(actual)}`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked: 1, edgesChecked: 0, propsChecked: 1 },
  };
}

/**
 * Verify key index integrity
 */
export function verifyKeyIndex(
  db: GraphDB,
  expectedKeys: Map<string, NodeID>
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  let nodesChecked = 0;

  for (const [key, expectedNodeId] of expectedKeys) {
    nodesChecked++;
    const actualNodeId = getNodeByKey(db, key);
    
    if (actualNodeId === null) {
      errors.push(`Key "${key}" not found, expected node ${expectedNodeId}`);
    } else if (actualNodeId !== expectedNodeId) {
      errors.push(`Key "${key}": expected node ${expectedNodeId}, got ${actualNodeId}`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked, edgesChecked: 0, propsChecked: 0 },
  };
}

/**
 * Verify no orphan edges (edges pointing to deleted nodes)
 */
export function verifyNoOrphanEdges(
  db: GraphDB,
  nodeIds: NodeID[],
  etypes: ETypeID[]
): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  let edgesChecked = 0;
  
  const nodeSet = new Set(nodeIds);

  for (const nodeId of nodeIds) {
    if (!nodeExists(db, nodeId)) continue;
    
    for (const neighbor of getNeighborsOut(db, nodeId)) {
      edgesChecked++;
      if (!nodeSet.has(neighbor.dst)) {
        // Could be a node we don't know about - just warn
        if (!nodeExists(db, neighbor.dst)) {
          errors.push(`Orphan edge: (${nodeId})-[${neighbor.etype}]->(${neighbor.dst}) points to non-existent node`);
        }
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats: { nodesChecked: nodeIds.length, edgesChecked, propsChecked: 0 },
  };
}

/**
 * Combine multiple verification results
 */
export function combineResults(...results: VerificationResult[]): VerificationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  const stats = { nodesChecked: 0, edgesChecked: 0, propsChecked: 0 };

  for (const result of results) {
    errors.push(...result.errors);
    warnings.push(...result.warnings);
    stats.nodesChecked += result.stats.nodesChecked;
    stats.edgesChecked += result.stats.edgesChecked;
    stats.propsChecked += result.stats.propsChecked;
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    stats,
  };
}

/**
 * Assert verification result is valid, throwing if not
 */
export function assertValid(result: VerificationResult, message?: string): void {
  if (!result.valid) {
    const prefix = message ? `${message}: ` : "";
    throw new Error(`${prefix}Verification failed with ${result.errors.length} errors:\n${result.errors.join("\n")}`);
  }
}
