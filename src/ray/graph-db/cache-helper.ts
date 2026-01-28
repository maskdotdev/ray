import type { GraphDB } from "../../types.js";
import { CacheManager } from "../../cache/index.js";

/**
 * Get cache manager from database
 */
export function getCache(db: GraphDB): CacheManager | null {
  return (db._cache as CacheManager | undefined) || null;
}

