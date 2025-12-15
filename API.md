# Ray API Documentation

This document provides a high-level overview of Ray's architecture and API layers.

## Architecture Overview

Ray is organized into several key layers:

```
┌─────────────────────────────────────────┐
│  Application Code                       │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│  High-Level API (src/api/)              │
│  - Type-safe schema definitions         │
│  - Fluent query builders                │
│  - Graph traversal                      │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│  Core Database (src/db/)                │
│  - Low-level CRUD operations            │
│  - Transaction management               │
│  - Node/edge IDs                        │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│  Storage Layer (src/core/)              │
│  - WAL (Write-Ahead Log)                │
│  - Snapshots (CSR format)               │
│  - Compaction                           │
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│  Utilities (src/util/)                  │
│  - Binary encoding                      │
│  - Compression                          │
│  - CRC checksums                        │
│  - File locking                         │
└─────────────────────────────────────────┘
```

## API Layers

### 1. High-Level API (`src/api/`)

**For application developers** - Recommended for most use cases.

Features:
- Type-safe schema definitions (`defineNode`, `defineEdge`)
- Fluent query builders (insert, update, delete)
- Graph traversal with filtering
- Automatic type inference
- Transaction support
- Property type validation

**Modules:**
- `ray.ts` - Main database context
- `schema.ts` - Schema builders
- `builders.ts` - Query builders
- `traversal.ts` - Graph traversal
- `index.ts` - Public exports

**Example:**
```typescript
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  props: { name: prop.string('name') },
});

const db = await ray('./db', { nodes: [user], edges: [] });
const alice = await db.insert(user).values({ key: 'alice', name: 'Alice' }).returning();
```

### 2. Low-Level API (`src/db/`)

**For advanced users and framework builders** - Direct database access.

Provides:
- `GraphDB` - Raw database handle
- Node/edge CRUD with numeric IDs
- Transaction primitives (`beginTx`, `commit`, `rollback`)
- Property access (get/set)
- Edge queries and traversal
- Database maintenance

**Key types:**
- `NodeID` - Numeric node identifier (bigint)
- `ETypeID` - Edge type identifier
- `PropKeyID` - Property key identifier
- `TxHandle` - Transaction handle

**Example:**
```typescript
const db = await openGraphDB('./db');
const tx = beginTx(db);

const alice = createNode(tx, { key: 'user:alice' });
const bob = createNode(tx, { key: 'user:bob' });

const knows = defineEtype(tx, 'knows');
addEdge(tx, alice, knows, bob);

await commit(tx);
```

### 3. Core Storage (`src/core/`)

**Internal** - Handles persistence and optimization.

Components:
- `wal.ts` - Write-Ahead Log for durability
- `snapshot-reader.ts` / `snapshot-writer.ts` - CSR snapshot format
- `compactor.ts` - Merges deltas into new snapshots
- `delta.ts` - In-memory delta overlay
- `manifest.ts` - Database metadata

## File Structure

```
src/
├── api/                    # High-level API
│   ├── README.md          # API documentation
│   ├── ray.ts             # Main database context
│   ├── schema.ts          # Schema definitions
│   ├── builders.ts        # Query builders
│   ├── traversal.ts       # Graph traversal
│   └── index.ts           # Exports
│
├── db/                    # Low-level database
│   ├── graph-db.ts        # Core operations
│   ├── iterators.ts       # Edge traversal
│   ├── key-index.ts       # Key lookup
│   ├── types.ts           # Type definitions
│   └── index.ts           # Exports
│
├── core/                  # Storage layer
│   ├── wal.ts             # Write-ahead log
│   ├── snapshot-reader.ts # Snapshot reading
│   ├── snapshot-writer.ts # Snapshot writing
│   ├── compactor.ts       # Compaction
│   ├── delta.ts           # Delta overlay
│   ├── manifest.ts        # Metadata
│   └── index.ts           # Exports
│
├── util/                  # Utilities
│   ├── compression.ts     # Compression
│   ├── binary.ts          # Binary encoding
│   ├── crc.ts             # Checksums
│   ├── hash.ts            # Hashing
│   ├── lock.ts            # File locks
│   └── index.ts           # Exports
│
├── check/                 # Verification
│   ├── checker.ts         # Integrity checking
│   └── constants.ts       # Error codes
│
├── api.ts                 # (Legacy?) API exports
├── ray.ts                 # (Legacy?) Main export
├── index.ts               # Main entry point
├── schema.ts              # (Legacy?) Schema
├── types.ts               # Type definitions
└── traversal.ts           # (Legacy?) Traversal
```

## Choosing the Right API

### Use High-Level API (`src/api/`)

✅ You're building an application
✅ You want type safety and ergonomics
✅ You want automatic property type handling
✅ You want traversal with filtering
✅ You want comfortable error handling

```typescript
import { ray, defineNode, defineEdge, prop } from './src/api';
```

### Use Low-Level API (`src/db/`)

✅ You're building a framework or tool
✅ You need maximum control
✅ You want to work with numeric IDs directly
✅ You're implementing custom traversal logic
✅ You need escape-hatch access to raw operations

```typescript
import { openGraphDB, createNode, addEdge } from './src/db/graph-db';
```

### Use Raw Database via Escape Hatch

```typescript
const raw: GraphDB = db.$raw;
// Now you can use low-level APIs directly
```

## Common Patterns

### Define a Schema

```typescript
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  props: {
    name: prop.string('name'),
    email: prop.string('email'),
    created: prop.int('created'),
  },
});

const knows = defineEdge('knows', {
  since: prop.int('since'),
  confidence: optional(prop.float('confidence')),
});
```

### CRUD Operations

```typescript
// Create
const alice = await db
  .insert(user)
  .values({ key: 'alice', name: 'Alice', email: 'alice@example.com', created: Date.now() })
  .returning();

// Read
const retrieved = await db.get(user, 'alice');

// Update
await db
  .update(alice)
  .set({ name: 'Alice Updated' })
  .execute();

// Delete
const success = await db.delete(alice);
```

### Relationships

```typescript
const alice = await db.get(user, 'alice');
const bob = await db.get(user, 'bob');

// Link
await db.link(alice, knows, bob, { since: 2020 });

// Query
const friends = await db
  .from(alice)
  .out(knows)
  .nodes()
  .toArray();

// Unlink
await db.unlink(alice, knows, bob);
```

### Transactions

```typescript
await db.transaction(async (ctx) => {
  const alice = await ctx
    .insert(user)
    .values({ key: 'alice', name: 'Alice', email: '...' })
    .returning();

  const bob = await ctx
    .insert(user)
    .values({ key: 'bob', name: 'Bob', email: '...' })
    .returning();

  await ctx.link(alice, knows, bob);
});
// All committed or all rolled back
```

## Type Inference

The API uses TypeScript's advanced type system for full inference:

```typescript
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  props: {
    name: prop.string('name'),
    age: optional(prop.int('age')),
  },
});

// Inferred insert type
type InsertUser = InferNodeInsert<typeof user>;
// { key: string; name: string; age?: bigint; }

// Inferred return type
type User = InferNode<typeof user>;
// { $id: bigint; $key: string; name: string; age?: bigint; }

// Inferred edge props
const knows = defineEdge('knows', { since: prop.int('since') });
type KnowsProps = InferEdgeProps<typeof knows>;
// { since: bigint; }
```

## Property Types

| Type | TypeScript | Storage | Notes |
|------|-----------|---------|-------|
| `prop.string()` | `string` | UTF-8 | Interned strings |
| `prop.int()` | `bigint` | i64 | 64-bit signed |
| `prop.float()` | `number` | f64 | IEEE 754 |
| `prop.bool()` | `boolean` | bool | True/false |

Optional properties can be omitted or set to `undefined`.

## Performance Characteristics

- **Node creation**: O(1)
- **Edge creation**: O(log n) with CSR compaction
- **Key lookup**: O(1) average with hash index
- **Edge existence**: O(log n) binary search on CSR
- **Traversal**: O(k) where k = number of edges
- **Snapshot read**: Zero-copy mmap

## File Formats

Database files are stored in a directory:

```
db/
  manifest.gdm              # Metadata (JSON)
  lock.gdl                  # Optional file lock
  snapshots/
    snap_0000000000000001.gds
    snap_0000000000000002.gds
  wal/
    wal_0000000000000001.gdw
    wal_0000000000000002.gdw
```

### Snapshot Format (`.gds`)

- Magic: `GDS1`
- CSR (Compressed Sparse Row) format for edges
- Separate in-edge and out-edge indexes
- String table for interned strings
- Key index for fast lookups
- CRC32C integrity check

### WAL Format (`.gdw`)

- Magic: `GDW1`
- 8-byte aligned records
- CRC32C per record
- Transaction boundaries

## Getting Started

See `src/api/README.md` for detailed API documentation and examples.

## References

- [Ray Main README](./README.md)
- [High-Level API Docs](./src/api/README.md)
- [TypeScript Docs](./tsconfig.json)
