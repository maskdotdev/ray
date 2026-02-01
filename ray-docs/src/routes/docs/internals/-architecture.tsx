import CodeBlock from "~/components/code-block";
import DocPage from "~/components/doc-page";
import { Code, FlowArrow, FlowItem, FlowStep, Label } from "./-components";

// ============================================================================
// ARCHITECTURE-SPECIFIC COMPONENTS
// ============================================================================

// Insert data flow diagram
function InsertDataFlow() {
	return (
		<div class="my-6 space-y-3">
			<FlowStep number="1" title="Query Layer" color="cyan">
				<FlowItem color="cyan">
					Validates schema <Code color="cyan">user</Code> has required
					properties
				</FlowItem>
				<FlowItem color="cyan">
					Converts <Code color="cyan">age: 30</Code> → internal I64 type
				</FlowItem>
				<FlowItem color="cyan" isLast>
					Calls graph layer: <Code color="cyan">createNode(...)</Code>
				</FlowItem>
			</FlowStep>

			<FlowArrow />

			<FlowStep number="2" title="Graph Layer" color="violet">
				<FlowItem color="violet">
					Begins transaction (if not already in one)
				</FlowItem>
				<FlowItem color="violet">
					Allocates new <Code color="violet">NodeID</Code> (monotonic counter)
				</FlowItem>
				<FlowItem color="violet">Records in transaction state</FlowItem>
				<FlowItem color="violet" isLast>
					On commit → writes to WAL and Delta
				</FlowItem>
			</FlowStep>

			<FlowArrow />

			<FlowStep number="3" title="Storage Layer" color="emerald">
				<FlowItem color="emerald">
					<Label color="emerald">WAL</Label>: Appends CREATE_NODE record
					(durability)
				</FlowItem>
				<FlowItem color="emerald">
					<Label color="emerald">Delta</Label>: Adds node to{" "}
					<Code color="emerald">createdNodes</Code> map
				</FlowItem>
				<FlowItem color="emerald" isLast>
					<Label color="emerald">Later</Label>: Checkpoint merges into snapshot
				</FlowItem>
			</FlowStep>
		</div>
	);
}

// Read data flow diagram
function ReadDataFlow() {
	return (
		<div class="my-6 space-y-3">
			<FlowStep number="1" title="Key Index Lookup" color="cyan">
				<FlowItem color="cyan">
					Check <Code color="cyan">delta.keyIndex</Code> (recent changes)
				</FlowItem>
				<FlowItem color="cyan">
					If not found → check snapshot's hash-bucketed index
				</FlowItem>
				<FlowItem color="cyan" isLast>
					Returns <Code color="cyan">NodeID</Code>
				</FlowItem>
			</FlowStep>

			<FlowArrow />

			<FlowStep number="2" title="Property Fetch" color="violet">
				<FlowItem color="violet">
					Check <Code color="violet">delta.modifiedNodes</Code> for changes
				</FlowItem>
				<FlowItem color="violet">
					Fall back to snapshot for unchanged properties
				</FlowItem>
				<FlowItem color="violet" isLast>
					Merge and return combined result
				</FlowItem>
			</FlowStep>

			{/* Result badge */}
			<div class="flex justify-center pt-2">
				<div class="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 text-sm">
					<svg
						class="w-4 h-4"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							d="M5 13l4 4L19 7"
						/>
					</svg>
					<span>Returns latest committed data</span>
				</div>
			</div>
		</div>
	);
}

// Architecture diagram component with thematic styling
function ArchitectureDiagram() {
	return (
		<div class="my-8 space-y-3">
			{/* Query Layer */}
			<div class="relative rounded-2xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-lg shadow-cyan-500/10">
				<div class="absolute -top-px left-8 right-8 h-px bg-gradient-to-r from-transparent via-cyan-400 to-transparent" />
				<h3 class="text-xl font-bold text-cyan-400 mb-3">Query Layer</h3>
				<div class="flex items-start gap-4">
					<div class="flex-1">
						<div class="flex items-center gap-2 text-slate-300 mb-2">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M13 10V3L4 14h7v7l9-11h-7z"
								/>
							</svg>
							<span>Fluent API, type inference, schema validation</span>
						</div>
						<code class="text-sm text-cyan-300/80 font-mono">
							db.insert(user).values({"{...}"})
						</code>
					</div>
					<div class="hidden sm:flex items-center gap-1 text-cyan-400/60">
						<svg
							class="w-5 h-5 fill-none stroke-current"
							viewBox="0 0 24 24"
							stroke-width="2"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M19 14l-7 7m0 0l-7-7m7 7V3"
							/>
						</svg>
					</div>
				</div>
			</div>

			{/* Arrow connector */}
			<div class="flex justify-center">
				<svg
					class="w-6 h-6 text-cyan-400/50"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M19 14l-7 7m0 0l-7-7m7 7V3"
					/>
				</svg>
			</div>

			{/* Graph Layer */}
			<div class="relative rounded-2xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-lg shadow-cyan-500/10">
				<h3 class="text-xl font-bold text-cyan-400 mb-3">Graph Layer</h3>
				<div class="flex items-start gap-4">
					<div class="flex-1">
						<div class="flex items-center gap-2 text-slate-300 mb-2">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
								/>
							</svg>
							<span>Nodes, edges, traversal, transactions</span>
						</div>
						<code class="text-sm text-cyan-300/80 font-mono">
							createNode(), addEdge(), getNeighborsOut()
						</code>
					</div>
					{/* Mini graph visualization - using CSS circles */}
					<div class="hidden sm:flex items-center gap-1">
						<div class="flex flex-col items-center gap-1">
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
							<div class="w-px h-3 bg-cyan-400/40" />
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
						</div>
						<div class="flex flex-col gap-1">
							<div class="w-6 h-px bg-cyan-400/40" />
							<div class="w-6 h-px bg-cyan-400/40" />
						</div>
						<div class="flex flex-col items-center gap-1">
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
							<div class="w-px h-3 bg-cyan-400/40" />
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
						</div>
					</div>
				</div>
			</div>

			{/* Arrow connector */}
			<div class="flex justify-center">
				<svg
					class="w-6 h-6 text-cyan-400/50"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M19 14l-7 7m0 0l-7-7m7 7V3"
					/>
				</svg>
			</div>

			{/* Storage Layer */}
			<div class="relative rounded-2xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-lg shadow-cyan-500/10">
				<div class="absolute -bottom-px left-8 right-8 h-px bg-gradient-to-r from-transparent via-cyan-400 to-transparent" />
				<h3 class="text-xl font-bold text-cyan-400 mb-3">Storage Layer</h3>
				<div class="flex items-start gap-4">
					<div class="flex-1">
						<div class="flex items-center gap-2 text-slate-300 mb-2">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
								/>
							</svg>
							<span>Snapshot (CSR), Delta, WAL, Key Index</span>
						</div>
						<div class="flex items-center gap-2 text-slate-300">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
								/>
							</svg>
							<span>Memory-mapped files, crash recovery</span>
						</div>
					</div>
					{/* Storage icons */}
					<div class="hidden sm:flex gap-3 text-cyan-400/60">
						<svg
							class="w-8 h-8 fill-none stroke-current"
							viewBox="0 0 24 24"
							stroke-width="1.5"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
							/>
						</svg>
						<svg
							class="w-8 h-8 fill-none stroke-current"
							viewBox="0 0 24 24"
							stroke-width="1.5"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
							/>
						</svg>
					</div>
				</div>
			</div>
		</div>
	);
}

// ============================================================================
// PAGE COMPONENT
// ============================================================================

export function ArchitecturePage() {
	return (
		<DocPage slug="internals/architecture">
			<p>
				KiteDB is built as a layered system. Each layer has a specific job, and
				they work together to provide fast, reliable graph storage.
			</p>

			<h2 id="the-layers">The Three Layers</h2>

			<ArchitectureDiagram />

			<h3>Query Layer</h3>
			<p>
				This is what you interact with. It provides the Drizzle-style API with
				full TypeScript type inference. When you write{" "}
				<code>db.insert(user).values(...)</code>, the query layer validates your
				schema, converts TypeScript types to storage types, and calls into the
				graph layer.
			</p>

			<h3>Graph Layer</h3>
			<p>
				Manages the graph abstraction: nodes with properties, edges between
				nodes, and traversals. Handles transaction boundaries and coordinates
				reads between the snapshot and delta.
			</p>

			<h3>Storage Layer</h3>
			<p>
				The foundation. Stores data in a format optimized for graph operations.
				The key insight here is the <strong>Snapshot + Delta</strong> model,
				which separates immutable historical data from pending changes.
			</p>

			<h2 id="data-flow">What Happens When You Insert a Node</h2>

			<p>Let's trace through a simple insert:</p>

			<CodeBlock
				code={`await db.insert(user).values({ key: 'alice', name: 'Alice', age: 30 });`}
				language="typescript"
			/>

			<InsertDataFlow />

			<h2 id="read-path">What Happens When You Read</h2>

			<p>Reads merge data from two sources:</p>

			<CodeBlock
				code={`const alice = await db.get(user, 'alice');`}
				language="typescript"
			/>

			<ReadDataFlow />

			<h2 id="why-this-design">Why This Design</h2>

			<table>
				<thead>
					<tr>
						<th>Design Choice</th>
						<th>Benefit</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td>Snapshot + Delta</td>
						<td>
							Reads don't block writes. Snapshot is immutable, delta is small.
						</td>
					</tr>
					<tr>
						<td>CSR format for edges</td>
						<td>Traversals read contiguous memory. CPU cache loves this.</td>
					</tr>
					<tr>
						<td>WAL for durability</td>
						<td>Committed data survives crashes. Recovery is fast.</td>
					</tr>
					<tr>
						<td>Single file</td>
						<td>Portable, atomic operations, simpler deployment.</td>
					</tr>
					<tr>
						<td>Memory-mapped I/O</td>
						<td>OS handles caching. Zero-copy reads.</td>
					</tr>
				</tbody>
			</table>

			<h2 id="next">Next Steps</h2>
			<ul>
				<li>
					<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – The
					core storage model in detail
				</li>
				<li>
					<a href="/docs/internals/csr">CSR Format</a> – How edges are stored
				</li>
				<li>
					<a href="/docs/internals/single-file">Single-File Format</a> – The
					.kitedb file layout
				</li>
			</ul>
		</DocPage>
	);
}
