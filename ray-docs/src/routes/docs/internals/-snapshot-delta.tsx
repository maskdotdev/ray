import DocPage from "~/components/doc-page";
import { CheckpointStep } from "./-components";

// ============================================================================
// SNAPSHOT-DELTA SPECIFIC COMPONENTS
// ============================================================================

// Snapshot + Delta model diagram
function SnapshotDeltaModel() {
	return (
		<div class="my-8">
			{/* Outer container with title */}
			<div class="rounded-2xl border border-slate-600/50 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-xl">
				<h4 class="text-center text-lg font-semibold text-slate-300 mb-6">
					Database State
				</h4>

				{/* Three boxes in a row */}
				<div class="flex flex-col sm:flex-row items-stretch gap-4">
					{/* Snapshot */}
					<div class="flex-1 rounded-xl border border-cyan-500/40 bg-slate-800/50 p-4 relative">
						<div class="absolute -top-px left-4 right-4 h-px bg-gradient-to-r from-transparent via-cyan-400/50 to-transparent" />
						<div class="flex items-center gap-2 mb-3">
							<svg
								class="w-5 h-5 text-cyan-400"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
								/>
							</svg>
							<h5 class="font-bold text-cyan-400">Snapshot</h5>
						</div>
						<p class="text-xs text-slate-400 mb-2">(disk)</p>
						<ul class="space-y-1 text-sm text-slate-300">
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-cyan-400/60" />
								Immutable
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-cyan-400/60" />
								CSR format
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-cyan-400/60" />
								Zero-copy
							</li>
						</ul>
					</div>

					{/* Plus sign */}
					<div class="hidden sm:flex items-center justify-center text-2xl text-slate-500 font-light">
						+
					</div>
					<div class="sm:hidden flex justify-center text-2xl text-slate-500 font-light">
						+
					</div>

					{/* Delta */}
					<div class="flex-1 rounded-xl border border-violet-500/40 bg-slate-800/50 p-4 relative">
						<div class="absolute -top-px left-4 right-4 h-px bg-gradient-to-r from-transparent via-violet-400/50 to-transparent" />
						<div class="flex items-center gap-2 mb-3">
							<svg
								class="w-5 h-5 text-violet-400"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
								/>
							</svg>
							<h5 class="font-bold text-violet-400">Delta</h5>
						</div>
						<p class="text-xs text-slate-400 mb-2">(memory)</p>
						<ul class="space-y-1 text-sm text-slate-300">
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-violet-400/60" />
								Pending changes
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-violet-400/60" />
								Fast writes
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-violet-400/60" />
								Merged on read
							</li>
						</ul>
					</div>

					{/* Arrow */}
					<div class="hidden sm:flex items-center justify-center text-slate-500">
						<svg
							class="w-6 h-6"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M14 5l7 7m0 0l-7 7m7-7H3"
							/>
						</svg>
					</div>
					<div class="sm:hidden flex justify-center text-slate-500">
						<svg
							class="w-6 h-6"
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

					{/* WAL */}
					<div class="flex-1 rounded-xl border border-emerald-500/40 bg-slate-800/50 p-4 relative">
						<div class="absolute -top-px left-4 right-4 h-px bg-gradient-to-r from-transparent via-emerald-400/50 to-transparent" />
						<div class="flex items-center gap-2 mb-3">
							<svg
								class="w-5 h-5 text-emerald-400"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
								/>
							</svg>
							<h5 class="font-bold text-emerald-400">WAL</h5>
						</div>
						<p class="text-xs text-slate-400 mb-2">(durability)</p>
						<ul class="space-y-1 text-sm text-slate-300">
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-emerald-400/60" />
								Recovery log
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-emerald-400/60" />
								Crash safety
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-emerald-400/60" />
								Write-ahead
							</li>
						</ul>
					</div>
				</div>
			</div>
		</div>
	);
}

// Delta State structure visualization
function DeltaStateStructure() {
	return (
		<div class="my-6 rounded-xl border border-violet-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
				<svg
					class="w-5 h-5 text-violet-400"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"
					/>
				</svg>
				<h4 class="font-semibold text-violet-400">Delta State</h4>
			</div>
			<div class="space-y-2">
				<DeltaRow
					connector="├"
					name="createdNodes"
					type="Map<NodeID, NodeData>"
					desc="New nodes"
					color="emerald"
				/>
				<DeltaRow
					connector="├"
					name="deletedNodes"
					type="Set<NodeID>"
					desc="Tombstones"
					color="red"
				/>
				<DeltaRow
					connector="├"
					name="modifiedNodes"
					type="Map<NodeID, PropChanges>"
					desc="Property updates"
					color="amber"
				/>
				<DeltaRow
					connector="├"
					name="outAdd/outDel"
					type="Map<NodeID, EdgePatch[]>"
					desc="Edge changes"
					color="violet"
				/>
				<DeltaRow
					connector="├"
					name="inAdd/inDel"
					type="Map<NodeID, EdgePatch[]>"
					desc="Reverse index"
					color="violet"
				/>
				<DeltaRow
					connector="└"
					name="keyIndex"
					type="Map<string, NodeID>"
					desc="Key lookups"
					color="cyan"
				/>
			</div>
		</div>
	);
}

function DeltaRow(props: {
	connector: string;
	name: string;
	type: string;
	desc: string;
	color: string;
}) {
	const colorClass = () => {
		switch (props.color) {
			case "emerald":
				return "text-emerald-400";
			case "red":
				return "text-red-400";
			case "amber":
				return "text-amber-400";
			case "violet":
				return "text-violet-400";
			default:
				return "text-cyan-400";
		}
	};
	return (
		<div class="flex items-center gap-3 text-sm">
			<span class="text-slate-600">{props.connector}</span>
			<code class={`font-mono ${colorClass()}`}>{props.name}</code>
			<span class="text-slate-500 text-xs hidden sm:inline">{props.type}</span>
			<span class="text-slate-400 ml-auto text-xs">{props.desc}</span>
		</div>
	);
}

// Read decision flow visualization
function ReadFlowDiagram() {
	return (
		<div class="my-6 space-y-3">
			{/* Step 1 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0">
						1
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Is{" "}
							<code class="text-red-400 text-xs px-1 bg-slate-700/50 rounded">
								nodeId
							</code>{" "}
							in{" "}
							<code class="text-violet-400 text-xs px-1 bg-slate-700/50 rounded">
								delta.deletedNodes
							</code>
							?
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-emerald-400">→ Yes:</span>
							<span class="text-slate-400">return null (deleted)</span>
						</div>
					</div>
				</div>
			</div>

			{/* Step 2 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0">
						2
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Is{" "}
							<code class="text-red-400 text-xs px-1 bg-slate-700/50 rounded">
								nodeId
							</code>{" "}
							in{" "}
							<code class="text-violet-400 text-xs px-1 bg-slate-700/50 rounded">
								delta.createdNodes
							</code>
							?
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-emerald-400">→ Yes:</span>
							<span class="text-slate-400">return delta data (new node)</span>
						</div>
					</div>
				</div>
			</div>

			{/* Step 3 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0">
						3
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Does{" "}
							<code class="text-cyan-400 text-xs px-1 bg-slate-700/50 rounded">
								snapshot
							</code>{" "}
							have this node?
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-red-400">→ No:</span>
							<span class="text-slate-400">return null (never existed)</span>
						</div>
					</div>
				</div>
			</div>

			{/* Step 4 - Result */}
			<div class="rounded-xl border border-emerald-500/30 bg-emerald-500/5 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-emerald-500/20 text-emerald-400 text-xs font-bold shrink-0">
						4
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Merge{" "}
							<code class="text-cyan-400 text-xs px-1 bg-slate-700/50 rounded">
								snapshot
							</code>{" "}
							+{" "}
							<code class="text-violet-400 text-xs px-1 bg-slate-700/50 rounded">
								delta.modifiedNodes
							</code>
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-emerald-400">→</span>
							<span class="text-emerald-400 font-medium">
								Return combined result
							</span>
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}

// Write flow visualization
function WriteFlowDiagram() {
	return (
		<div class="my-6 rounded-xl border border-slate-600/50 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="text-sm font-semibold text-slate-400 mb-4">
				Transaction Commit
			</h4>
			<div class="space-y-3">
				{/* WAL */}
				<div class="flex items-center gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-emerald-500/20 text-emerald-400 text-xs font-bold shrink-0">
						1
					</span>
					<div class="flex items-center gap-2 flex-1">
						<span class="font-semibold text-emerald-400 w-16">WAL</span>
						<span class="text-slate-500">→</span>
						<span class="text-slate-300 text-sm">
							Append records (ensures durability)
						</span>
					</div>
				</div>

				{/* Delta */}
				<div class="flex items-center gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-violet-500/20 text-violet-400 text-xs font-bold shrink-0">
						2
					</span>
					<div class="flex items-center gap-2 flex-1">
						<span class="font-semibold text-violet-400 w-16">Delta</span>
						<span class="text-slate-500">→</span>
						<span class="text-slate-300 text-sm">
							Update in-memory state (visible to reads)
						</span>
					</div>
				</div>

				{/* Cache */}
				<div class="flex items-center gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-amber-500/20 text-amber-400 text-xs font-bold shrink-0">
						3
					</span>
					<div class="flex items-center gap-2 flex-1">
						<span class="font-semibold text-amber-400 w-16">Cache</span>
						<span class="text-slate-500">→</span>
						<span class="text-slate-300 text-sm">
							Invalidate affected entries
						</span>
					</div>
				</div>
			</div>

			{/* Note */}
			<div class="mt-4 pt-4 border-t border-slate-700/50">
				<p class="text-xs text-slate-500 flex items-center gap-2">
					<svg
						class="w-4 h-4 text-cyan-500"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
						/>
					</svg>
					The snapshot is NOT touched during normal writes
				</p>
			</div>
		</div>
	);
}

// Checkpoint process visualization
function CheckpointProcess() {
	return (
		<div class="my-6">
			<div class="rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
				<h4 class="text-sm font-semibold text-slate-400 mb-4">
					Checkpoint Process
				</h4>

				{/* Steps */}
				<div class="relative">
					{/* Vertical line */}
					<div class="absolute left-3 top-3 bottom-3 w-px bg-gradient-to-b from-cyan-500/50 via-violet-500/50 to-emerald-500/50" />

					<div class="space-y-3">
						<CheckpointStep num={1} text="Read current snapshot" />
						<CheckpointStep num={2} text="Apply all delta changes" />
						<CheckpointStep
							num={3}
							text="Write new snapshot (CSR, compressed)"
						/>
						<CheckpointStep
							num={4}
							text="Update header to point to new snapshot"
						/>
						<CheckpointStep num={5} text="Clear delta and WAL" />
					</div>
				</div>

				{/* Timing note */}
				<div class="mt-4 pt-4 border-t border-slate-700/50 flex flex-wrap gap-4 text-xs">
					<div class="flex items-center gap-2">
						<span class="w-2 h-2 rounded-full bg-cyan-400" />
						<span class="text-slate-400">Auto: when WAL reaches threshold</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="w-2 h-2 rounded-full bg-violet-400" />
						<span class="text-slate-400">
							Manual: <code class="text-violet-400">db.optimize()</code>
						</span>
					</div>
				</div>
			</div>
		</div>
	);
}

// ============================================================================
// PAGE COMPONENT
// ============================================================================

export function SnapshotDeltaPage() {
	return (
		<DocPage slug="internals/snapshot-delta">
			<p>
				KiteDB separates storage into two parts: a <strong>snapshot</strong>{" "}
				(immutable, on disk) and a <strong>delta</strong> (mutable, in memory).
				This separation is the foundation of how KiteDB achieves fast reads and
				writes.
			</p>

			<h2 id="the-model">The Model</h2>

			<SnapshotDeltaModel />

			<h2 id="snapshot">Snapshot</h2>

			<p>
				The snapshot is a point-in-time image of the entire database. It's
				stored in <a href="/docs/internals/csr">CSR format</a> and memory-mapped
				directly from disk.
			</p>

			<p>
				<strong>Key properties:</strong>
			</p>
			<ul>
				<li>
					<strong>Immutable</strong> – Once written, never modified. Safe for
					concurrent reads.
				</li>
				<li>
					<strong>Zero-copy</strong> – Memory-mapped via <code>mmap()</code>.
					The OS handles caching.
				</li>
				<li>
					<strong>Compressed</strong> – zstd compression reduces disk usage by
					~60%.
				</li>
				<li>
					<strong>Complete</strong> – Contains all nodes, edges, properties, and
					indexes.
				</li>
			</ul>

			<h2 id="delta">Delta</h2>

			<p>
				The delta holds all changes since the last snapshot. It's a collection
				of in-memory data structures optimized for both reads and writes.
			</p>

			<DeltaStateStructure />

			<h2 id="reading">How Reads Work</h2>

			<p>Every read operation merges snapshot and delta:</p>

			<ReadFlowDiagram />

			<p>
				Edge traversals work similarly—scan snapshot edges, skip deleted ones,
				add new ones from delta.
			</p>

			<h2 id="writing">How Writes Work</h2>

			<p>Writes go to three places:</p>

			<WriteFlowDiagram />

			<h2 id="checkpoint">Checkpoint: Merging Delta into Snapshot</h2>

			<p>
				Periodically, KiteDB creates a new snapshot that incorporates all delta
				changes. This is called a <strong>checkpoint</strong>.
			</p>

			<CheckpointProcess />

			<p>
				During checkpoint, reads continue against the old snapshot + delta. The
				switch to the new snapshot is atomic.
			</p>

			<h2 id="why-it-works">Why This Works Well</h2>

			<table>
				<thead>
					<tr>
						<th>Property</th>
						<th>How It's Achieved</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td>Fast reads</td>
						<td>Snapshot is mmap'd. OS caches hot pages. Delta is small.</td>
					</tr>
					<tr>
						<td>Fast writes</td>
						<td>WAL append + memory update. No disk seeks.</td>
					</tr>
					<tr>
						<td>Crash safety</td>
						<td>WAL survives crashes. Replay rebuilds delta.</td>
					</tr>
					<tr>
						<td>Concurrent reads</td>
						<td>Snapshot is immutable. MVCC handles delta visibility.</td>
					</tr>
				</tbody>
			</table>

			<h2 id="next">Next Steps</h2>
			<ul>
				<li>
					<a href="/docs/internals/csr">CSR Format</a> – How the snapshot stores
					edges
				</li>
				<li>
					<a href="/docs/internals/wal">WAL & Durability</a> – How the
					write-ahead log works
				</li>
				<li>
					<a href="/docs/internals/mvcc">MVCC & Transactions</a> – How
					concurrent access is handled
				</li>
			</ul>
		</DocPage>
	);
}
