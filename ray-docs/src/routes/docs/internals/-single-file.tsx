import DocPage from "~/components/doc-page";

// ============================================================================
// SINGLE-FILE SPECIFIC COMPONENTS
// ============================================================================

// File layout diagram
function FileLayoutDiagram() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="space-y-0">
				{/* Header */}
				<div class="rounded-t-lg border-2 border-cyan-500/40 bg-cyan-500/10 p-4">
					<div class="flex items-center justify-between">
						<div class="flex items-center gap-3">
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
									d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
								/>
							</svg>
							<span class="font-semibold text-cyan-400">Header</span>
						</div>
						<span class="text-xs text-cyan-400/70 font-mono">4 KB</span>
					</div>
					<p class="text-xs text-slate-400 mt-1 ml-8">
						Database metadata, pointers, checksums
					</p>
				</div>

				{/* WAL Area */}
				<div class="border-2 border-t-0 border-violet-500/40 bg-violet-500/5 p-4">
					<div class="flex items-center justify-between mb-3">
						<div class="flex items-center gap-3">
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
									d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
								/>
							</svg>
							<span class="font-semibold text-violet-400">WAL Area</span>
						</div>
						<span class="text-xs text-violet-400/70 font-mono">~64 MB</span>
					</div>
					{/* Inner regions */}
					<div class="ml-8 space-y-2">
						<div class="rounded border border-violet-500/30 bg-violet-500/10 px-3 py-2 flex justify-between items-center">
							<span class="text-sm text-violet-300">Primary Region</span>
							<span class="text-xs text-slate-500">75% — normal writes</span>
						</div>
						<div class="rounded border border-violet-500/20 bg-violet-500/5 px-3 py-2 flex justify-between items-center">
							<span class="text-sm text-violet-300/70">Secondary Region</span>
							<span class="text-xs text-slate-500">
								25% — during checkpoint
							</span>
						</div>
					</div>
				</div>

				{/* Snapshot Area */}
				<div class="rounded-b-lg border-2 border-t-0 border-emerald-500/40 bg-emerald-500/5 p-4">
					<div class="flex items-center justify-between">
						<div class="flex items-center gap-3">
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
									d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
								/>
							</svg>
							<span class="font-semibold text-emerald-400">Snapshot Area</span>
						</div>
						<span class="text-xs text-emerald-400/70 font-mono">grows</span>
					</div>
					<p class="text-xs text-slate-400 mt-1 ml-8">
						CSR data, compressed with zstd
					</p>
				</div>
			</div>
		</div>
	);
}

// Header contents visualization
function HeaderContents() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
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
						d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
					/>
				</svg>
				<h4 class="font-semibold text-cyan-400">Header Contents</h4>
				<span class="text-xs text-slate-500 ml-auto">
					offset 0, always 4 KB
				</span>
			</div>
			<div class="space-y-1.5">
				<HeaderRow
					connector="├"
					name="Magic bytes"
					value={`"KITE" + version`}
				/>
				<HeaderRow connector="├" name="Page size" value="4096 (default)" />
				<HeaderRow
					connector="├"
					name="Snapshot location"
					value="Start page, page count"
				/>
				<HeaderRow
					connector="├"
					name="WAL location"
					value="Start page, page count"
				/>
				<HeaderRow
					connector="├"
					name="WAL pointers"
					value="Head and tail positions"
				/>
				<HeaderRow
					connector="├"
					name="Counters"
					value="Max node ID, next tx ID"
				/>
				<HeaderRow
					connector="├"
					name="Snapshot generation"
					value="Incremented on checkpoint"
				/>
				<HeaderRow
					connector="└"
					name="Checksums"
					value="CRC32C of header data"
				/>
			</div>
		</div>
	);
}

function HeaderRow(props: { connector: string; name: string; value: string }) {
	return (
		<div class="flex items-center gap-3 text-sm">
			<span class="text-slate-600 font-mono">{props.connector}</span>
			<span class="text-cyan-400 font-medium w-36">{props.name}</span>
			<span class="text-slate-400">{props.value}</span>
		</div>
	);
}

// Atomic checkpoint process
function AtomicCheckpointProcess() {
	return (
		<div class="my-6 rounded-xl border border-emerald-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="text-sm font-semibold text-slate-400 mb-4">
				Checkpoint Process
			</h4>

			<div class="relative">
				{/* Vertical line */}
				<div class="absolute left-3 top-3 bottom-16 w-px bg-gradient-to-b from-cyan-500/50 via-violet-500/50 to-emerald-500/50" />

				<div class="space-y-3">
					<CheckpointAtomicStep
						num={1}
						text="Write new snapshot to free space at end of file"
					/>
					<CheckpointAtomicStep
						num={2}
						text="fsync() to ensure snapshot is durable"
					/>
					<CheckpointAtomicStep
						num={3}
						text="Update header with new snapshot location"
					/>
					<CheckpointAtomicStep num={4} text="fsync() header" highlight />
					<CheckpointAtomicStep
						num={5}
						text="Old snapshot space becomes free"
					/>
				</div>
			</div>

			{/* Crash recovery note */}
			<div class="mt-5 pt-4 border-t border-slate-700/50 space-y-2">
				<p class="text-xs text-slate-500 font-medium">If crash occurs:</p>
				<div class="grid grid-cols-2 gap-3 text-xs">
					<div class="px-3 py-2 rounded bg-amber-500/10 border border-amber-500/20">
						<span class="text-amber-400">Before step 4:</span>
						<span class="text-slate-400 ml-1">Old snapshot valid</span>
					</div>
					<div class="px-3 py-2 rounded bg-emerald-500/10 border border-emerald-500/20">
						<span class="text-emerald-400">After step 4:</span>
						<span class="text-slate-400 ml-1">New snapshot valid</span>
					</div>
				</div>
				<p class="text-xs text-slate-500 italic">
					No intermediate state is possible.
				</p>
			</div>
		</div>
	);
}

function CheckpointAtomicStep(props: {
	num: number;
	text: string;
	highlight?: boolean;
}) {
	const bgColor = () => {
		if (props.highlight) return "bg-emerald-500/20 text-emerald-400";
		if (props.num <= 2) return "bg-cyan-500/20 text-cyan-400";
		if (props.num <= 4) return "bg-violet-500/20 text-violet-400";
		return "bg-slate-500/20 text-slate-400";
	};
	return (
		<div class="flex items-center gap-3 relative">
			<span
				class={`flex items-center justify-center w-6 h-6 rounded-full text-xs font-bold shrink-0 z-10 ${bgColor()}`}
			>
				{props.num}
			</span>
			<span
				class={`text-sm ${props.highlight ? "text-emerald-300 font-medium" : "text-slate-300"}`}
			>
				{props.text}
			</span>
		</div>
	);
}

// WAL dual region diagram
function WALDualRegion() {
	return (
		<div class="my-6 rounded-xl border border-violet-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center justify-between mb-4">
				<h4 class="font-semibold text-violet-400">WAL Area</h4>
				<span class="text-xs text-slate-500 font-mono">64 MB example</span>
			</div>

			{/* Visual representation */}
			<div class="mb-4">
				<div class="flex rounded-lg overflow-hidden border border-violet-500/30">
					<div class="w-3/4 bg-violet-500/20 p-3 border-r border-violet-500/30">
						<div class="text-sm font-medium text-violet-300">Primary</div>
						<div class="text-xs text-slate-400">48 MB</div>
					</div>
					<div class="w-1/4 bg-violet-500/10 p-3">
						<div class="text-sm font-medium text-violet-300/70">Secondary</div>
						<div class="text-xs text-slate-400">16 MB</div>
					</div>
				</div>
			</div>

			{/* Explanation */}
			<div class="space-y-2 text-sm">
				<p class="text-slate-400">
					<span class="text-violet-400 font-medium">Why two regions?</span>
				</p>
				<ul class="space-y-1 text-slate-400 ml-4">
					<li class="flex items-start gap-2">
						<span class="text-violet-400 mt-1">•</span>
						<span>Checkpoint reads primary to build new snapshot</span>
					</li>
					<li class="flex items-start gap-2">
						<span class="text-violet-400 mt-1">•</span>
						<span>Concurrent transactions write to secondary</span>
					</li>
					<li class="flex items-start gap-2">
						<span class="text-emerald-400 mt-1">→</span>
						<span class="text-emerald-400">
							No blocking between reads and writes
						</span>
					</li>
				</ul>
			</div>
		</div>
	);
}

// Snapshot sections tree
function SnapshotSections() {
	return (
		<div class="my-6 rounded-xl border border-emerald-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
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
						d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
					/>
				</svg>
				<h4 class="font-semibold text-emerald-400">Snapshot Sections</h4>
			</div>
			<div class="space-y-1.5">
				<SnapshotRow
					connector="├"
					name="Node ID mappings"
					desc="Physical ↔ Logical ID translation"
				/>
				<SnapshotRow
					connector="├"
					name="Out-edge CSR"
					desc="offsets[], destinations[], edge_types[]"
					code
				/>
				<SnapshotRow
					connector="├"
					name="In-edge CSR"
					desc="offsets[], sources[], edge_types[]"
					code
				/>
				<SnapshotRow
					connector="├"
					name="Properties"
					desc="Node and edge property values"
				/>
				<SnapshotRow
					connector="├"
					name="String table"
					desc="Deduplicated string storage"
				/>
				<SnapshotRow
					connector="├"
					name="Key index"
					desc="Hash-bucketed node key lookups"
				/>
				<SnapshotRow
					connector="└"
					name="Schema"
					desc="Labels, edge types, property keys"
				/>
			</div>
			<div class="mt-4 pt-3 border-t border-slate-700/50 text-xs text-slate-500">
				Each section independently compressed (zstd). Typical ratio: 40-60% of
				raw size.
			</div>
		</div>
	);
}

function SnapshotRow(props: {
	connector: string;
	name: string;
	desc: string;
	code?: boolean;
}) {
	return (
		<div class="flex items-center gap-3 text-sm">
			<span class="text-slate-600 font-mono">{props.connector}</span>
			<span class="text-emerald-400 font-medium w-32">{props.name}</span>
			<span
				class={
					props.code ? "text-slate-500 font-mono text-xs" : "text-slate-400"
				}
			>
				{props.desc}
			</span>
		</div>
	);
}

// File growth visualization
function FileGrowthDiagram() {
	return (
		<div class="my-6 rounded-xl border border-slate-600/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="text-sm font-semibold text-slate-400 mb-4">
				File Size Examples
			</h4>

			<div class="space-y-4">
				{/* Initial */}
				<div class="flex items-center gap-4">
					<div class="w-20 text-xs text-slate-500">Initial</div>
					<div class="flex-1 flex items-center gap-1">
						<div class="h-6 w-1 bg-cyan-500/60 rounded" title="Header 4KB" />
						<div class="h-6 flex-1 bg-violet-500/40 rounded" title="WAL 64MB" />
						<div
							class="h-6 w-1 bg-emerald-500/40 rounded"
							title="Empty snapshot"
						/>
					</div>
					<div class="w-20 text-right text-xs text-slate-400 font-mono">
						~64 MB
					</div>
				</div>

				{/* 100K nodes */}
				<div class="flex items-center gap-4">
					<div class="w-20 text-xs text-slate-500">100K nodes</div>
					<div class="flex-1 flex items-center gap-1">
						<div class="h-6 w-1 bg-cyan-500/60 rounded" />
						<div class="h-6 w-3/4 bg-violet-500/40 rounded" />
						<div class="h-6 w-1/5 bg-emerald-500/60 rounded" />
					</div>
					<div class="w-20 text-right text-xs text-slate-400 font-mono">
						~72 MB
					</div>
				</div>

				{/* 1M nodes */}
				<div class="flex items-center gap-4">
					<div class="w-20 text-xs text-slate-500">1M nodes</div>
					<div class="flex-1 flex items-center gap-1">
						<div class="h-6 w-1 bg-cyan-500/60 rounded" />
						<div class="h-6 w-1/2 bg-violet-500/40 rounded" />
						<div class="h-6 w-2/5 bg-emerald-500/60 rounded" />
					</div>
					<div class="w-20 text-right text-xs text-slate-400 font-mono">
						~150 MB
					</div>
				</div>
			</div>

			{/* Legend */}
			<div class="mt-4 pt-3 border-t border-slate-700/50 flex flex-wrap gap-4 text-xs">
				<div class="flex items-center gap-1.5">
					<div class="w-2 h-2 rounded-sm bg-cyan-500/60" />
					<span class="text-slate-500">Header (fixed)</span>
				</div>
				<div class="flex items-center gap-1.5">
					<div class="w-2 h-2 rounded-sm bg-violet-500/40" />
					<span class="text-slate-500">WAL (configurable)</span>
				</div>
				<div class="flex items-center gap-1.5">
					<div class="w-2 h-2 rounded-sm bg-emerald-500/60" />
					<span class="text-slate-500">Snapshot (grows)</span>
				</div>
			</div>
		</div>
	);
}

// Database open process
function DatabaseOpenProcess() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="text-sm font-semibold text-slate-400 mb-4">
				Opening a Database
			</h4>

			<div class="relative">
				{/* Vertical line */}
				<div class="absolute left-3 top-3 bottom-12 w-px bg-gradient-to-b from-cyan-500/50 via-violet-500/50 to-emerald-500/50" />

				<div class="space-y-2.5">
					<OpenStep num={1} text="Read header (4 KB at offset 0)" />
					<OpenStep num={2} text="Validate magic bytes and checksums" />
					<OpenStep num={3} text="mmap() snapshot area (zero-copy)" />
					<OpenStep num={4} text="Parse snapshot sections" />
					<OpenStep num={5} text="Replay WAL to rebuild delta" />
					<OpenStep num={6} text="Ready for queries" success />
				</div>
			</div>

			{/* Recovery note */}
			<div class="mt-4 pt-3 border-t border-slate-700/50">
				<p class="text-xs text-slate-500">
					<span class="text-amber-400">
						If WAL replay finds incomplete transaction:
					</span>
					<span class="text-slate-400 ml-1">
						Discard it (never committed). Recovery is automatic and fast.
					</span>
				</p>
			</div>
		</div>
	);
}

function OpenStep(props: { num: number; text: string; success?: boolean }) {
	const bgColor = () => {
		if (props.success) return "bg-emerald-500/20 text-emerald-400";
		if (props.num <= 2) return "bg-cyan-500/20 text-cyan-400";
		if (props.num <= 4) return "bg-violet-500/20 text-violet-400";
		return "bg-slate-500/20 text-slate-400";
	};
	return (
		<div class="flex items-center gap-3 relative">
			<span
				class={`flex items-center justify-center w-6 h-6 rounded-full text-xs font-bold shrink-0 z-10 ${bgColor()}`}
			>
				{props.success ? "✓" : props.num}
			</span>
			<span
				class={`text-sm ${props.success ? "text-emerald-400 font-medium" : "text-slate-300"}`}
			>
				{props.text}
			</span>
		</div>
	);
}

// ============================================================================
// PAGE COMPONENT
// ============================================================================

export function SingleFilePage() {
	return (
		<DocPage slug="internals/single-file">
			<p>
				KiteDB stores everything in a single <code>.kitedb</code> file. This
				makes databases portable, simplifies deployment, and enables atomic
				operations.
			</p>

			<h2 id="file-layout">File Layout</h2>

			<FileLayoutDiagram />

			<h2 id="header">The Header</h2>

			<p>
				The header is 4 KB and contains all metadata needed to open the
				database:
			</p>

			<HeaderContents />

			<h2 id="atomicity">Atomic Updates</h2>

			<p>
				The header enables atomic state transitions. A checkpoint works like
				this:
			</p>

			<AtomicCheckpointProcess />

			<h2 id="wal-area">WAL Area</h2>

			<p>The WAL area is a circular buffer divided into two regions:</p>

			<WALDualRegion />

			<h2 id="snapshot-area">Snapshot Area</h2>

			<p>The snapshot area holds the CSR-formatted graph data:</p>

			<SnapshotSections />

			<h2 id="growth">File Growth</h2>

			<p>The file grows in predictable ways:</p>

			<FileGrowthDiagram />

			<h2 id="vs-directory">Single-File vs Multi-File</h2>

			<p>
				KiteDB previously supported a directory-based format. Single-file is now
				the default:
			</p>

			<table>
				<thead>
					<tr>
						<th>Aspect</th>
						<th>Single-File</th>
						<th>Directory (legacy)</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td>Portability</td>
						<td>Copy one file</td>
						<td>Copy entire directory</td>
					</tr>
					<tr>
						<td>Atomic ops</td>
						<td>Header flip</td>
						<td>Manifest + renames</td>
					</tr>
					<tr>
						<td>Disk usage</td>
						<td>~40% smaller</td>
						<td>More overhead</td>
					</tr>
					<tr>
						<td>Complexity</td>
						<td>Simpler</td>
						<td>More moving parts</td>
					</tr>
				</tbody>
			</table>

			<h2 id="opening">Opening a Database</h2>

			<DatabaseOpenProcess />

			<h2 id="next">Next Steps</h2>
			<ul>
				<li>
					<a href="/docs/internals/wal">WAL & Durability</a> – How the
					write-ahead log provides crash safety
				</li>
				<li>
					<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – How
					reads merge these two sources
				</li>
			</ul>
		</DocPage>
	);
}
