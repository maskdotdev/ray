import DocPage from "~/components/doc-page";

// ============================================================================
// KEY-INDEX SPECIFIC COMPONENTS
// ============================================================================

// Problem comparison
function KeyIndexProblem() {
	return (
		<div class="my-6 grid sm:grid-cols-2 gap-4">
			{/* Without index */}
			<div class="rounded-xl border border-red-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-4 shadow-lg">
				<div class="flex items-center gap-2 mb-3">
					<svg
						class="w-5 h-5 text-red-400"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
						/>
					</svg>
					<span class="font-semibold text-red-400">Without Index</span>
				</div>
				<p class="text-sm text-slate-400 mb-2">"Find user:alice"</p>
				<p class="text-sm text-slate-300">
					Scan all nodes → <span class="text-red-400 font-mono">O(n)</span>
				</p>
				<div class="mt-3 pt-3 border-t border-slate-700/50">
					<p class="text-xs text-slate-500">
						1M nodes × 1μs ={" "}
						<span class="text-red-400 font-medium">1 second</span>
					</p>
				</div>
			</div>

			{/* With index */}
			<div class="rounded-xl border border-emerald-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-4 shadow-lg">
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
							d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
						/>
					</svg>
					<span class="font-semibold text-emerald-400">With Hash Index</span>
				</div>
				<p class="text-sm text-slate-400 mb-2">"Find user:alice"</p>
				<p class="text-sm text-slate-300">
					Hash lookup → <span class="text-emerald-400 font-mono">O(1)</span>
				</p>
				<div class="mt-3 pt-3 border-t border-slate-700/50">
					<p class="text-xs text-slate-500">
						1M nodes = <span class="text-emerald-400 font-medium">~100ns</span>
					</p>
				</div>
			</div>
		</div>
	);
}

// Index structure visualization
function KeyIndexStructure() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-cyan-400 mb-4">Key Index Structure</h4>

			<div class="space-y-4">
				{/* Bucket array */}
				<div class="p-3 rounded-lg bg-slate-800/50 border border-cyan-500/20">
					<div class="flex items-center justify-between mb-2">
						<span class="text-sm font-medium text-cyan-400">Bucket Array</span>
						<span class="text-xs text-slate-500">n buckets</span>
					</div>
					<div class="flex gap-0.5">
						<div class="w-10 py-1 text-center text-xs font-mono rounded-l bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
							0
						</div>
						<div class="w-10 py-1 text-center text-xs font-mono bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
							1
						</div>
						<div class="w-10 py-1 text-center text-xs font-mono bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
							2
						</div>
						<div class="w-10 py-1 text-center text-xs font-mono bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
							3
						</div>
						<div class="w-10 py-1 text-center text-xs font-mono rounded-r bg-cyan-500/20 text-cyan-400">
							...
						</div>
					</div>
					<p class="text-xs text-slate-500 mt-1">
						← Start offsets into entry array
					</p>
				</div>

				{/* Entry array */}
				<div class="p-3 rounded-lg bg-slate-800/50 border border-violet-500/20">
					<div class="flex items-center justify-between mb-2">
						<span class="text-sm font-medium text-violet-400">Entry Array</span>
						<span class="text-xs text-slate-500">
							sorted by bucket, then hash
						</span>
					</div>
					<div class="space-y-1">
						<div class="flex gap-0.5 text-xs">
							<div class="flex-1 py-1 px-2 rounded-l bg-violet-500/20 text-violet-400 font-mono">
								hash64
							</div>
							<div class="flex-1 py-1 px-2 bg-violet-500/20 text-violet-400 font-mono">
								stringId
							</div>
							<div class="flex-1 py-1 px-2 rounded-r bg-violet-500/20 text-violet-400 font-mono">
								nodeId
							</div>
							<div class="w-20 py-1 text-center text-slate-500">← bucket 0</div>
						</div>
						<div class="flex gap-0.5 text-xs">
							<div class="flex-1 py-1 px-2 rounded-l bg-violet-500/10 text-violet-300 font-mono">
								hash64
							</div>
							<div class="flex-1 py-1 px-2 bg-violet-500/10 text-violet-300 font-mono">
								stringId
							</div>
							<div class="flex-1 py-1 px-2 rounded-r bg-violet-500/10 text-violet-300 font-mono">
								nodeId
							</div>
							<div class="w-20 py-1 text-center text-slate-500">← bucket 1</div>
						</div>
						<div class="flex gap-0.5 text-xs">
							<div class="flex-1 py-1 px-2 rounded-l bg-slate-700/50 text-slate-400 font-mono">
								...
							</div>
							<div class="flex-1 py-1 px-2 bg-slate-700/50 text-slate-400 font-mono">
								...
							</div>
							<div class="flex-1 py-1 px-2 rounded-r bg-slate-700/50 text-slate-400 font-mono">
								...
							</div>
							<div class="w-20" />
						</div>
					</div>
				</div>
			</div>

			{/* Legend */}
			<div class="mt-4 pt-3 border-t border-slate-700/50 space-y-1 text-xs text-slate-500">
				<p>
					<span class="text-cyan-400 font-mono">hash64</span>: xxHash64 of the
					key string
				</p>
				<p>
					<span class="text-cyan-400 font-mono">stringId</span>: Index into
					string table (for collision resolution)
				</p>
				<p>
					<span class="text-cyan-400 font-mono">nodeId</span>: The NodeID this
					key maps to
				</p>
			</div>
		</div>
	);
}

// Lookup process
function KeyLookupProcess() {
	return (
		<div class="my-6 rounded-xl border border-violet-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-violet-400 mb-4">Lookup Process</h4>

			<div class="space-y-4">
				{/* Step 1 - Delta */}
				<div class="p-3 rounded-lg bg-cyan-500/10 border border-cyan-500/20">
					<div class="flex items-center gap-2 mb-2">
						<span class="flex items-center justify-center w-5 h-5 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold">
							1
						</span>
						<span class="text-sm font-medium text-cyan-400">
							Check Delta First
						</span>
						<span class="text-xs text-slate-500 ml-auto">recent changes</span>
					</div>
					<div class="ml-7 space-y-1 text-sm text-slate-400">
						<p>
							If key in{" "}
							<code class="text-red-400 text-xs">delta.keyIndexDeleted</code> →
							return null
						</p>
						<p>
							If key in{" "}
							<code class="text-emerald-400 text-xs">delta.keyIndex</code> →
							return NodeID
						</p>
					</div>
				</div>

				{/* Step 2 - Snapshot */}
				<div class="p-3 rounded-lg bg-violet-500/10 border border-violet-500/20">
					<div class="flex items-center gap-2 mb-2">
						<span class="flex items-center justify-center w-5 h-5 rounded-full bg-violet-500/20 text-violet-400 text-xs font-bold">
							2
						</span>
						<span class="text-sm font-medium text-violet-400">
							Search Snapshot Index
						</span>
						<span class="text-xs text-slate-500 ml-auto">on disk</span>
					</div>
					<div class="ml-7 space-y-1 text-sm font-mono text-slate-400">
						<p>hash = xxHash64(key)</p>
						<p>bucket = hash % numBuckets</p>
						<p>start = bucketArray[bucket]</p>
						<p>end = bucketArray[bucket + 1]</p>
					</div>
				</div>

				{/* Step 3 - Search */}
				<div class="p-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
					<div class="flex items-center gap-2 mb-2">
						<span class="flex items-center justify-center w-5 h-5 rounded-full bg-emerald-500/20 text-emerald-400 text-xs font-bold">
							3
						</span>
						<span class="text-sm font-medium text-emerald-400">
							Binary Search in Bucket
						</span>
					</div>
					<div class="ml-7 space-y-1 text-sm text-slate-400">
						<p>
							Find entry where{" "}
							<code class="text-cyan-400 text-xs">entry.hash64 == hash</code>
						</p>
						<p>
							Verify:{" "}
							<code class="text-cyan-400 text-xs">
								stringTable[entry.stringId] == key
							</code>
						</p>
						<p>
							Return <code class="text-emerald-400 text-xs">entry.nodeId</code>
						</p>
					</div>
				</div>
			</div>
		</div>
	);
}

// Two-level lookup
function TwoLevelLookup() {
	return (
		<div class="my-6 rounded-xl border border-slate-600/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-slate-400 mb-4">Two-Level Lookup</h4>

			<div class="grid sm:grid-cols-2 gap-4 mb-4">
				{/* Delta */}
				<div class="p-3 rounded-lg bg-violet-500/10 border border-violet-500/20">
					<div class="text-sm font-medium text-violet-400 mb-2">
						Delta (in-memory)
					</div>
					<div class="space-y-1 text-xs">
						<div class="flex items-center gap-2">
							<span class="text-slate-600">├</span>
							<code class="text-emerald-400">keyIndex</code>
							<span class="text-slate-500">Map&lt;string, NodeID&gt;</span>
						</div>
						<div class="flex items-center gap-2">
							<span class="text-slate-600">└</span>
							<code class="text-red-400">keyIndexDeleted</code>
							<span class="text-slate-500">Set&lt;string&gt;</span>
						</div>
					</div>
				</div>

				{/* Snapshot */}
				<div class="p-3 rounded-lg bg-cyan-500/10 border border-cyan-500/20">
					<div class="text-sm font-medium text-cyan-400 mb-2">
						Snapshot (on disk)
					</div>
					<div class="space-y-1 text-xs">
						<div class="flex items-center gap-2">
							<span class="text-slate-600">├</span>
							<code class="text-cyan-400">bucketArray</code>
							<span class="text-slate-500">u32[]</span>
						</div>
						<div class="flex items-center gap-2">
							<span class="text-slate-600">└</span>
							<code class="text-cyan-400">entries</code>
							<span class="text-slate-500">
								{"{hash64, stringId, nodeId}[]"}
							</span>
						</div>
					</div>
				</div>
			</div>

			{/* Lookup order */}
			<div class="pt-3 border-t border-slate-700/50">
				<p class="text-xs text-slate-500 mb-2">Lookup order:</p>
				<div class="space-y-1 text-sm">
					<div class="flex items-center gap-2">
						<span class="w-5 h-5 rounded bg-slate-700 text-slate-400 flex items-center justify-center text-xs font-bold">
							1
						</span>
						<code class="text-red-400 text-xs">delta.keyIndexDeleted</code>
						<span class="text-slate-500">→ If found, return null</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="w-5 h-5 rounded bg-slate-700 text-slate-400 flex items-center justify-center text-xs font-bold">
							2
						</span>
						<code class="text-emerald-400 text-xs">delta.keyIndex</code>
						<span class="text-slate-500">→ If found, return NodeID</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="w-5 h-5 rounded bg-slate-700 text-slate-400 flex items-center justify-center text-xs font-bold">
							3
						</span>
						<code class="text-cyan-400 text-xs">snapshot index</code>
						<span class="text-slate-500">→ Search hash buckets</span>
					</div>
				</div>
				<p class="text-xs text-slate-500 mt-2 italic">
					This order ensures recent changes override old data.
				</p>
			</div>
		</div>
	);
}

// xxHash64 explanation
function XxHash64Explanation() {
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
						d="M13 10V3L4 14h7v7l9-11h-7z"
					/>
				</svg>
				<h4 class="font-semibold text-emerald-400">Why xxHash64</h4>
			</div>

			{/* Requirements */}
			<div class="mb-4 space-y-1 text-sm">
				<div class="flex items-center gap-2">
					<span class="text-emerald-400">✓</span>
					<span class="text-emerald-400 font-medium">Fast</span>
					<span class="text-slate-400">— Called on every key lookup</span>
				</div>
				<div class="flex items-center gap-2">
					<span class="text-emerald-400">✓</span>
					<span class="text-emerald-400 font-medium">Good distribution</span>
					<span class="text-slate-400">— Minimize bucket collisions</span>
				</div>
				<div class="flex items-center gap-2">
					<span class="text-emerald-400">✓</span>
					<span class="text-emerald-400 font-medium">Deterministic</span>
					<span class="text-slate-400">— Same key always same hash</span>
				</div>
			</div>

			{/* Comparison */}
			<div class="p-3 rounded-lg bg-slate-800/50 border border-slate-700 mb-4">
				<p class="text-xs text-slate-500 mb-2">
					For typical key lengths (10-100 bytes):
				</p>
				<div class="grid grid-cols-2 gap-3">
					<div>
						<p class="text-sm text-emerald-400 font-medium">xxHash64</p>
						<p class="text-xs text-slate-400 font-mono">~50-100ns per hash</p>
						<p class="text-xs text-slate-500">~10 GB/s throughput</p>
					</div>
					<div>
						<p class="text-sm text-slate-400 font-medium">SHA-256</p>
						<p class="text-xs text-slate-400 font-mono">~500-1000ns per hash</p>
						<p class="text-xs text-slate-500">Cryptographic (overkill)</p>
					</div>
				</div>
			</div>

			<p class="text-xs text-emerald-400">
				10x faster, and we don't need cryptographic security.
			</p>
		</div>
	);
}

// Collision handling
function CollisionHandling() {
	return (
		<div class="my-6 rounded-xl border border-amber-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
				<svg
					class="w-5 h-5 text-amber-400"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
					/>
				</svg>
				<h4 class="font-semibold text-amber-400">Handling Collisions</h4>
			</div>

			{/* Scenario */}
			<div class="mb-4 p-3 rounded-lg bg-slate-800/50 border border-slate-700">
				<p class="text-xs text-slate-500 mb-2">
					Scenario: Two keys hash to same bucket
				</p>
				<div class="space-y-1 text-sm font-mono">
					<p>
						<span class="text-cyan-400">"user:alice"</span> → hash:{" "}
						<span class="text-slate-400">0x1234...</span>
					</p>
					<p>
						<span class="text-cyan-400">"user:alfred"</span> → hash:{" "}
						<span class="text-amber-400">0x1234...</span>{" "}
						<span class="text-xs text-amber-400">(collision!)</span>
					</p>
				</div>
			</div>

			{/* Resolution steps */}
			<div class="space-y-2 mb-4">
				<div class="flex items-center gap-3 text-sm">
					<span class="w-5 h-5 rounded bg-slate-700 text-slate-400 flex items-center justify-center text-xs font-bold">
						1
					</span>
					<span class="text-slate-300">Both entries stored in same bucket</span>
				</div>
				<div class="flex items-center gap-3 text-sm">
					<span class="w-5 h-5 rounded bg-slate-700 text-slate-400 flex items-center justify-center text-xs font-bold">
						2
					</span>
					<span class="text-slate-300">On lookup, hash matches both</span>
				</div>
				<div class="flex items-center gap-3 text-sm">
					<span class="w-5 h-5 rounded bg-slate-700 text-slate-400 flex items-center justify-center text-xs font-bold">
						3
					</span>
					<span class="text-slate-300">stringId comparison breaks tie</span>
				</div>
				<div class="flex items-center gap-3 text-sm">
					<span class="w-5 h-5 rounded bg-emerald-500/20 text-emerald-400 flex items-center justify-center text-xs font-bold">
						4
					</span>
					<span class="text-emerald-300">
						Actual string comparison confirms match
					</span>
				</div>
			</div>

			<p class="text-xs text-slate-500">
				Cost: <span class="text-amber-400">O(k)</span> string comparisons where
				k = entries with same hash. With 64-bit hash:{" "}
				<span class="text-emerald-400">k ≈ 1</span>
			</p>
		</div>
	);
}

// Load factor visualization
function LoadFactorDiagram() {
	return (
		<div class="my-6 rounded-xl border border-slate-600/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-slate-400 mb-4">Load Factor</h4>

			<p class="text-sm text-slate-400 mb-4">
				<code class="text-cyan-400">Load factor = entries / buckets</code>
			</p>

			<div class="p-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20 mb-4">
				<p class="text-sm text-emerald-400 font-medium mb-2">
					KiteDB uses ~50% load factor
				</p>
				<p class="text-xs text-slate-400">(2x buckets as entries)</p>
				<ul class="mt-2 space-y-1 text-sm text-slate-400">
					<li class="flex items-center gap-2">
						<span class="text-emerald-400">•</span>
						Low collision rate
					</li>
					<li class="flex items-center gap-2">
						<span class="text-emerald-400">•</span>
						Reasonable memory usage
					</li>
					<li class="flex items-center gap-2">
						<span class="text-emerald-400">•</span>
						Fast lookups
					</li>
				</ul>
			</div>

			{/* Example */}
			<div class="p-3 rounded-lg bg-slate-800/50 border border-slate-700">
				<p class="text-xs text-slate-500 mb-2">With 1M keys:</p>
				<div class="space-y-1 text-sm">
					<div class="flex justify-between">
						<span class="text-slate-400">Buckets:</span>
						<span class="text-cyan-400 font-mono">2M × 4 bytes = 8 MB</span>
					</div>
					<div class="flex justify-between">
						<span class="text-slate-400">Entries:</span>
						<span class="text-cyan-400 font-mono">1M × 24 bytes = 24 MB</span>
					</div>
					<div class="flex justify-between pt-2 border-t border-slate-700 mt-2">
						<span class="text-slate-300 font-medium">Total index:</span>
						<span class="text-emerald-400 font-mono font-medium">~32 MB</span>
					</div>
				</div>
				<p class="text-xs text-slate-500 mt-3">
					Lookup: 1 bucket read + 1-2 entry reads ={" "}
					<span class="text-emerald-400">~100ns</span>
				</p>
			</div>
		</div>
	);
}

// ============================================================================
// PAGE COMPONENT
// ============================================================================

export function KeyIndexPage() {
	return (
		<DocPage slug="internals/key-index">
			<p>
				Every node in KiteDB can have a string key for lookup. The key index
				provides O(1) average-case lookups from key to NodeID.
			</p>

			<h2 id="the-problem">The Problem</h2>

			<KeyIndexProblem />

			<h2 id="structure">Index Structure</h2>

			<p>The key index uses hash buckets with linear probing:</p>

			<KeyIndexStructure />

			<h2 id="lookup">Lookup Process</h2>

			<KeyLookupProcess />

			<h2 id="two-level">Two-Level Lookup</h2>

			<p>The key index is split between delta (memory) and snapshot (disk):</p>

			<TwoLevelLookup />

			<h2 id="hashing">Why xxHash64</h2>

			<XxHash64Explanation />

			<h2 id="collisions">Handling Collisions</h2>

			<CollisionHandling />

			<h2 id="load-factor">Load Factor</h2>

			<LoadFactorDiagram />

			<h2 id="next">Next Steps</h2>
			<ul>
				<li>
					<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – How
					the two-level lookup fits in
				</li>
				<li>
					<a href="/docs/internals/performance">Performance</a> – Index
					optimization techniques
				</li>
			</ul>
		</DocPage>
	);
}
