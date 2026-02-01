import CodeBlock from "~/components/code-block";
import DocPage from "~/components/doc-page";

// ============================================================================
// CSR-SPECIFIC COMPONENTS
// ============================================================================

// Adjacency Matrix problem visualization
function AdjacencyMatrixProblem() {
	return (
		<div class="my-6 rounded-xl border border-red-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
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
						d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
					/>
				</svg>
				<h4 class="font-semibold text-red-400">Adjacency Matrix</h4>
			</div>

			{/* Mini matrix visualization */}
			<div class="flex flex-col sm:flex-row gap-4 items-start">
				<div class="font-mono text-xs">
					<div class="text-slate-500 mb-1">{"     A  B  C  D"}</div>
					<div class="text-slate-400">
						A <span class="text-slate-600">[</span> 0{" "}
						<span class="text-cyan-400">1</span>{" "}
						<span class="text-cyan-400">1</span> 0{" "}
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						B <span class="text-slate-600">[</span> 0 0 0{" "}
						<span class="text-cyan-400">1</span>{" "}
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						C <span class="text-slate-600">[</span>{" "}
						<span class="text-cyan-400">1</span> 0 0 0{" "}
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						D <span class="text-slate-600">[</span> 0 0 0 0{" "}
						<span class="text-slate-600">]</span>
					</div>
				</div>
				<div class="flex-1 space-y-2 text-sm">
					<div class="flex items-center gap-2">
						<span class="text-red-400 font-mono">100K × 100K</span>
						<span class="text-slate-500">=</span>
						<span class="text-red-400 font-bold">10 billion</span>
						<span class="text-slate-400">entries</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="text-slate-400">Actual edges:</span>
						<span class="text-cyan-400 font-mono">1M</span>
						<span class="text-slate-500">(0.01% used)</span>
					</div>
					<div class="mt-3 px-3 py-2 rounded-lg bg-red-500/10 border border-red-500/20 text-red-400 text-xs">
						Wastes 99.99% of space
					</div>
				</div>
			</div>
		</div>
	);
}

// Linked List problem visualization
function LinkedListProblem() {
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
						d="M13 10V3L4 14h7v7l9-11h-7z"
					/>
				</svg>
				<h4 class="font-semibold text-amber-400">Linked Adjacency Lists</h4>
			</div>

			{/* Linked list visualization */}
			<div class="space-y-2 font-mono text-sm mb-4">
				<div class="flex items-center gap-2">
					<span class="text-cyan-400 w-4">A</span>
					<span class="text-slate-500">→</span>
					<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-300">B</span>
					<span class="text-slate-500">→</span>
					<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-300">C</span>
					<span class="text-slate-500">→</span>
					<span class="text-slate-600">null</span>
				</div>
				<div class="flex items-center gap-2">
					<span class="text-cyan-400 w-4">B</span>
					<span class="text-slate-500">→</span>
					<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-300">D</span>
					<span class="text-slate-500">→</span>
					<span class="text-slate-600">null</span>
				</div>
			</div>

			<div class="space-y-2 text-sm">
				<p class="text-slate-400">
					<span class="text-amber-400 font-semibold">Problem:</span> Pointer
					chasing. Each lookup goes to random memory.
				</p>
				<div class="flex flex-wrap gap-4 text-xs">
					<div class="flex items-center gap-2">
						<span class="text-red-400">Cache miss:</span>
						<span class="text-slate-300 font-mono">~100ns</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="text-emerald-400">Cache hit:</span>
						<span class="text-slate-300 font-mono">~1ns</span>
					</div>
				</div>
				<div class="mt-3 px-3 py-2 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-400 text-xs">
					1000 edges × 100ns = 100μs wasted waiting for RAM
				</div>
			</div>
		</div>
	);
}

// CSR Solution visualization
function CSRSolutionDiagram() {
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
						d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
					/>
				</svg>
				<h4 class="font-semibold text-emerald-400">CSR Solution</h4>
			</div>

			{/* Graph */}
			<div class="mb-6 p-3 rounded-lg bg-slate-800/50 border border-slate-700">
				<div class="text-xs text-slate-500 mb-2">Graph:</div>
				<div class="flex flex-wrap gap-x-6 gap-y-1 font-mono text-sm">
					<span>
						<span class="text-cyan-400">A</span>{" "}
						<span class="text-slate-500">→</span> B, C
					</span>
					<span>
						<span class="text-cyan-400">B</span>{" "}
						<span class="text-slate-500">→</span> D
					</span>
					<span>
						<span class="text-cyan-400">C</span>{" "}
						<span class="text-slate-500">→</span> A
					</span>
					<span>
						<span class="text-cyan-400">D</span>{" "}
						<span class="text-slate-500">→</span>{" "}
						<span class="text-slate-600">(none)</span>
					</span>
				</div>
			</div>

			{/* Step by step */}
			<div class="space-y-6">
				{/* Step 1 - Destinations */}
				<div class="flex gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0 mt-0.5">
						1
					</span>
					<div>
						<div class="text-sm text-slate-400 mb-3">
							Concatenate all destinations:
						</div>
						<div class="flex items-center font-mono text-sm">
							<span class="text-violet-400 mr-3">destinations</span>
							<span class="text-slate-500">=</span>
							{/* Boxes with bracket labels below */}
							<div class="ml-3">
								<div class="flex">
									<div class="w-10 py-1 text-center rounded-l bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
										B
									</div>
									<div class="w-10 py-1 text-center bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
										C
									</div>
									<div class="w-10 py-1 text-center bg-violet-500/20 text-violet-400 border-r border-violet-500/30">
										D
									</div>
									<div class="w-10 py-1 text-center rounded-r bg-emerald-500/20 text-emerald-400">
										A
									</div>
								</div>
								{/* Bracket labels using borders for cleaner look */}
								<div class="flex mt-1">
									{/* A's edges (B, C) - spans 2 boxes */}
									<div class="w-20 flex flex-col items-center">
										<div class="w-full h-2 border-l border-r border-b border-slate-500 rounded-b-sm" />
										<span class="text-xs text-cyan-400 mt-0.5">A</span>
									</div>
									{/* B's edge (D) */}
									<div class="w-10 flex flex-col items-center">
										<div class="w-4 h-2 border-l border-r border-b border-slate-500 rounded-b-sm" />
										<span class="text-xs text-violet-400 mt-0.5">B</span>
									</div>
									{/* C's edge (A) */}
									<div class="w-10 flex flex-col items-center">
										<div class="w-4 h-2 border-l border-r border-b border-slate-500 rounded-b-sm" />
										<span class="text-xs text-emerald-400 mt-0.5">C</span>
									</div>
								</div>
							</div>
						</div>
					</div>
				</div>

				{/* Step 2 - Offsets */}
				<div class="flex gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0 mt-0.5">
						2
					</span>
					<div>
						<div class="text-sm text-slate-400 mb-3">
							Record where each node's edges start:
						</div>
						<div class="flex items-center font-mono text-sm">
							<span class="text-violet-400 mr-3">offsets</span>
							<span class="text-slate-500">=</span>
							{/* Boxes with arrows and labels below - each column aligned */}
							<div class="ml-3">
								<div class="flex">
									<div class="w-10 py-1 text-center rounded-l bg-slate-700 text-slate-300 border-r border-slate-600">
										0
									</div>
									<div class="w-10 py-1 text-center bg-slate-700 text-slate-300 border-r border-slate-600">
										2
									</div>
									<div class="w-10 py-1 text-center bg-slate-700 text-slate-300 border-r border-slate-600">
										3
									</div>
									<div class="w-10 py-1 text-center bg-slate-700 text-slate-300 border-r border-slate-600">
										4
									</div>
									<div class="w-10 py-1 text-center rounded-r bg-slate-700 text-slate-300">
										4
									</div>
								</div>
								<div class="flex text-xs text-slate-500 mt-0.5">
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
								</div>
								<div class="flex text-xs text-slate-500">
									<div class="w-10 text-center">A</div>
									<div class="w-10 text-center">B</div>
									<div class="w-10 text-center">C</div>
									<div class="w-10 text-center">D</div>
									<div class="w-10 text-center text-slate-400">end</div>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}

// CSR Traversal example
function CSRTraversalExample() {
	return (
		<div class="my-6 space-y-3">
			{/* Example 1 */}
			<div class="rounded-xl border border-cyan-500/30 bg-slate-800/50 p-4">
				<div class="text-sm text-slate-400 mb-2">
					"Who does <span class="text-cyan-400 font-semibold">A</span> connect
					to?"
				</div>
				<div class="space-y-1 font-mono text-sm">
					<div class="text-slate-400">
						start = offsets[<span class="text-cyan-400">0</span>] ={" "}
						<span class="text-emerald-400">0</span>
					</div>
					<div class="text-slate-400">
						end = offsets[<span class="text-cyan-400">1</span>] ={" "}
						<span class="text-emerald-400">2</span>
					</div>
					<div class="text-slate-400">
						destinations[<span class="text-emerald-400">0</span>:
						<span class="text-emerald-400">2</span>] ={" "}
						<span class="text-cyan-400">[B, C]</span>{" "}
						<span class="text-emerald-400">✓</span>
					</div>
				</div>
			</div>

			{/* Example 2 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="text-sm text-slate-400 mb-2">
					"Who does <span class="text-cyan-400 font-semibold">D</span> connect
					to?"
				</div>
				<div class="space-y-1 font-mono text-sm">
					<div class="text-slate-400">
						start = offsets[<span class="text-cyan-400">3</span>] ={" "}
						<span class="text-emerald-400">4</span>
					</div>
					<div class="text-slate-400">
						end = offsets[<span class="text-cyan-400">4</span>] ={" "}
						<span class="text-emerald-400">4</span>
					</div>
					<div class="text-slate-400">
						destinations[<span class="text-emerald-400">4</span>:
						<span class="text-emerald-400">4</span>] ={" "}
						<span class="text-slate-500">[]</span>{" "}
						<span class="text-slate-500">(no edges)</span>{" "}
						<span class="text-emerald-400">✓</span>
					</div>
				</div>
			</div>

			{/* Algorithm */}
			<div class="rounded-xl border border-violet-500/30 bg-slate-800/50 p-4">
				<div class="text-xs text-violet-400 font-semibold mb-2">Algorithm:</div>
				<div class="font-mono text-sm text-slate-300">
					<div>start = offsets[node]</div>
					<div>end = offsets[node + 1]</div>
					<div class="text-emerald-400">return destinations[start:end]</div>
				</div>
			</div>
		</div>
	);
}

// Memory layout comparison
function MemoryLayoutComparison() {
	return (
		<div class="my-6 grid sm:grid-cols-2 gap-4">
			{/* Linked List - Bad */}
			<div class="rounded-xl border border-red-500/30 bg-slate-800/50 p-4">
				<div class="flex items-center gap-2 mb-4">
					<span class="text-red-400 text-xs font-semibold">LINKED LIST</span>
					<span class="text-slate-600">—</span>
					<span class="text-slate-500 text-xs">scattered</span>
				</div>
				{/* Boxes with addresses aligned below each */}
				<div class="flex gap-6 mb-1">
					<div class="w-10 px-3 py-2 rounded bg-slate-700 text-slate-300 font-mono text-sm text-center">
						B
					</div>
					<div class="w-10 px-3 py-2 rounded bg-slate-700 text-slate-300 font-mono text-sm text-center">
						C
					</div>
					<div class="w-10 px-3 py-2 rounded bg-slate-700 text-slate-300 font-mono text-sm text-center">
						D
					</div>
				</div>
				<div class="flex gap-6 mb-3">
					<div class="w-10 text-center text-xs font-mono text-red-400/70">
						0x1000
					</div>
					<div class="w-10 text-center text-xs font-mono text-red-400/70">
						0x5F00
					</div>
					<div class="w-10 text-center text-xs font-mono text-red-400/70">
						0x2A00
					</div>
				</div>
				<div class="text-xs text-red-400">
					↑ Random locations = cache misses
				</div>
			</div>

			{/* CSR - Good */}
			<div class="rounded-xl border border-emerald-500/30 bg-slate-800/50 p-4">
				<div class="flex items-center gap-2 mb-4">
					<span class="text-emerald-400 text-xs font-semibold">CSR</span>
					<span class="text-slate-600">—</span>
					<span class="text-slate-500 text-xs">contiguous</span>
				</div>
				{/* Connected boxes with addresses aligned below each */}
				<div class="flex mb-1">
					<div class="w-10 py-2 rounded-l bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center border-r border-cyan-500/30">
						B
					</div>
					<div class="w-10 py-2 bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center border-r border-cyan-500/30">
						C
					</div>
					<div class="w-10 py-2 bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center border-r border-cyan-500/30">
						D
					</div>
					<div class="w-10 py-2 rounded-r bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center">
						A
					</div>
				</div>
				<div class="flex mb-3">
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						0x1000
					</div>
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						+4
					</div>
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						+8
					</div>
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						+C
					</div>
				</div>
				<div class="text-xs text-emerald-400">
					↑ Sequential = CPU prefetcher works
				</div>
			</div>
		</div>
	);
}

// Bidirectional edges visualization
function BidirectionalEdges() {
	return (
		<div class="my-6 rounded-xl border border-violet-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="grid sm:grid-cols-2 gap-4">
				{/* Out-edges */}
				<div class="p-3 rounded-lg bg-slate-800/50 border border-cyan-500/20">
					<div class="text-xs text-cyan-400 font-semibold mb-2">
						Out-edges (A → B)
					</div>
					<div class="space-y-1 font-mono text-xs">
						<div class="text-slate-400">
							out_offsets = <span class="text-slate-300">[0, 2, 3, 4, 4]</span>
						</div>
						<div class="text-slate-400">
							out_dst = <span class="text-cyan-400">[B, C, D, A]</span>
						</div>
					</div>
					<div class="mt-2 text-xs text-slate-500">
						"Who does Alice follow?"
					</div>
				</div>

				{/* In-edges */}
				<div class="p-3 rounded-lg bg-slate-800/50 border border-violet-500/20">
					<div class="text-xs text-violet-400 font-semibold mb-2">
						In-edges (A ← C)
					</div>
					<div class="space-y-1 font-mono text-xs">
						<div class="text-slate-400">
							in_offsets = <span class="text-slate-300">[0, 1, 2, 3, 4]</span>
						</div>
						<div class="text-slate-400">
							in_src = <span class="text-violet-400">[C, A, A, B]</span>
						</div>
					</div>
					<div class="mt-2 text-xs text-slate-500">"Who follows Alice?"</div>
				</div>
			</div>

			<div class="mt-4 px-3 py-2 rounded-lg bg-slate-700/30 text-xs text-slate-400">
				<span class="text-amber-400">Trade-off:</span> 2× storage, but O(1)
				traversal in both directions
			</div>
		</div>
	);
}

// Edge types visualization
function EdgeTypesSorting() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="mb-4">
				<div class="font-mono text-sm space-y-1">
					<div class="text-slate-400">
						out_dst = <span class="text-slate-600">[</span>
						<span class="text-cyan-400">B</span>,{" "}
						<span class="text-cyan-400">C</span>,{" "}
						<span class="text-cyan-400">D</span>,{" "}
						<span class="text-cyan-400">A</span>
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						out_etype = <span class="text-slate-600">[</span>
						<span class="text-emerald-400">0</span>,{" "}
						<span class="text-violet-400">1</span>,{" "}
						<span class="text-emerald-400">0</span>,{" "}
						<span class="text-emerald-400">0</span>
						<span class="text-slate-600">]</span>
					</div>
				</div>
				<div class="flex gap-4 mt-2 text-xs">
					<div class="flex items-center gap-1">
						<span class="w-2 h-2 rounded-full bg-emerald-400" />
						<span class="text-slate-400">0 = KNOWS</span>
					</div>
					<div class="flex items-center gap-1">
						<span class="w-2 h-2 rounded-full bg-violet-400" />
						<span class="text-slate-400">1 = LIKES</span>
					</div>
				</div>
			</div>

			<div class="text-xs text-slate-500 mb-3">
				Sorted by (etype, dst) within each node:
			</div>

			<div class="space-y-2">
				<div class="flex items-center gap-2 text-sm">
					<span class="w-4 h-4 rounded bg-emerald-500/20 text-emerald-400 flex items-center justify-center text-xs">
						1
					</span>
					<span class="text-slate-300">
						Binary search to find specific edge type
					</span>
				</div>
				<div class="flex items-center gap-2 text-sm">
					<span class="w-4 h-4 rounded bg-emerald-500/20 text-emerald-400 flex items-center justify-center text-xs">
						2
					</span>
					<span class="text-slate-300">
						Early termination when past desired type
					</span>
				</div>
				<div class="flex items-center gap-2 text-sm">
					<span class="w-4 h-4 rounded bg-emerald-500/20 text-emerald-400 flex items-center justify-center text-xs">
						3
					</span>
					<span class="text-slate-300">
						"Get A's KNOWS edges" doesn't scan all
					</span>
				</div>
			</div>
		</div>
	);
}

// ============================================================================
// PAGE COMPONENT
// ============================================================================

export function CSRPage() {
	return (
		<DocPage slug="internals/csr">
			<p>
				KiteDB uses <strong>Compressed Sparse Row (CSR)</strong> format to store
				graph edges. CSR is a standard format for sparse matrices that provides
				fast traversal with minimal memory overhead.
			</p>

			<h2 id="the-problem">The Problem with Naive Edge Storage</h2>

			<p>Consider a graph with 100,000 nodes and 1 million edges.</p>

			<AdjacencyMatrixProblem />

			<LinkedListProblem />

			<h2 id="csr-solution">The CSR Solution</h2>

			<p>
				CSR stores all edges in two flat arrays: <strong>offsets</strong> and{" "}
				<strong>destinations</strong>. No pointers, no wasted space.
			</p>

			<CSRSolutionDiagram />

			<h2 id="traversal">How Traversal Works</h2>

			<p>Finding a node's neighbors is two array lookups:</p>

			<CSRTraversalExample />

			<h2 id="memory-layout">Why It's Fast: Memory Layout</h2>

			<MemoryLayoutComparison />

			<p class="text-sm text-slate-400">
				After the first access, B/C/D/A are already in CPU cache.
			</p>

			<h2 id="bidirectional">Bidirectional Edges</h2>

			<p>
				KiteDB stores edges in <strong>both directions</strong> for fast
				traversal either way:
			</p>

			<BidirectionalEdges />

			<h2 id="edge-types">Edge Types and Sorting</h2>

			<p>
				Real graphs have different edge types (follows, likes, knows). KiteDB
				stores edge types in a parallel array, sorted within each node:
			</p>

			<EdgeTypesSorting />

			<h2 id="existence-check">Edge Existence Check</h2>

			<p>To check if edge A→B exists with type KNOWS:</p>

			<CodeBlock
				code={`function hasEdge(src: NodeID, etype: EdgeType, dst: NodeID): boolean {
  const start = offsets[src];
  const end = offsets[src + 1];
  
  // Binary search for etype within [start, end)
  const typeStart = binarySearchStart(etypes, start, end, etype);
  const typeEnd = binarySearchEnd(etypes, start, end, etype);
  
  // Binary search for dst within type range
  return binarySearch(destinations, typeStart, typeEnd, dst);
}

// Complexity: O(log k) where k = number of edges from src`}
				language="typescript"
			/>

			<h2 id="numbers">Performance Numbers</h2>

			<table>
				<thead>
					<tr>
						<th>Operation</th>
						<th>CSR</th>
						<th>Linked List</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td>Start traversal</td>
						<td>O(1) – two array lookups</td>
						<td>O(1) – follow pointer</td>
					</tr>
					<tr>
						<td>Iterate k neighbors</td>
						<td>O(k) – sequential read</td>
						<td>O(k) – but cache misses</td>
					</tr>
					<tr>
						<td>Edge existence</td>
						<td>O(log k) – binary search</td>
						<td>O(k) – linear scan</td>
					</tr>
					<tr>
						<td>Cache behavior</td>
						<td>Excellent – prefetcher works</td>
						<td>Poor – random access</td>
					</tr>
				</tbody>
			</table>

			<h2 id="next">Next Steps</h2>
			<ul>
				<li>
					<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – How
					CSR fits into the storage model
				</li>
				<li>
					<a href="/docs/internals/key-index">Key Index</a> – How node lookups
					work
				</li>
				<li>
					<a href="/docs/internals/performance">Performance</a> – Optimization
					techniques
				</li>
			</ul>
		</DocPage>
	);
}
