import CodeBlock from "~/components/code-block";
import DocPage from "~/components/doc-page";

// ============================================================================
// MVCC-SPECIFIC COMPONENTS
// ============================================================================

// Snapshot isolation timeline
function SnapshotIsolationTimeline() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-cyan-400 mb-4">
				Snapshot Isolation Timeline
			</h4>

			{/* Timeline visual */}
			<div class="relative mb-6">
				{/* Timeline line */}
				<div class="h-1 bg-gradient-to-r from-cyan-500/50 via-violet-500/50 to-emerald-500/50 rounded-full" />

				{/* Events */}
				<div class="flex justify-between mt-2">
					<div class="flex flex-col items-center">
						<div class="w-3 h-3 rounded-full bg-cyan-400 -mt-4 mb-2" />
						<span class="text-xs text-cyan-400 font-medium">T1 starts</span>
						<span class="text-xs text-slate-500">sees v1</span>
					</div>
					<div class="flex flex-col items-center">
						<div class="w-3 h-3 rounded-full bg-violet-400 -mt-4 mb-2" />
						<span class="text-xs text-violet-400 font-medium">T2 starts</span>
						<span class="text-xs text-slate-500">sees v1</span>
					</div>
					<div class="flex flex-col items-center">
						<div class="w-3 h-3 rounded-full bg-emerald-400 -mt-4 mb-2" />
						<span class="text-xs text-emerald-400 font-medium">T1 commits</span>
						<span class="text-xs text-slate-500">writes v2</span>
					</div>
				</div>
			</div>

			{/* Key point */}
			<div class="p-3 rounded-lg bg-violet-500/10 border border-violet-500/20">
				<p class="text-sm text-slate-300">
					<span class="text-violet-400 font-medium">T2 still sees v1</span> —
					T1's changes are invisible until T2 restarts
				</p>
			</div>
		</div>
	);
}

// Version chain visualization
function VersionChainDiagram() {
	return (
		<div class="my-6 rounded-xl border border-violet-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-violet-400 mb-4">
				Version Chain for Node "alice"
			</h4>

			{/* Chain visualization */}
			<div class="flex items-center justify-center gap-2 mb-4 overflow-x-auto py-2">
				{/* v3 */}
				<div class="shrink-0 rounded-lg border-2 border-emerald-500/40 bg-emerald-500/10 p-3 min-w-[120px]">
					<div class="text-sm font-medium text-emerald-400">v3: age=32</div>
					<div class="text-xs text-slate-500 font-mono mt-1">commitTs=150</div>
				</div>
				<div class="text-slate-500 shrink-0">←</div>
				{/* v2 */}
				<div class="shrink-0 rounded-lg border-2 border-violet-500/40 bg-violet-500/10 p-3 min-w-[120px]">
					<div class="text-sm font-medium text-violet-400">v2: age=31</div>
					<div class="text-xs text-slate-500 font-mono mt-1">commitTs=120</div>
				</div>
				<div class="text-slate-500 shrink-0">←</div>
				{/* v1 */}
				<div class="shrink-0 rounded-lg border-2 border-cyan-500/40 bg-cyan-500/10 p-3 min-w-[120px]">
					<div class="text-sm font-medium text-cyan-400">v1: age=30</div>
					<div class="text-xs text-slate-500 font-mono mt-1">commitTs=80</div>
				</div>
			</div>

			{/* Who sees what */}
			<div class="grid grid-cols-3 gap-2 text-center text-xs">
				<div class="p-2 rounded bg-emerald-500/10">
					<span class="text-emerald-400">T3 sees this</span>
					<div class="text-slate-500 font-mono">startTs=145</div>
				</div>
				<div class="p-2 rounded bg-violet-500/10">
					<span class="text-violet-400">T2 sees this</span>
					<div class="text-slate-500 font-mono">startTs=115</div>
				</div>
				<div class="p-2 rounded bg-cyan-500/10">
					<span class="text-cyan-400">T1 sees this</span>
					<div class="text-slate-500 font-mono">startTs=75</div>
				</div>
			</div>

			<p class="text-xs text-slate-500 mt-4 pt-3 border-t border-slate-700/50">
				Each transaction follows the chain to find the version committed before
				it started.
			</p>
		</div>
	);
}

// Visibility rules
function VisibilityRules() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-cyan-400 mb-4">Visibility Rules</h4>

			<p class="text-sm text-slate-400 mb-4">
				A version is visible to transaction T if:
			</p>

			<div class="space-y-3 mb-4">
				{/* Rule 1 */}
				<div class="flex items-start gap-3 p-3 rounded-lg bg-slate-800/50 border border-slate-700">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0">
						1
					</span>
					<div>
						<code class="text-sm text-cyan-400 font-mono">
							version.commitTs &lt;= T.startTs
						</code>
						<p class="text-xs text-slate-500 mt-1">
							Version was committed before T started
						</p>
					</div>
				</div>

				{/* OR */}
				<div class="text-center text-slate-500 text-sm font-medium">OR</div>

				{/* Rule 2 */}
				<div class="flex items-start gap-3 p-3 rounded-lg bg-slate-800/50 border border-slate-700">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-violet-500/20 text-violet-400 text-xs font-bold shrink-0">
						2
					</span>
					<div>
						<code class="text-sm text-violet-400 font-mono">
							version.txid == T.txid
						</code>
						<p class="text-xs text-slate-500 mt-1">
							T created this version itself (read-your-own-writes)
						</p>
					</div>
				</div>
			</div>

			{/* Process */}
			<div class="pt-3 border-t border-slate-700/50 space-y-1 text-sm text-slate-400">
				<p>Walk the chain from newest to oldest.</p>
				<p>Return first visible version.</p>
				<p class="text-slate-500">
					If none visible → entity doesn't exist for this transaction.
				</p>
			</div>
		</div>
	);
}

// Write conflict diagram
function WriteConflictDiagram() {
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
				<h4 class="font-semibold text-amber-400">First-Committer-Wins</h4>
			</div>

			{/* Scenario */}
			<div class="mb-4 p-3 rounded-lg bg-slate-800/50 border border-slate-700">
				<p class="text-xs text-slate-500 mb-2">
					Scenario: Both T1 and T2 modify "alice"
				</p>
				<div class="space-y-1 text-sm">
					<div class="flex items-center gap-2">
						<span class="text-cyan-400 font-mono text-xs">T1</span>
						<span class="text-slate-400">starts at ts=100</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="text-violet-400 font-mono text-xs">T2</span>
						<span class="text-slate-400">starts at ts=105</span>
					</div>
				</div>
			</div>

			{/* Outcomes */}
			<div class="space-y-2">
				<div class="flex items-start gap-3 p-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
					<span class="text-emerald-400 text-lg">✓</span>
					<div>
						<p class="text-sm text-emerald-400 font-medium">
							T1 commits first (ts=110)
						</p>
						<p class="text-xs text-slate-400">Succeeds — no conflict</p>
					</div>
				</div>
				<div class="flex items-start gap-3 p-3 rounded-lg bg-red-500/10 border border-red-500/20">
					<span class="text-red-400 text-lg">✗</span>
					<div>
						<p class="text-sm text-red-400 font-medium">
							T2 tries to commit (ts=115)
						</p>
						<p class="text-xs text-slate-400">
							Was "alice" modified after T2.startTs (105)? Yes!
						</p>
						<p class="text-xs text-red-400 mt-1">
							Rolled back with ConflictError
						</p>
					</div>
				</div>
			</div>

			<p class="text-xs text-slate-500 mt-4 pt-3 border-t border-slate-700/50">
				<span class="text-amber-400">Resolution:</span> T2 must retry with fresh
				read
			</p>
		</div>
	);
}

// Lazy MVCC optimization
function LazyMVCCDiagram() {
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
				<h4 class="font-semibold text-emerald-400">Lazy MVCC Optimization</h4>
			</div>

			<p class="text-sm text-slate-400 mb-4">When T1 modifies "alice":</p>

			<div class="space-y-3 mb-4">
				{/* No concurrent */}
				<div class="flex items-start gap-3 p-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
					<span class="text-emerald-400 font-medium text-sm shrink-0">IF</span>
					<div>
						<p class="text-sm text-slate-300">
							No other transactions are active
						</p>
						<p class="text-xs text-emerald-400 mt-1">
							→ Modify in-place (no version chain)
						</p>
					</div>
				</div>

				{/* Concurrent */}
				<div class="flex items-start gap-3 p-3 rounded-lg bg-violet-500/10 border border-violet-500/20">
					<span class="text-violet-400 font-medium text-sm shrink-0">ELSE</span>
					<div>
						<p class="text-sm text-slate-300">Other transactions are active</p>
						<p class="text-xs text-violet-400 mt-1">
							→ Create version chain (preserve old value)
						</p>
					</div>
				</div>
			</div>

			{/* Result */}
			<div class="pt-3 border-t border-slate-700/50">
				<p class="text-sm text-slate-400">
					<span class="text-emerald-400 font-medium">Result:</span> Serial
					workloads have zero MVCC overhead. Concurrent workloads get correct
					isolation.
				</p>
			</div>
		</div>
	);
}

// Garbage collection process
function MVCCGarbageCollection() {
	return (
		<div class="my-6 rounded-xl border border-slate-600/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="font-semibold text-slate-400 mb-4">Garbage Collection</h4>

			<div class="space-y-2 mb-4">
				<div class="flex items-center gap-3 text-sm">
					<span class="w-5 h-5 rounded bg-cyan-500/20 text-cyan-400 flex items-center justify-center text-xs font-bold">
						1
					</span>
					<span class="text-slate-300">
						Track oldest active transaction (
						<code class="text-cyan-400 text-xs">minStartTs</code>)
					</span>
				</div>
				<div class="flex items-center gap-3 text-sm">
					<span class="w-5 h-5 rounded bg-cyan-500/20 text-cyan-400 flex items-center justify-center text-xs font-bold">
						2
					</span>
					<span class="text-slate-300">For each version chain:</span>
				</div>
				<div class="ml-8 space-y-1 text-sm">
					<div class="flex items-center gap-2">
						<span class="text-emerald-400">•</span>
						<span class="text-slate-400">
							Keep versions where{" "}
							<code class="text-emerald-400 text-xs">
								commitTs &gt;= minStartTs
							</code>
						</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="text-red-400">•</span>
						<span class="text-slate-400">
							Delete older versions (no one can see them)
						</span>
					</div>
				</div>
			</div>

			{/* Triggers */}
			<div class="p-3 rounded-lg bg-slate-800/50 border border-slate-700 mb-4">
				<p class="text-xs text-slate-500 mb-2">Triggered:</p>
				<ul class="space-y-1 text-sm text-slate-400">
					<li>• After transaction commits</li>
					<li>• Periodically in background</li>
				</ul>
			</div>

			{/* Warning */}
			<div class="pt-3 border-t border-slate-700/50">
				<p class="text-xs text-amber-400">
					<span class="font-medium">Note:</span> Long-running transactions delay
					GC and hold memory.
				</p>
			</div>
		</div>
	);
}

// ============================================================================
// PAGE COMPONENT
// ============================================================================

export function MVCCPage() {
	return (
		<DocPage slug="internals/mvcc">
			<p>
				KiteDB supports concurrent transactions using{" "}
				<strong>Multi-Version Concurrency Control (MVCC)</strong>. Multiple
				readers can access the database simultaneously without blocking each
				other or writers.
			</p>

			<h2 id="isolation">Snapshot Isolation</h2>

			<p>
				Each transaction sees a consistent snapshot of the database as it
				existed when the transaction started. Other transactions' uncommitted
				changes are invisible.
			</p>

			<SnapshotIsolationTimeline />

			<h2 id="version-chains">Version Chains</h2>

			<p>
				When data is modified while readers exist, KiteDB keeps old versions in
				a chain:
			</p>

			<VersionChainDiagram />

			<h2 id="visibility">Visibility Rules</h2>

			<VisibilityRules />

			<h2 id="conflict-detection">Write Conflicts</h2>

			<p>
				KiteDB uses <strong>First-Committer-Wins</strong> to handle conflicts:
			</p>

			<WriteConflictDiagram />

			<CodeBlock
				code={`// Handling conflicts
try {
  await db.transaction(async () => {
    const alice = await db.get(user, 'alice');
    await db.update(user)
      .set({ age: alice.age + 1 })
      .where({ key: 'alice' });
  });
} catch (e) {
  if (e instanceof ConflictError) {
    // Another transaction modified alice
    // Retry with fresh data
  }
}`}
				language="typescript"
			/>

			<h2 id="lazy-versioning">Lazy Version Chains</h2>

			<p>
				Version chains are only created when necessary. If there are no
				concurrent readers, modifications happen in-place without versioning
				overhead.
			</p>

			<LazyMVCCDiagram />

			<h2 id="garbage-collection">Garbage Collection</h2>

			<p>Old versions are cleaned up when no transaction can see them:</p>

			<MVCCGarbageCollection />

			<h2 id="transaction-api">Transaction API</h2>

			<CodeBlock
				code={`// Explicit transaction
await db.transaction(async (ctx) => {
  const alice = await ctx.get(user, 'alice');
  await ctx.update(user).set({ age: alice.age + 1 }).where({ key: 'alice' });
  // Commits on successful return
  // Rolls back on exception
});

// Batch operations (single transaction)
await db.batch([
  db.insert(user).values({ key: 'bob', name: 'Bob' }),
  db.link(user, follows, user).from({ key: 'alice' }).to({ key: 'bob' }),
]);

// Without explicit transaction: each operation is auto-committed`}
				language="typescript"
			/>

			<h2 id="next">Next Steps</h2>
			<ul>
				<li>
					<a href="/docs/internals/wal">WAL & Durability</a> – How commits are
					made durable
				</li>
				<li>
					<a href="/docs/guides/transactions">Transactions Guide</a> – Practical
					usage patterns
				</li>
				<li>
					<a href="/docs/guides/concurrency">Concurrency Guide</a> –
					Multi-threaded access
				</li>
			</ul>
		</DocPage>
	);
}
