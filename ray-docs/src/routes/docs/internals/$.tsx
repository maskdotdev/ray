import { createFileRoute, useLocation } from "@tanstack/solid-router";
import { Show } from "solid-js";
import DocPage from "~/components/doc-page";
import { findDocBySlug } from "~/lib/docs";

// Import page components
import { ArchitecturePage } from "./-architecture";
import { CSRPage } from "./-csr";
import { KeyIndexPage } from "./-key-index";
import { MVCCPage } from "./-mvcc";
import { PerformancePage } from "./-performance";
import { SingleFilePage } from "./-single-file";
import { SnapshotDeltaPage } from "./-snapshot-delta";
import { WALPage } from "./-wal";

export const Route = createFileRoute("/docs/internals/$")({
	component: InternalsSplatPage,
});

function InternalsSplatPage() {
	const location = useLocation();
	const slug = () => {
		const path = location().pathname;
		const match = path.match(/^\/docs\/(.+)$/);
		return match ? match[1] : "";
	};
	const doc = () => findDocBySlug(slug());

	return (
		<Show when={doc()} fallback={<DocNotFound slug={slug()} />}>
			<DocPageContent slug={slug()} />
		</Show>
	);
}

function DocNotFound(props: { slug: string }) {
	return (
		<div class="max-w-4xl mx-auto px-6 py-12">
			<div class="text-center">
				<h1 class="text-4xl font-extrabold text-slate-900 dark:text-white mb-4">
					Page Not Found
				</h1>
				<p class="text-lg text-slate-600 dark:text-slate-400 mb-8">
					The internals page{" "}
					<code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">
						{props.slug}
					</code>{" "}
					doesn't exist yet.
				</p>
				<a
					href="/docs"
					class="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-cyan-500 to-violet-500 text-white font-semibold rounded-xl hover:shadow-lg hover:shadow-cyan-500/25 transition-all duration-200"
				>
					Back to Documentation
				</a>
			</div>
		</div>
	);
}

function DocPageContent(props: { slug: string }) {
	const slug = props.slug;

	// Route to the appropriate page component
	if (slug === "internals/architecture") {
		return <ArchitecturePage />;
	}

	if (slug === "internals/snapshot-delta") {
		return <SnapshotDeltaPage />;
	}

	if (slug === "internals/csr") {
		return <CSRPage />;
	}

	if (slug === "internals/single-file") {
		return <SingleFilePage />;
	}

	if (slug === "internals/wal") {
		return <WALPage />;
	}

	if (slug === "internals/mvcc") {
		return <MVCCPage />;
	}

	if (slug === "internals/key-index") {
		return <KeyIndexPage />;
	}

	if (slug === "internals/performance") {
		return <PerformancePage />;
	}

	// Default fallback
	return (
		<DocPage slug={slug}>
			<p>This internals documentation is coming soon.</p>
		</DocPage>
	);
}
