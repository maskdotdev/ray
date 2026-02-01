import type { JSX } from "solid-js";

// ============================================================================
// SHARED TEXT/CODE COMPONENTS
// ============================================================================

// Inline code component for data flow
export function Code(props: { children: JSX.Element; color?: string }) {
	const colorClass = () => {
		switch (props.color) {
			case "cyan":
				return "text-cyan-300/80";
			case "violet":
				return "text-violet-300/80";
			case "emerald":
				return "text-emerald-300/80";
			default:
				return "text-cyan-300/80";
		}
	};
	return (
		<code class={`${colorClass()} text-xs px-1 bg-slate-700/50 rounded`}>
			{props.children}
		</code>
	);
}

// Bold label for storage layer items
export function Label(props: { children: JSX.Element; color?: string }) {
	const colorClass = () => {
		switch (props.color) {
			case "emerald":
				return "text-emerald-400";
			case "violet":
				return "text-violet-400";
			default:
				return "text-cyan-400";
		}
	};
	return <strong class={colorClass()}>{props.children}</strong>;
}

// ============================================================================
// FLOW DIAGRAM COMPONENTS
// ============================================================================

// Flow item row
export function FlowItem(props: {
	isLast?: boolean;
	color: string;
	children: JSX.Element;
}) {
	const connectorColor = () => {
		switch (props.color) {
			case "cyan":
				return "text-cyan-500/60";
			case "violet":
				return "text-violet-500/60";
			case "emerald":
				return "text-emerald-500/60";
			default:
				return "text-slate-500/60";
		}
	};
	return (
		<div class="flex items-start gap-2 text-sm text-slate-300">
			<span class={`mt-0.5 ${connectorColor()}`}>
				{props.isLast ? "└" : "├"}
			</span>
			<span>{props.children}</span>
		</div>
	);
}

// Data flow step card
export function FlowStep(props: {
	number: string;
	title: string;
	color: string;
	children: JSX.Element;
}) {
	const borderColor = () => {
		switch (props.color) {
			case "cyan":
				return "border-cyan-500/30";
			case "violet":
				return "border-violet-500/30";
			case "emerald":
				return "border-emerald-500/30";
			default:
				return "border-slate-500/30";
		}
	};
	const badgeColor = () => {
		switch (props.color) {
			case "cyan":
				return "bg-cyan-500/20 text-cyan-400";
			case "violet":
				return "bg-violet-500/20 text-violet-400";
			case "emerald":
				return "bg-emerald-500/20 text-emerald-400";
			default:
				return "bg-slate-500/20 text-slate-400";
		}
	};
	const titleColor = () => {
		switch (props.color) {
			case "cyan":
				return "text-cyan-400";
			case "violet":
				return "text-violet-400";
			case "emerald":
				return "text-emerald-400";
			default:
				return "text-slate-400";
		}
	};
	return (
		<div
			class={`rounded-xl border ${borderColor()} bg-gradient-to-br from-slate-900 to-slate-800 p-4 shadow-lg`}
		>
			<div class="flex items-center gap-3 mb-3">
				<span
					class={`flex items-center justify-center w-7 h-7 rounded-lg text-sm font-bold ${badgeColor()}`}
				>
					{props.number}
				</span>
				<h4 class={`font-semibold ${titleColor()}`}>{props.title}</h4>
			</div>
			<div class="space-y-2 pl-2">{props.children}</div>
		</div>
	);
}

// Arrow connector between steps
export function FlowArrow() {
	return (
		<div class="flex justify-center">
			<svg
				class="w-5 h-5 text-slate-500"
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
	);
}

// ============================================================================
// BADGE COMPONENTS
// ============================================================================

export function RecordTypeBadge(props: { name: string; color: string }) {
	const colorClass = () => {
		switch (props.color) {
			case "cyan":
				return "bg-cyan-500/20 text-cyan-400 border-cyan-500/30";
			case "emerald":
				return "bg-emerald-500/20 text-emerald-400 border-emerald-500/30";
			case "red":
				return "bg-red-500/20 text-red-400 border-red-500/30";
			default:
				return "bg-violet-500/20 text-violet-400 border-violet-500/30";
		}
	};
	return (
		<span
			class={`px-2 py-0.5 rounded border text-xs font-mono ${colorClass()}`}
		>
			{props.name}
		</span>
	);
}

// ============================================================================
// STEP COMPONENTS
// ============================================================================

// Checkpoint step component
export function CheckpointStep(props: { num: number; text: string }) {
	const colorClass = () => {
		if (props.num <= 2) return "bg-cyan-500/20 text-cyan-400";
		if (props.num <= 4) return "bg-violet-500/20 text-violet-400";
		return "bg-emerald-500/20 text-emerald-400";
	};
	return (
		<div class="flex items-center gap-3 relative">
			<span
				class={`flex items-center justify-center w-6 h-6 rounded-full text-xs font-bold shrink-0 z-10 ${colorClass()}`}
			>
				{props.num}
			</span>
			<span class="text-slate-300 text-sm">{props.text}</span>
		</div>
	);
}
