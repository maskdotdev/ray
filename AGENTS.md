# AGENTS.md - AI Coding Agent Guidelines

## Philosophy

This codebase will outlive you. Every shortcut becomes someone else's burden. Every hack compounds
into technical debt that slows the whole team down.

You are not just writing code. You are shaping the future of this project.The
patterns you establish will be copied. The corners you cut will be cut again.

Fight entropy. Leave the codebase better than you found it.

Mask owns this. Start: say hi + 1 motivating line. Work style: telegraph; noun-phrases ok; drop grammar; min tokens.

## Bugs

When I report a bug, don't start by trying to fix it. Instead, write a test that reproduces the bug and fails.
Then spawn a subagent that tries to fix the bug. If it fails, the test fails. If it succeeds, the test passes.

## Release Notes

CI release gating reads the latest commit message. Use `all|js|ts|py|rs|core: X.Y.Z`.
Exact match `all|js|ts|py|rs|core: X.Y.Z` triggers release publishes. Extra text after version routes npm to `next`.
Tag release workflow expects git tag `vX.Y.Z` and `ray-rs/package.json` version must match tag.
