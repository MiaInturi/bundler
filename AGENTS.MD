# AGENTS

## Purpose

This repository is a fork of `asyncapi/bundler`.
Most fork-specific behavior currently lives in:

- `src/hoistBundledSchemas.ts`
- `src/index.ts`
- `src/parser.ts`

Primary goal: keep fork delta intentional, test-backed, and easy to rebase on upstream.

## Scope Priorities

1. Prefer changes in `src/hoistBundledSchemas.ts` for bundling normalization work.
2. In `src/index.ts`, keep `bundle(files, options)` behavior stable unless explicitly requested.
3. In `src/parser.ts`, keep AsyncAPI v2/v3 dereference behavior stable, including circular dereference and `x-origin` handling.
4. Keep fork-specific documentation in `README.md` focused only on fork behavior.

## Design Rules For AI-Generated Code

- Keep functions focused and deterministic (stable naming, stable traversal order, collision-safe schema naming).
- Rewrite only schema-context external refs into `#/components/schemas/...`.
- Do not rewrite non-schema refs or unrelated document/content refs.
- Preserve non-schema metadata and extension properties unless a task explicitly requires changes.
- Keep `x-origin` behavior intact (`xOrigin: true` preserves; default output strips via `stripXOrigin`).
- Add comments only when the "why" is non-obvious.

## Testing Rules (Required)

For each change to normalization/rewriting behavior:

1. Add or update integration coverage in `tests/specs/bundling/bundling.spec.ts` for bundling normalization behavior.
2. Prefer focused bundling specs in `tests/specs/bundling/bundling.spec.ts` and keep one behavior per test.
3. Add or update fixtures under `tests/specs/bundling/<functionality>/` (for example `mapping`, `hoisting`, `channel-refs`).
4. Prefer compact, readable fixture schema names (for example `Pet`, `Owner`, `Animal`) over noisy domain names.
5. Assert both behavior and safety:
   - schema refs in schema contexts resolve to `#/components/schemas/...` where expected;
   - operation channel refs resolve to local refs where possible (`#/channels/...` or `#/components/channels/...`);
   - non-schema refs/metadata are preserved;
   - output remains serializable (`JSON.stringify(...)` does not throw).

## Validation Commands

Run from repo root:

- Targeted integration test:
  - `npx jest tests/specs/bundling/bundling.spec.ts`
- Full test suite:
  - `npm test`
- Lint:
  - `npm run lint`
- Build:
  - `npm run build`

## PR Checklist

- [ ] Fork-specific behavior is isolated and intentional
- [ ] `bundle()` API behavior is not unintentionally changed
- [ ] Tests + fixtures cover new logic and safety behavior
- [ ] `README.md` documents fork-specific behavior only
- [ ] `npm test` and `npm run lint` pass
