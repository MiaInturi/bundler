# @asyncapi/bundler (fork notes)

This package is maintained in a fork of [asyncapi/bundler](https://github.com/asyncapi/bundler).

For full installation instructions, API surface, and general usage examples, see the upstream [README](https://github.com/asyncapi/bundler#readme).

This document only covers behavior added in this fork.

## Fork-specific additions

This fork extends `bundle()` with schema hoisting and reference normalization implemented in [`src/hoistBundledSchemas.ts`](src/hoistBundledSchemas.ts).

After bundling, output is normalized for production `AsyncAPI` workflows:

- External and reusable schemas are hoisted into `#/components/schemas/...`.
- External schema `$ref` values are rewritten to local component schema refs.
- Deep and composed schema structures are normalized consistently (`allOf`, `oneOf`, `anyOf`, nested properties, array items, and related schema keywords).
- External operation channel refs are rewritten to local refs (`#/channels/...` or `#/components/channels/...` when resolvable).
- `discriminator.mapping` file values are converted to `#/components/schemas/...` refs.
- Equivalent schemas are aliased to canonical component names to reduce duplicates.
- Top-level and nested `x-origin` metadata is used internally for stable naming and rewrites.

## Parser behavior in this fork

- Circular dereference is enabled for AsyncAPI v2 and v3 parser flows (`src/parser.ts`).

## Scope of impact

- These changes affect bundled output produced by `bundle()` and its post-processing normalization steps.
- Upstream documentation remains the source of truth for baseline package usage and API details.
- `x-origin` metadata is stripped from the final output unless `xOrigin: true` is explicitly set.
