# pg-event-bus Agent Guide

## Overview

PostgreSQL event bus.

Read [README.md](README.md) completely before changing the public API, package behavior, supported runtimes, or user documentation.

Extend this guide only with stable, non-obvious conventions, architecture, contracts, workflows, and gotchas.
Do not catalog files or restate information evident from their names and locations.

## Scope

- Keep production code in `src/`.
- Use `src/*.test.ts` only for focused tests of one source module.
- Keep integration, package-boundary, and type-inference tests in `tests/`.
- Keep `src/index.ts` as a barrel that exports only public modules.
- Treat `package.json` exports and supported runtimes as public contracts.
- Publish ESM output only.

## Architecture

Listener and publisher ownership is asymmetric.
The package owns the dedicated `LISTEN` connection, while applications inject publication through their current database connection.
Postgres.js manages the listener connection and reconnection lifecycle.
The package is ORM-agnostic and contains no ORM-specific adapters.
`createPublisher` builds one parameterized `pg_notify` query and delegates its execution to the application's current database connection.
`createRawPublisher` delegates the readonly notification array instead, including when `send()` publishes one event.

Each `deliveryGaps()` call creates an independent stream that receives a signal after every successful listener reconnection, but not after the initial `LISTEN`.
Delivery gap streams honor their consumer abort signal and complete when the bus closes.
Consumer failures must not affect listener reconnection or other delivery gap streams.

When `createEventChannelFactory` receives a resolver, it resolves its `EventBus` when `send()`, `sendMany()`, or `on()` runs.
Channels declared at module load therefore observe dependency-injection overrides active during an operation.
When it receives an `EventBus` directly, channels remain bound to that instance.
Keep each channel's event-name resolver in library-private metadata so test instrumentation can resolve logical channel keys without exposing transport names.

## Documentation

- Write public README and JSDoc text for package users who do not know the implementation.
- Add JSDoc to every non-trivial public export and public member.
- Do not document obvious or implied defaults.
- Describe a default only when readers need it to make a decision or avoid surprising behavior.
- Use One Sentence Per Line for connected prose.
- Keep semantically connected explanations as prose paragraphs.
- Use lists for separate assertions instead of presenting them as prose paragraphs.

## Changesets

- Add one `.changeset/*.md` file for each independently releasable user-visible change.
- Do not add changesets for internal refactors, maintenance, tests, or documentation changes that do not require a package release.
- Choose the SemVer bump from the public contract: `patch` for backward-compatible fixes, `minor` for backward-compatible functionality, and `major` for breaking changes.
- Create `.changeset/<unique-name>.md` with this format:

```markdown
---
"pg-event-bus": patch
---

Describe the user-visible change.
```

- Write one or two sentences for package users that describe the observable change or new capability without implementation details or rationale.
- Do not edit the package version or `CHANGELOG.md` by hand, and do not run `changeset version` or `changeset publish`; the release workflow consumes pending changesets.

## Checks

- Run the `types` script when public types or TypeScript configuration change.
- Run the `test` script when behavior changes.
- Run the `build` script when package exports, declarations, or supported runtimes change.
