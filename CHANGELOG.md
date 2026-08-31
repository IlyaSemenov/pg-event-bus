# pg-event-bus

## 4.0.0

### Major Changes

- 5ec5b4b: Replace the single `onDeliveryGap` callback with `deliveryGaps()` stream.
  Publish `pg-event-bus` as ESM-only.

## 3.6.0

### Minor Changes

- 658a9f6: Add asynchronous disposal support to PostgreSQL and in-memory test event buses through `Symbol.asyncDispose`.

## 3.5.0

### Minor Changes

- c915575: Import `createTestEventBus` and its related types from `pg-event-bus/testing` instead of the package root.

## 3.4.0

### Minor Changes

- 54a2ade: Add `onDeliveryGap` to report possible notification delivery gaps after the PostgreSQL listener reconnects.

## 3.3.0

### Minor Changes

- f4e7241: Bind a typed event channel with `TestEventBus.for()` to inspect payloads for keys associated with a narrower compatible payload subtype.

## 3.2.0

### Minor Changes

- 8b45fc0: Inspect payloads published to a typed event channel and key with `TestEventBus.payloadsFor()` without depending on transport event names.

## 3.1.0

### Minor Changes

- d4a0722: Narrow `EventChannel.on<TEvent>()` to a compatible payload subtype when your application knows the event contract for a specific key.

## 3.0.0

### Major Changes

- 79fc814: Event channels now provide `sendMany()` for publishing multiple events in one database call.
  Configure `createPgEventBus()` with `publisher: createPublisher(query => ...)` to publish events through your current database connection.

## 2.0.0

### Major Changes

- c066ae8: Replace `PgEventBus` and `EventBusResource` with one managed `EventBus` contract.
  `eventBus.defineEventChannel()` has been removed; `createEventChannelFactory(eventBus)` returns a standalone `defineEventChannel` function.

### Minor Changes

- b0b35e2: Add `createTestEventBus()` for testing sends and subscriptions without PostgreSQL.

## 1.0.0

### Major Changes

- 97b8dcb: Initial release.
