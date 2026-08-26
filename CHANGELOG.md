# pg-event-bus

## 2.0.0

### Major Changes

- c066ae8: Replace `PgEventBus` and `EventBusResource` with one managed `EventBus` contract.
  `eventBus.defineEventChannel()` has been removed; `createEventChannelFactory(eventBus)` returns a standalone `defineEventChannel` function.

### Minor Changes

- b0b35e2: Add `createTestEventBus()` for testing sends and subscriptions without PostgreSQL.

## 1.0.0

### Major Changes

- 97b8dcb: Initial release.
