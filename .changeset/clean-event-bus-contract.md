---
"pg-event-bus": major
---

Replace `PgEventBus` and `EventBusResource` with one managed `EventBus` contract.
`eventBus.defineEventChannel()` has been removed; `createEventChannelFactory(eventBus)` returns a standalone `defineEventChannel` function.
