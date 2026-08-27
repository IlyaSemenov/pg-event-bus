---
"pg-event-bus": major
---

Event channels now provide `sendMany()` for publishing multiple events in one database call.
Configure `createPgEventBus()` with `publisher: createPublisher(query => ...)` to publish events through your current database connection.
