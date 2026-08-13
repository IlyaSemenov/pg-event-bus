# pg-event-bus

Typed realtime events over PostgreSQL `LISTEN` and `NOTIFY`.

Use this package to propagate cache invalidations and live updates between application processes.

The package owns a dedicated, reconnecting `LISTEN` connection while your application controls the connection used to publish notifications.

## Install

Node.js 22 or newer is required.

```sh
npm install pg-event-bus
```

## Create a bus

Creating a bus immediately starts its dedicated PostgreSQL listener.

Provide a `publish` function that executes `pg_notify` through your application's database client or ORM.

```ts
import { createPgEventBus } from "pg-event-bus"

// Use your application's current database connection.
import { db } from "#db"

const eventBus = createPgEventBus({
  connectionString: databaseUrl,
  channel: "my-app",
  async publish({ channel, payload }) {
    await db.sql`SELECT pg_notify(${channel}, ${payload})`
  },
})
```

## Define an event channel

An event channel maps a typed application key to an internal event name.

The following channel uses a post ID as its key and accepts one payload type.

```ts
interface CommentEvent {
  eventType: "created" | "updated" | "deleted"
  commentId: string
}

export const commentEvents = eventBus.defineEventChannel<CommentEvent>(
  postId => `post:${postId}:comment`,
)
```

Event keys are strings by default.
Pass a second generic argument only when a channel uses another key type.

```ts
eventBus.defineEventChannel<ChatEvent, SessionKey>(
  session => `chat:${session.userId}:${session.scope}`,
)
```

## Send and receive events

Call `send()` to publish an event.

```ts
await commentEvents.send(postId, {
  eventType: "created",
  commentId,
})
```

Call `on()` to consume matching events as an `AsyncIterable`.

```ts
for await (const event of commentEvents.on(postId, signal)) {
  console.log(event.commentId)
}
```

Pass an `AbortSignal` to stop the stream when its consumer disconnects.

## Listener readiness

Creating a bus starts its dedicated listener in the background.

`eventBus.ready` resolves after the listener has connected and executed `LISTEN`.
Sending does not depend on this promise because `send()` uses the injected `publish` function.

Await it during startup only when a receiving process must not report itself as ready before it can receive notifications.
Notifications published before `LISTEN` becomes active can be missed.

```ts
await eventBus.ready
```

## Shutdown

Call `close()` during application shutdown to stop the PostgreSQL listener and complete active event streams.

```ts
await eventBus.close()
```

## Transactions

`send()` waits for the injected `publish` function, so always await it.

To make notification delivery depend on a transaction, call `send()` through a publisher that uses that transaction's connection.

```ts
await db.transaction(async () => {
  await updateComment()
  await commentEvents.send(postId, event)
})
```

PostgreSQL delivers the notification after commit and discards it on rollback.

## Dependency injection

Skip this section if your application uses the concrete `eventBus` directly.

With dependency injection, domain modules usually outlive a particular event bus binding.
Create their channel factory from a function that resolves the current `EventBus` instead of binding channels to one instance.

The following example uses [`ripple-di`](https://www.npmjs.com/package/ripple-di), but the same resolver pattern works with another DI container.

Install it separately to follow this example.

```sh
npm install ripple-di
```

### Application wiring

Register the production PostgreSQL bus and create the shared `defineEventChannel` function from its resolver.

```ts
import {
  createEventChannelFactory,
  createPgEventBus,
} from "pg-event-bus"
import { defineDependency } from "ripple-di"

export const useEventBus = defineDependency(
  () => createPgEventBus({
    connectionString: databaseUrl,
    channel: "my-app",
    async publish({ channel, payload }) {
      await db.sql`SELECT pg_notify(${channel}, ${payload})`
    },
  }),
  {
    dispose: eventBus => eventBus.close(),
  },
)

export const defineEventChannel = createEventChannelFactory(useEventBus)
```

### Domain channel

Domain modules import the shared factory instead of the concrete PostgreSQL bus.

```ts
import { defineEventChannel } from "#events"

export const commentEvents = defineEventChannel<CommentEvent>(
  postId => `post:${postId}:comment`,
)
```

### Test override

Tests can replace the dependency without recreating domain channels that were declared when their modules loaded.

```ts
import { provide, withOverrides } from "ripple-di"
import type { EventBus } from "pg-event-bus"

const sent: Array<{ event: string; payload: unknown }> = []

const testEventBus: EventBus = {
  async send(event, payload) {
    sent.push({ event, payload })
  },
  async *on() {},
}

await withOverrides(
  provide(useEventBus, testEventBus),
  () => commentEvents.send("post-id", {
    eventType: "created",
    commentId: "comment-id",
  }),
)
```

`createEventChannelFactory` calls `useEventBus()` when `send()` or `on()` runs, not when `commentEvents` is declared.
The operation therefore uses the scoped test binding installed by `withOverrides`.

## Delivery semantics

This package provides best-effort realtime signaling, not a durable queue.
Disconnected listeners miss events, reconnects do not replay history, and delivery is not exactly once.

Use an outbox, job queue, or durable broker when every event must be processed.

Payloads use JSON serialization and must be JSON-serializable.
Keep them small because PostgreSQL notification payloads must be shorter than 8000 bytes with the default configuration.

PostgreSQL can also coalesce identical channel-and-payload notifications emitted within one transaction.
