# pg-event-bus

Typed realtime events over PostgreSQL `LISTEN` and `NOTIFY`.

Use this package to propagate cache invalidations and live updates between application processes.

The package owns a dedicated, reconnecting `LISTEN` connection, while your application controls the connection used to publish notifications, so events can be published inside your own transactions.

## Install

Node.js 22 or newer is required.
The package is ESM-only.

```sh
npm install pg-event-bus
```

## Create a bus

Create the bus with a `connectionString` for its listener connection, the PostgreSQL notification channel of your application, and a `publisher` that sends a notification batch through your database client or ORM in one call.

```ts
import { createPgEventBus, createPublisher } from "pg-event-bus"

// Use your application's current database connection.
import { db } from "#db"

const eventBus = createPgEventBus({
  connectionString: databaseUrl,
  // Raw PostgreSQL channel name, used for both LISTEN and NOTIFY.
  channel: "my-app",
  publisher: createPublisher(query => db.query(query)),
})
```

Use one channel name per application, such as the application's own name.
Every process of that application must use the same name, and a different application sharing the database must use another one.

`createPublisher()` passes its callback a parameterized `pg_notify` query as `{ text, values }`, which the callback executes through the application's current database connection.
If your database client uses a different query API, build a custom adapter with [createRawPublisher()](#custom-adapters).

Creating a bus immediately starts its dedicated listener, which executes `LISTEN` on that channel.

## Define an event channel

An event channel is a typed event stream that application code can publish to and subscribe to.
Each channel has a name and a payload type.
All event channels defined for a bus travel through its single PostgreSQL notification channel.

First create a channel factory for the bus:

```ts
import { createEventChannelFactory } from "pg-event-bus"

const defineEventChannel = createEventChannelFactory(eventBus)
```

Define a channel with its name and payload type:

```ts
interface LogEvent {
  message: string
}

export const logEvents = defineEventChannel<LogEvent>("log")
```

`logEvents` keeps the `"log"` name and the `LogEvent` payload contract together, so the rest of the application uses the channel without repeating either one.

A keyed event channel is a family of related subchannels that share one payload type.
For example, every post can have its own comment event stream while all comment events share one payload type.

Define such a keyed channel with a function that maps each key to the name used by the bus:

```ts
interface CommentEvent {
  eventType: "created" | "updated" | "deleted"
  commentId: string
}

export const commentEvents = defineEventChannel<CommentEvent>(
  postId => `post:${postId}:comment`,
)
```

`commentEvents` represents the whole family, and a post ID selects one subchannel when the application publishes or subscribes.

Event keys are strings by default.
Supply the second generic argument when a channel uses another key type.

```ts
defineEventChannel<ChatEvent, SessionKey>(
  session => `chat:${session.userId}:${session.scope}`,
)
```

## Send and receive events

Publish one payload with `send()`:

```ts
await logEvents.send({
  message: "Comment created",
})
```

Publish several payloads in one database call with `sendMany()`:

```ts
await logEvents.sendMany([
  { message: "First comment created" },
  { message: "Second comment created" },
])
```

Subscribe with `on()`, which returns an `AsyncIterable` of channel payloads:

```ts
for await (const event of logEvents.on(signal)) {
  console.log(event.message)
}
```

Use an `AbortSignal` to stop the subscription when its consumer disconnects.

For a keyed channel, `send()` and `on()` take the key of the selected subchannel.
Every `sendMany()` item carries its own key because one batch can target several subchannels.

```ts
await commentEvents.send(postId, {
  eventType: "created",
  commentId,
})

await commentEvents.sendMany(
  comments.map(comment => ({
    key: comment.postId,
    payload: {
      eventType: "created",
      commentId: comment.id,
    },
  })),
)

for await (const event of commentEvents.on(postId, signal)) {
  console.log(event.commentId)
}
```

When a keyed channel has a union payload and the application contract associates a key with one of its subtypes, supply that subtype explicitly to `on()`.

```ts
type ActivityEvent = CommentCreatedEvent | CommentDeletedEvent

const activityEvents = defineEventChannel<ActivityEvent>(
  scope => `activity:${scope}`,
)

for await (const event of activityEvents.on<CommentCreatedEvent>("created")) {
  console.log(event.commentId)
}
```

The subtype must extend the channel payload type, and the key-to-subtype relationship is trusted rather than checked at runtime.

## Transactions

Always await `send()` and `sendMany()`.

Send inside a transaction to tie the notification or notification batch to it.
PostgreSQL then delivers the notification after commit and discards it on rollback.

This requires the `publisher` to run every `pg_notify` on the transaction's own connection.
Clients that route every query through the current transaction do that for you.
Clients that expose the transaction as a separate handle need the `publisher` to pick that handle up, for example from an async-local context.

For example, [`orchid-orm`](https://www.npmjs.com/package/orchid-orm) runs every query of a `$transaction` callback in that transaction, the publisher's `pg_notify` included:

```ts
await db.$transaction(async () => {
  const commentId = await db.comment.insert({ postId, text }).get("id")
  await commentEvents.send(postId, { eventType: "created", commentId })
})
```

## Listener readiness

The `ready` promise resolves after the listener has connected and executed `LISTEN`.

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

The PostgreSQL and in-memory test buses also implement the asynchronous disposal protocol, so integrations can use `Symbol.asyncDispose` instead of calling `close()` directly.

## Custom adapters

An adapter that cannot execute the `{ text, values }` query produced by `createPublisher()` can use `createRawPublisher()` as an escape hatch.

```ts
import { createRawPublisher } from "pg-event-bus"
import { db } from "#db"

export const publisher = createRawPublisher(notifications =>
  db.executeSql({
    raw: `
      SELECT pg_notify(
        notification.value->>'channel',
        notification.value->>'payload'
      )
      FROM jsonb_array_elements($notifications::jsonb)
      WITH ORDINALITY AS notification(value, position)
      ORDER BY notification.position
    `,
    values: {
      notifications: JSON.stringify(notifications),
    },
  }),
)
```

## Delivery semantics

This package provides best-effort realtime signaling, not a durable queue.
Disconnected listeners miss events, reconnects do not replay history, and delivery is not exactly once.

Use an outbox, job queue, or durable broker when every event must be processed.

Payloads use JSON serialization and must be JSON-serializable.
Keep them small: PostgreSQL notification payloads must be shorter than 8000 bytes with the default configuration, and the encoded event name counts toward that limit.

PostgreSQL can also coalesce identical channel-and-payload notifications emitted within one transaction.

### Recover from possible delivery gaps

Consume `deliveryGaps()` when the application needs to restore derived state after a listener reconnect because notifications might have been lost while it was disconnected.
Each active stream receives a signal after every repeated successful `LISTEN`, when the bus is ready to receive notifications again.
The stream does not yield after the initial `LISTEN` and completes during a normal `close()`.
An error in one consumer does not affect listener reconnection or other consumers.

```ts
for await (const _ of eventBus.deliveryGaps(signal)) {
  await invalidateRealtimeViews()
}
```

## Dependency injection

Skip this section if your application uses the concrete `eventBus` directly.

With dependency injection, domain modules usually outlive a particular event bus binding.
Create their channel factory from a function that resolves the current `EventBus` instead of binding channels to one instance.

The following example uses [`ripple-di`](https://www.npmjs.com/package/ripple-di), but the same resolver pattern works with other DI containers.

### Application wiring

Register the production PostgreSQL bus and create the shared `defineEventChannel` function from its resolver.

```ts
import { createEventChannelFactory, createPgEventBus } from "pg-event-bus"
import { defineDependency } from "ripple-di"
import { publisher } from "#events/publisher"

export const useEventBus = defineDependency(
  () => createPgEventBus({
    connectionString: databaseUrl,
    channel: "my-app",
    publisher,
  }),
  {
    dispose: eventBus => eventBus.close(),
  },
)

export const defineEventChannel = createEventChannelFactory(useEventBus)
```

The transport-independent `EventBus` contract lets tests replace the complete dependency, including its startup lifecycle.

### Domain channel

Domain modules import the shared factory instead of the concrete PostgreSQL bus.

```ts
import { defineEventChannel } from "#events"

export const logEvents = defineEventChannel<LogEvent>("log")

export const commentEvents = defineEventChannel<CommentEvent>(
  postId => `post:${postId}:comment`,
)
```

### Testing with TestEventBus

Create an in-memory event bus and provide it as the dependency override for each test.

```ts
import { createTestEventBus } from "pg-event-bus/testing"
import { provide, withOverrides } from "ripple-di"

const testEventBus = createTestEventBus()

afterEach(() => testEventBus.clearCalls())
```

Run the operation inside `withOverrides()` to provide the test bus.
The channel factory resolves `useEventBus()` when `send()` runs, so `logEvents`, although declared earlier, uses the active override.
After the operation, pass `logEvents` to `payloadsFor()` to inspect its recorded payloads; no separate channel name is required.

```ts
test("publishes a log event through the override", async () => {
  await withOverrides(
    provide(useEventBus, testEventBus),
    () => logEvents.send({ message: "Comment created" }),
  )

  expect(testEventBus.payloadsFor(logEvents)).toEqual([
    { message: "Comment created" },
  ])
})
```

For a keyed channel, `payloadsFor()` additionally takes the selected subchannel key.

```ts
test("publishes a comment event through the override", async () => {
  await withOverrides(
    provide(useEventBus, testEventBus),
    () => commentEvents.send("post-id", {
      eventType: "created",
      commentId: "comment-id",
    }),
  )

  expect(testEventBus.payloadsFor(commentEvents, "post-id")).toEqual([
    {
      eventType: "created",
      commentId: "comment-id",
    },
  ])
})
```

`payloadsFor()` normally returns the channel's full payload type.
When a particular key represents only one member of a union payload, a test may need a narrower result type.
Bind the channel with `for()`, then supply the expected subtype to `payloadsFor()`.
This two-step form keeps the channel payload and key types inferred while checking that the requested subtype belongs to the channel payload.

```ts
const createdEvents = testEventBus
  .for(activityEvents)
  .payloadsFor<CommentCreatedEvent>("created")
```

The in-memory test bus also:

- Records successful individual and batch sends in `calls`, while `clearCalls()` clears only that history.
- Resolves `ready` immediately.
- Delivers sends to active subscribers, honors their abort signals, and reports their count through `getActiveSubscriptionCount()`.
- Produces delivery gap signals through `simulateDeliveryGap()` and `deliveryGaps()`.
- Completes all active streams when closed.
