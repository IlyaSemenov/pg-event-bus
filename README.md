# pg-event-bus

Typed realtime events over PostgreSQL `LISTEN` and `NOTIFY`.

Use this package to propagate cache invalidations and live updates between application processes.

The package owns a dedicated, reconnecting `LISTEN` connection, while your application controls the connection used to publish notifications, so events can be published inside your own transactions.

## Install

Node.js 22 or newer is required.

```sh
npm install pg-event-bus
```

## Create a bus

Pass a `connectionString` for the package's own listener connection, the PostgreSQL notification channel of your application, and a `publisher` that sends a notification batch through your database client or ORM in one call.

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
Every process of that application must pass the same name, and a different application sharing the database must pass another one.

`createPublisher()` passes its callback a parameterized `pg_notify` query as `{ text, values }`, which the callback executes through the application's current database connection.
If your database client uses a different query API, build a custom adapter with [createRawPublisher()](#custom-adapters).

Creating a bus immediately starts its dedicated listener, which executes `LISTEN` on that channel.

## Define an event channel

An event channel is a typed layer above the bus.
It maps an application key to an event name, and all its events share one payload type.

Define as many event channels on a bus as you need; they all travel through the single PostgreSQL channel of their bus.

Create a channel factory bound to the bus:

```ts
import { createEventChannelFactory } from "pg-event-bus"

const defineEventChannel = createEventChannelFactory(eventBus)
```

The following channel uses a post ID as its key and accepts one payload type.

```ts
interface CommentEvent {
  eventType: "created" | "updated" | "deleted"
  commentId: string
}

export const commentEvents = defineEventChannel<CommentEvent>(
  postId => `post:${postId}:comment`,
)
```

Event keys are strings by default.
Pass a second generic argument only when a channel uses another key type.

```ts
defineEventChannel<ChatEvent, SessionKey>(
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

Call `sendMany()` to publish several events in one database call.

```ts
await commentEvents.sendMany(
  comments.map(comment => ({
    key: comment.postId,
    payload: {
      eventType: "created",
      commentId: comment.id,
    },
  })),
)
```

Call `on()` to consume matching events as an `AsyncIterable`.

```ts
for await (const event of commentEvents.on(postId, signal)) {
  console.log(event.commentId)
}
```

Pass an `AbortSignal` to stop the stream when its consumer disconnects.

When a channel has a union payload and the application contract associates a key with one of its subtypes, pass that subtype explicitly to `on()`.

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

export const commentEvents = defineEventChannel<CommentEvent>(
  postId => `post:${postId}:comment`,
)
```

### Test override

Tests can replace the dependency without recreating domain channels that were declared when their modules loaded.

```ts
import { createTestEventBus } from "pg-event-bus"
import { provide, withOverrides } from "ripple-di"

const testEventBus = createTestEventBus()

afterEach(() => testEventBus.clearCalls())

test("publishes a comment event through the override", async () => {
  await withOverrides(
    provide(useEventBus, testEventBus),
    () => commentEvents.send("post-id", {
      eventType: "created",
      commentId: "comment-id",
    }),
  )

  expect(testEventBus.calls).toEqual([
    {
      event: "post:post-id:comment",
      payload: {
        eventType: "created",
        commentId: "comment-id",
      },
    },
  ])

  expect(testEventBus.payloadsFor(commentEvents, "post-id")).toEqual([
    {
      eventType: "created",
      commentId: "comment-id",
    },
  ])
})
```

The in-memory test bus:

- Records successful individual and batch sends in `calls`.
- Returns typed payload snapshots for a channel and key through `payloadsFor()`.
- Resolves `ready` immediately.
- Delivers sends to active subscribers and honors their abort signals.
- Clears only the recorded history with `clearCalls()`.
- Reports active subscriptions through `getActiveSubscriptionCount()`.
- Completes all active streams when closed.

When the application contract associates a key with a narrower payload subtype, bind the channel first and pass that subtype explicitly.

```ts
const createdEvents = testEventBus
  .for(activityEvents)
  .payloadsFor<CommentCreatedEvent>("created")
```

The factory calls `useEventBus()` when `send()`, `sendMany()`, or `on()` runs, not when `commentEvents` is declared, so the operation uses the scoped test binding.
