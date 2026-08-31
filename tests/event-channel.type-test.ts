import {
  createEventChannelFactory,
  type EventBus,
  type EventChannel,
  type KeyedEventChannel,
} from "pg-event-bus"

interface CommentEvent {
  commentId: string
}

interface ChatEvent {
  messageId: string
}

interface SessionKey {
  userId: string
  scope: string
}

interface CreatedActivity {
  kind: "created"
  commentId: string
}

interface DeletedActivity {
  kind: "deleted"
  commentId: string
}

type ActivityEvent = CreatedActivity | DeletedActivity

declare const eventBus: EventBus
const defineEventChannel = createEventChannelFactory(eventBus)

const auditEvents = defineEventChannel<ActivityEvent>("audit")
const commentEvents = defineEventChannel<CommentEvent>(
  (postId) => `post:${postId}:comment`,
)
const chatEvents = defineEventChannel<ChatEvent, SessionKey>(
  (session) => `chat:${session.userId}:${session.scope}`,
)
const activityEvents = defineEventChannel<ActivityEvent>(
  (scope) => `activity:${scope}`,
)
const eventChannel: EventChannel<ActivityEvent> = auditEvents
const keyedEventChannel: KeyedEventChannel<CommentEvent> = commentEvents

auditEvents.send({ kind: "created", commentId: "comment-id" })
auditEvents.sendMany([
  { kind: "created", commentId: "first-comment" },
  { kind: "deleted", commentId: "second-comment" },
])
const auditStream: AsyncGenerator<ActivityEvent, void, unknown> =
  auditEvents.on()
const auditSignalStream: AsyncGenerator<ActivityEvent, void, unknown> =
  auditEvents.on(new AbortController().signal)
const createdAuditStream: AsyncGenerator<CreatedActivity, void, unknown> =
  auditEvents.on<CreatedActivity>()

void auditStream
void auditSignalStream
void createdAuditStream
void eventChannel
void keyedEventChannel

commentEvents.send("post-id", { commentId: "comment-id" })
chatEvents.send(
  { userId: "user-id", scope: "support" },
  { messageId: "message-id" },
)
commentEvents.sendMany([
  { key: "first-post", payload: { commentId: "first-comment" } },
  { key: "second-post", payload: { commentId: "second-comment" } },
])
chatEvents.sendMany([
  {
    key: { userId: "user-id", scope: "support" },
    payload: { messageId: "message-id" },
  },
])

const activityStream: AsyncGenerator<ActivityEvent, void, unknown> =
  activityEvents.on("all")
const createdActivityStream: AsyncGenerator<CreatedActivity, void, unknown> =
  activityEvents.on<CreatedActivity>("created")

void activityStream
void createdActivityStream

// @ts-expect-error Explicit narrowing must remain within the channel payload type.
activityEvents.on<{ kind: "unrelated" }>("created")

// @ts-expect-error A fixed-name channel does not accept a key.
auditEvents.send("audit", { kind: "created", commentId: "comment-id" })

auditEvents.sendMany([
  // @ts-expect-error A fixed-name batch contains payloads rather than keyed events.
  { key: "audit", payload: { kind: "created", commentId: "comment-id" } },
])

// @ts-expect-error A fixed-name channel accepts an AbortSignal, not a key.
auditEvents.on("audit")

// @ts-expect-error A keyed channel requires a key when sending.
commentEvents.send({ commentId: "comment-id" })

// @ts-expect-error A keyed channel requires a key when subscribing.
commentEvents.on()

// @ts-expect-error String is the default key type.
commentEvents.send({ postId: "post-id" }, { commentId: "comment-id" })

// @ts-expect-error The payload must match the channel payload type.
commentEvents.send("post-id", { messageId: "message-id" })

commentEvents.sendMany([
  // @ts-expect-error Batch keys must match the channel key type.
  { key: { postId: "post-id" }, payload: { commentId: "comment-id" } },
])

commentEvents.sendMany([
  // @ts-expect-error Batch payloads must match the channel payload type.
  { key: "post-id", payload: { messageId: "message-id" } },
])

// @ts-expect-error A channel with a composite key does not accept a string key.
chatEvents.on("user-id")
