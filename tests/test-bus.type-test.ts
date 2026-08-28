import {
  createEventChannelFactory,
  createTestEventBus,
  type EventBus,
} from "pg-event-bus"

interface CommentEvent {
  commentId: string
}

interface SessionKey {
  userId: string
  scope: string
}

const testEventBus: EventBus = createTestEventBus()
const instrumentedTestEventBus = createTestEventBus()
const defineEventChannel = createEventChannelFactory(instrumentedTestEventBus)
const commentEvents = defineEventChannel<CommentEvent, SessionKey>(
  (session) => `comment:${session.userId}:${session.scope}`,
)

const payloads: readonly CommentEvent[] = instrumentedTestEventBus.payloadsFor(
  commentEvents,
  { userId: "user-id", scope: "support" },
)

void payloads

// @ts-expect-error The key must match the channel key type.
instrumentedTestEventBus.payloadsFor(commentEvents, "user-id")

// @ts-expect-error The payload type is inferred from the channel.
const wrongPayloads: readonly { messageId: string }[] =
  instrumentedTestEventBus.payloadsFor(commentEvents, {
    userId: "user-id",
    scope: "support",
  })

void wrongPayloads
