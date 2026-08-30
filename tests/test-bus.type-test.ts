import {
  createEventChannelFactory,
  createTestEventBus,
  type EventBus,
} from "pg-event-bus"

interface CommentCreatedEvent {
  type: "created"
  commentId: string
}

interface CommentDeletedEvent {
  type: "deleted"
  commentId: string
}

type CommentEvent = CommentCreatedEvent | CommentDeletedEvent

interface SessionKey {
  userId: string
  scope: string
}

const testEventBus: EventBus = createTestEventBus()
const instrumentedTestEventBus = createTestEventBus()
const deliveryGapTestEventBus = createTestEventBus({
  onDeliveryGap() {},
})
deliveryGapTestEventBus.simulateDeliveryGap()
const defineEventChannel = createEventChannelFactory(instrumentedTestEventBus)
const commentEvents = defineEventChannel<CommentEvent, SessionKey>(
  (session) => `comment:${session.userId}:${session.scope}`,
)

const payloads: readonly CommentEvent[] = instrumentedTestEventBus.payloadsFor(
  commentEvents,
  { userId: "user-id", scope: "support" },
)

void payloads

const channelInspector = instrumentedTestEventBus.for(commentEvents)
const inferredPayloads: readonly CommentEvent[] = channelInspector.payloadsFor({
  userId: "user-id",
  scope: "support",
})
const narrowedPayloads: readonly CommentCreatedEvent[] =
  channelInspector.payloadsFor<CommentCreatedEvent>({
    userId: "user-id",
    scope: "support",
  })

void inferredPayloads
void narrowedPayloads

// @ts-expect-error Explicit narrowing must remain within the channel payload type.
channelInspector.payloadsFor<{ type: "unrelated" }>({
  userId: "user-id",
  scope: "support",
})

// @ts-expect-error The key must match the bound channel key type.
channelInspector.payloadsFor("user-id")

// @ts-expect-error The key must match the channel key type.
instrumentedTestEventBus.payloadsFor(commentEvents, "user-id")

// @ts-expect-error The payload type is inferred from the channel.
const wrongPayloads: readonly { messageId: string }[] =
  instrumentedTestEventBus.payloadsFor(commentEvents, {
    userId: "user-id",
    scope: "support",
  })

void wrongPayloads
