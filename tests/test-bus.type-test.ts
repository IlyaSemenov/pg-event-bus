import { createEventChannelFactory, type EventBus } from "pg-event-bus"
import { createTestEventBus } from "pg-event-bus/testing"

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
const disposableTestEventBus: AsyncDisposable = instrumentedTestEventBus
const deliveryGapTestEventBus = createTestEventBus()
const deliveryGapStream: AsyncGenerator<void, void, unknown> =
  deliveryGapTestEventBus.deliveryGaps(new AbortController().signal)
deliveryGapTestEventBus.simulateDeliveryGap()
const defineEventChannel = createEventChannelFactory(instrumentedTestEventBus)
const auditEvents = defineEventChannel<CommentEvent>("audit")
const commentEvents = defineEventChannel<CommentEvent, SessionKey>(
  (session) => `comment:${session.userId}:${session.scope}`,
)

const payloads: readonly CommentEvent[] = instrumentedTestEventBus.payloadsFor(
  commentEvents,
  { userId: "user-id", scope: "support" },
)

const auditPayloads: readonly CommentEvent[] =
  instrumentedTestEventBus.payloadsFor(auditEvents)
const auditInspector = instrumentedTestEventBus.for(auditEvents)
const inspectedAuditPayloads: readonly CommentEvent[] =
  auditInspector.payloadsFor()
const narrowedAuditPayloads: readonly CommentCreatedEvent[] =
  auditInspector.payloadsFor<CommentCreatedEvent>()

void payloads
void auditPayloads
void inspectedAuditPayloads
void narrowedAuditPayloads
void testEventBus
void disposableTestEventBus
void deliveryGapStream

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

// @ts-expect-error Fixed-name channel instrumentation does not accept a key.
auditInspector.payloadsFor("audit")

// @ts-expect-error Fixed-name channel instrumentation does not accept a key.
instrumentedTestEventBus.payloadsFor(auditEvents, "audit")

// @ts-expect-error Keyed channel instrumentation requires a key.
channelInspector.payloadsFor()

// @ts-expect-error Keyed channel instrumentation requires a key.
instrumentedTestEventBus.payloadsFor(commentEvents)

// @ts-expect-error The key must match the channel key type.
instrumentedTestEventBus.payloadsFor(commentEvents, "user-id")

// @ts-expect-error The payload type is inferred from the channel.
const wrongPayloads: readonly { messageId: string }[] =
  instrumentedTestEventBus.payloadsFor(commentEvents, {
    userId: "user-id",
    scope: "support",
  })

void wrongPayloads
