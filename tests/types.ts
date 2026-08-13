import type { PgEventBus } from "pg-event-bus"

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

declare const eventBus: PgEventBus

const commentEvents = eventBus.defineEventChannel<CommentEvent>(
  (postId) => `post:${postId}:comment`,
)
const chatEvents = eventBus.defineEventChannel<ChatEvent, SessionKey>(
  (session) => `chat:${session.userId}:${session.scope}`,
)

commentEvents.send("post-id", { commentId: "comment-id" })
chatEvents.send(
  { userId: "user-id", scope: "support" },
  { messageId: "message-id" },
)

// @ts-expect-error String is the default key type.
commentEvents.send({ postId: "post-id" }, { commentId: "comment-id" })

// @ts-expect-error The payload must match the channel payload type.
commentEvents.send("post-id", { messageId: "message-id" })

// @ts-expect-error A channel with a composite key does not accept a string key.
chatEvents.on("user-id")
