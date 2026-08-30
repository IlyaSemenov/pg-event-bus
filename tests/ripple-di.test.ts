import { expect, it } from "bun:test"

import { createEventChannelFactory, type EventBus } from "pg-event-bus"
import { createTestEventBus } from "pg-event-bus/testing"
import { defineDependency, provide, withOverrides } from "ripple-di"

interface CommentEvent {
  commentId: string
}

const production = createTestEventBus()
const useEventBus = defineDependency<EventBus>(() => production)
const defineEventChannel = createEventChannelFactory(useEventBus)
const commentEvents = defineEventChannel<CommentEvent>(
  (postId) => `post:${postId}:comment`,
)

it("resolves a scoped override after the domain channel is declared", async () => {
  const test = createTestEventBus()

  await commentEvents.send("production", { commentId: "production-comment" })
  await withOverrides(provide(useEventBus, test), () =>
    commentEvents.send("test", { commentId: "test-comment" }),
  )

  expect(production.calls).toEqual([
    {
      event: "post:production:comment",
      payload: { commentId: "production-comment" },
    },
  ])
  expect(test.calls).toEqual([
    {
      event: "post:test:comment",
      payload: { commentId: "test-comment" },
    },
  ])
})
