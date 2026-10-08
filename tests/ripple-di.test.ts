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

it("isolates and inspects scoped channels in the current dependency context", async () => {
  const test = createTestEventBus()
  const useTenant = defineDependency<string>({ name: "tenant" })
  const defineTenantEventChannel = createEventChannelFactory(useEventBus, {
    scopeEventName: (event) => `tenant:${useTenant()}:${event}`,
  })
  const logEvents = defineTenantEventChannel<string>("log")
  const inTenant = <T>(tenant: string, fn: () => T) =>
    withOverrides([provide(useEventBus, test), provide(useTenant, tenant)], fn)

  const tenantOneEvents = await inTenant("one", () => logEvents.on())
  await inTenant("two", () => logEvents.send("two-message"))
  await inTenant("one", () => logEvents.send("one-message"))

  expect((await tenantOneEvents.next()).value).toBe("one-message")
  expect(await inTenant("one", () => test.payloadsFor(logEvents))).toEqual([
    "one-message",
  ])
  expect(await inTenant("two", () => test.payloadsFor(logEvents))).toEqual([
    "two-message",
  ])

  await tenantOneEvents.return()
})
