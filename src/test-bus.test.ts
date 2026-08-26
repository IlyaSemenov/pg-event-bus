import { expect, it } from "bun:test"

import { createTestEventBus } from "./test-bus"

it("records and delivers events", async () => {
  const eventBus = createTestEventBus()
  const stream = eventBus.on<{ id: string }>("entity:one")
  const received = stream.next()

  expect(await eventBus.ready).toBeUndefined()
  await eventBus.send("entity:one", { id: "first" })

  expect(await received).toEqual({ value: { id: "first" }, done: false })
  expect(eventBus.calls).toEqual([
    { event: "entity:one", payload: { id: "first" } },
  ])
  expect(eventBus.getActiveSubscriptionCount()).toBe(1)

  eventBus.clearCalls()

  expect(eventBus.calls).toEqual([])
  expect(eventBus.getActiveSubscriptionCount()).toBe(1)

  await eventBus.close()
  expect(await stream.next()).toEqual({ value: undefined, done: true })
})

it("tracks a subscription cancelled by its consumer", async () => {
  const eventBus = createTestEventBus()
  const controller = new AbortController()
  const stream = eventBus.on("event", controller.signal)
  const received = stream.next()

  expect(eventBus.getActiveSubscriptionCount()).toBe(1)

  controller.abort()

  expect(await received).toEqual({ value: undefined, done: true })
  expect(eventBus.getActiveSubscriptionCount()).toBe(0)
})

it("closes all subscriptions and rejects later sends", async () => {
  const eventBus = createTestEventBus()
  const stream1 = eventBus.on("one")
  const stream2 = eventBus.on("two")
  const received1 = stream1.next()
  const received2 = stream2.next()

  expect(eventBus.getActiveSubscriptionCount()).toBe(2)

  await eventBus.close()
  await eventBus.close()

  expect(await received1).toEqual({ value: undefined, done: true })
  expect(await received2).toEqual({ value: undefined, done: true })
  expect(eventBus.getActiveSubscriptionCount()).toBe(0)
  await expect(eventBus.send("one", "payload")).rejects.toThrow(
    "Cannot send an event after the test event bus is closed",
  )

  const closedStream = eventBus.on("one")
  expect(await closedStream.next()).toEqual({ value: undefined, done: true })
})
