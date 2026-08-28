import { expect, it } from "bun:test"

import { createEventChannelFactory } from "./channel"
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

it("records and delivers a batch in order", async () => {
  const eventBus = createTestEventBus()
  const firstStream = eventBus.on<string>("first")
  const secondStream = eventBus.on<string>("second")
  const firstReceived = firstStream.next()
  const secondReceived = secondStream.next()

  await eventBus.sendMany([
    { event: "first", payload: "one" },
    { event: "second", payload: "two" },
  ])

  expect(await firstReceived).toEqual({ value: "one", done: false })
  expect(await secondReceived).toEqual({ value: "two", done: false })
  expect(eventBus.calls).toEqual([
    { event: "first", payload: "one" },
    { event: "second", payload: "two" },
  ])
  await eventBus.close()
})

it("returns payload snapshots for a typed channel and key", async () => {
  const eventBus = createTestEventBus()
  const defineEventChannel = createEventChannelFactory(eventBus)
  const firstChannel = defineEventChannel<{ id: string }, { id: string }>(
    (key) => `first:${key.id}`,
  )
  const secondChannel = defineEventChannel<{ id: string }>(
    (key) => `second:${key}`,
  )

  await firstChannel.send({ id: "one" }, { id: "first" })
  await secondChannel.send("one", { id: "other-channel" })
  await firstChannel.sendMany([
    { key: { id: "two" }, payload: { id: "other-key" } },
    { key: { id: "one" }, payload: { id: "second" } },
  ])

  const payloads = eventBus.payloadsFor(firstChannel, { id: "one" })

  expect(payloads).toEqual([{ id: "first" }, { id: "second" }])
  const mutablePayloads = payloads as Array<{ id: string }>
  mutablePayloads.push({ id: "snapshot-only" })
  expect(eventBus.payloadsFor(firstChannel, { id: "one" })).toEqual([
    { id: "first" },
    { id: "second" },
  ])
  expect(eventBus.for(firstChannel).payloadsFor({ id: "one" })).toEqual([
    { id: "first" },
    { id: "second" },
  ])
})

it("rejects channels not created by the channel factory", () => {
  const eventBus = createTestEventBus()

  expect(() =>
    eventBus.payloadsFor(
      {
        async send() {},
        async sendMany() {},
        async *on() {},
      },
      "key",
    ),
  ).toThrow("Expected an event channel created by createEventChannelFactory")
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
  await expect(eventBus.sendMany([])).rejects.toThrow(
    "Cannot send events after the test event bus is closed",
  )

  const closedStream = eventBus.on("one")
  expect(await closedStream.next()).toEqual({ value: undefined, done: true })
})
