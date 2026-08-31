import { expect, it } from "bun:test"

import { createEventChannelFactory, type EventBus } from "./channel"

it("resolves the event bus lazily for dependency injection", async () => {
  const bus1 = createTestEventBus()
  const bus2 = createTestEventBus()
  let current: EventBus = bus1.bus
  const defineEventChannel = createEventChannelFactory(() => current)
  const events = defineEventChannel<{ id: string }>((key) => `entity:${key}`)

  await events.send("one", { id: "first" })
  current = bus2.bus
  await events.sendMany([
    { key: "two", payload: { id: "second" } },
    { key: "three", payload: { id: "third" } },
  ])

  expect(bus1.calls).toEqual([
    { event: "entity:one", payload: { id: "first" } },
  ])
  expect(bus2.calls).toEqual([
    { event: "entity:two", payload: { id: "second" } },
    { event: "entity:three", payload: { id: "third" } },
  ])
})

it("uses a fixed event name and resolves the event bus for every operation", async () => {
  const test = createTestEventBus()
  let resolverCalls = 0
  const defineEventChannel = createEventChannelFactory(() => {
    resolverCalls += 1
    return test.bus
  })
  const events = defineEventChannel<{ id: string }>("activity_log")
  const controller = new AbortController()

  await events.send({ id: "first" })
  await events.sendMany([{ id: "second" }, { id: "third" }])
  await events.on(controller.signal).next()

  expect(resolverCalls).toBe(3)
  expect(test.calls).toEqual([
    { event: "activity_log", payload: { id: "first" } },
    { event: "activity_log", payload: { id: "second" } },
    { event: "activity_log", payload: { id: "third" } },
  ])
  expect(test.batches).toEqual([
    [
      { event: "activity_log", payload: { id: "second" } },
      { event: "activity_log", payload: { id: "third" } },
    ],
  ])
  expect(test.subscriptions).toEqual([
    { event: "activity_log", signal: controller.signal },
  ])
})

function createTestEventBus() {
  const calls: Array<{ event: string; payload: unknown }> = []
  const batches: Array<readonly { event: string; payload: unknown }[]> = []
  const subscriptions: Array<{ event: string; signal?: AbortSignal }> = []
  const close = async () => {}
  const bus: EventBus = {
    ready: Promise.resolve(),
    async send(event, payload) {
      calls.push({ event, payload })
    },
    async sendMany(events) {
      batches.push(events)
      calls.push(...events)
    },
    async *on<TPayload>(event: string, signal?: AbortSignal) {
      subscriptions.push({ event, signal })
      yield* [] as TPayload[]
    },
    async *deliveryGaps() {},
    close,
    [Symbol.asyncDispose]: close,
  }

  return { bus, calls, batches, subscriptions }
}
