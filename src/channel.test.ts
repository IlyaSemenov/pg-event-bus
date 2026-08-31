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

function createTestEventBus() {
  const calls: Array<{ event: string; payload: unknown }> = []
  const close = async () => {}
  const bus: EventBus = {
    ready: Promise.resolve(),
    async send(event, payload) {
      calls.push({ event, payload })
    },
    async sendMany(events) {
      calls.push(...events)
    },
    async *on() {},
    close,
    [Symbol.asyncDispose]: close,
  }

  return { bus, calls }
}
