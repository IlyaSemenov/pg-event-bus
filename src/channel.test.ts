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
  await events.send("two", { id: "second" })

  expect(bus1.calls).toEqual([
    { event: "entity:one", payload: { id: "first" } },
  ])
  expect(bus2.calls).toEqual([
    { event: "entity:two", payload: { id: "second" } },
  ])
})

function createTestEventBus() {
  const calls: Array<{ event: string; payload: unknown }> = []
  const bus: EventBus = {
    async send(event, payload) {
      calls.push({ event, payload })
    },
    async *on() {},
  }

  return { bus, calls }
}
