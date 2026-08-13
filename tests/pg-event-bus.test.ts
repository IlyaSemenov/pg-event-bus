import { afterAll, beforeAll, expect, it } from "bun:test"

import { createPgEventBus, type PgEventBus } from "pg-event-bus"
import postgres from "postgres"

interface TestEvent {
  id: string
  value: number
}

const connectionString = process.env.DATABASE_URL ?? "postgresql://localhost"
const postgresChannel = `pg_event_bus_${process.pid}_${Date.now()}`
const listenerApplicationName = `${postgresChannel}_listener`
const querySeparator = connectionString.includes("?") ? "&" : "?"
const listenerConnectionString = `${connectionString}${querySeparator}application_name=${listenerApplicationName}`
const publisher = postgres(connectionString, { max: 1 })
let bus: PgEventBus

beforeAll(async () => {
  bus = createPgEventBus({
    connectionString: listenerConnectionString,
    channel: postgresChannel,
    async publish({ channel, payload }) {
      await publisher`SELECT pg_notify(${channel}, ${payload})`
    },
  })
  await bus.ready
})

afterAll(async () => {
  await bus.close()
  await publisher.end()
})

it("delivers typed events between separate PostgreSQL connections", async () => {
  const events = bus.defineEventChannel<TestEvent>((key) => `test:${key}`)
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()

  await events.send("first", { id: "event-1", value: 42 })

  expect(await received).toEqual({
    value: { id: "event-1", value: 42 },
    done: false,
  })
  controller.abort()
  expect(await stream.next()).toEqual({ value: undefined, done: true })
})

it("delivers a notification only after its publishing transaction commits", async () => {
  const events = bus.defineEventChannel<TestEvent>((key) => `commit:${key}`)
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()

  await publisher`BEGIN`
  await events.send("first", { id: "committed", value: 1 })
  expect(await settlesWithin(received, 75)).toBe(false)

  await publisher`COMMIT`
  expect(await received).toEqual({
    value: { id: "committed", value: 1 },
    done: false,
  })
  controller.abort()
})

it("does not deliver a notification from a rolled-back transaction", async () => {
  const events = bus.defineEventChannel<TestEvent>((key) => `rollback:${key}`)
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()

  await publisher`BEGIN`
  await events.send("first", { id: "rolled-back", value: 1 })
  await publisher`ROLLBACK`

  expect(await settlesWithin(received, 75)).toBe(false)
  controller.abort()
  expect(await received).toEqual({ value: undefined, done: true })
})

it("keeps active streams subscribed after the listener reconnects", async () => {
  const events = bus.defineEventChannel<TestEvent>((key) => `reconnect:${key}`)
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()
  const listenerPid = await waitForListenerPid()

  await publisher`SELECT pg_terminate_backend(${listenerPid})`
  await waitForListenerPid(listenerPid)
  await events.send("first", { id: "after-reconnect", value: 2 })

  expect(await withTimeout(received, 5_000)).toEqual({
    value: { id: "after-reconnect", value: 2 },
    done: false,
  })
  controller.abort()
})

it("ignores malformed notifications without closing active streams", async () => {
  const events = bus.defineEventChannel<TestEvent>((key) => `malformed:${key}`)
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()

  await publisher`SELECT pg_notify(${postgresChannel}, ${"not-json"})`
  await events.send("first", { id: "after-malformed", value: 3 })

  expect(await received).toEqual({
    value: { id: "after-malformed", value: 3 },
    done: false,
  })
  controller.abort()
})

it("completes active streams when the bus closes", async () => {
  const closingBus = createPgEventBus({
    connectionString,
    channel: `${postgresChannel}_close`,
    publish() {},
  })
  await closingBus.ready

  const events = closingBus.defineEventChannel<TestEvent>((key) => key)
  const stream = events.on("pending")
  const received = stream.next()
  await closingBus.close()

  expect(await received).toEqual({ value: undefined, done: true })
  await expect(
    events.send("pending", { id: "after-close", value: 4 }),
  ).rejects.toThrow(
    "Cannot send an event after the PostgreSQL event bus is closed",
  )
  await closingBus.close()
})

async function settlesWithin<T>(promise: Promise<T>, milliseconds: number) {
  const marker = Symbol("pending")
  const result = await Promise.race([
    promise,
    Bun.sleep(milliseconds).then(() => marker),
  ])
  return result !== marker
}

async function withTimeout<T>(
  promise: Promise<T>,
  milliseconds: number,
): Promise<T> {
  return Promise.race([
    promise,
    Bun.sleep(milliseconds).then(() => {
      throw new Error(`Timed out after ${milliseconds}ms`)
    }),
  ])
}

async function waitForListenerPid(excludedPid?: number): Promise<number> {
  const deadline = Date.now() + 5_000

  while (Date.now() < deadline) {
    const result = await publisher<{ pid: number }[]>`
      SELECT pid
      FROM pg_stat_activity
      WHERE application_name = ${listenerApplicationName}
    `
    const pid = result[0]?.pid

    if (pid !== undefined && pid !== excludedPid) {
      return pid
    }

    await Bun.sleep(25)
  }

  throw new Error("PostgreSQL listener did not reconnect in time")
}
