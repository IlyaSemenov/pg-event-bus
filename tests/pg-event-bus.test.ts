import { afterAll, beforeAll, expect, it } from "bun:test"

import {
  createEventChannelFactory,
  createPgEventBus,
  createPublisher,
  type EventBus,
} from "pg-event-bus"
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
const sql = postgres(connectionString, { max: 1 })
const publisher = createPublisher(({ text, values }) =>
  sql.unsafe(text, values),
)
let bus: EventBus

beforeAll(async () => {
  bus = createPgEventBus({
    connectionString: listenerConnectionString,
    channel: postgresChannel,
    publisher,
  })
  await bus.ready
})

afterAll(async () => {
  await bus.close()
  await sql.end()
})

it("delivers typed events between separate PostgreSQL connections", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(
    (key) => `test:${key}`,
  )
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

it("delivers a publication batch in input order", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(
    (key) => `batch:${key}`,
  )
  const controller = new AbortController()
  const stream = events.on("same", controller.signal)
  const firstReceived = stream.next()
  const secondReceived = stream.next()

  await events.sendMany([
    { key: "same", payload: { id: "event-1", value: 1 } },
    { key: "same", payload: { id: "event-2", value: 2 } },
  ])

  expect(await firstReceived).toEqual({
    value: { id: "event-1", value: 1 },
    done: false,
  })
  expect(await secondReceived).toEqual({
    value: { id: "event-2", value: 2 },
    done: false,
  })
  controller.abort()
})

it("delivers a notification only after its publishing transaction commits", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(
    (key) => `commit:${key}`,
  )
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()

  await sql`BEGIN`
  await events.send("first", { id: "committed", value: 1 })
  expect(await settlesWithin(received, 75)).toBe(false)

  await sql`COMMIT`
  expect(await received).toEqual({
    value: { id: "committed", value: 1 },
    done: false,
  })
  controller.abort()
})

it("does not deliver a notification from a rolled-back transaction", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(
    (key) => `rollback:${key}`,
  )
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()

  await sql`BEGIN`
  await events.send("first", { id: "rolled-back", value: 1 })
  await sql`ROLLBACK`

  expect(await settlesWithin(received, 75)).toBe(false)
  controller.abort()
  expect(await received).toEqual({ value: undefined, done: true })
})

it("keeps active streams subscribed after the listener reconnects", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(
    (key) => `reconnect:${key}`,
  )
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()
  const listenerPid = await waitForListenerPid()

  await sql`SELECT pg_terminate_backend(${listenerPid})`
  await waitForListenerPid(listenerPid)
  await events.send("first", { id: "after-reconnect", value: 2 })

  expect(await withTimeout(received, 5_000)).toEqual({
    value: { id: "after-reconnect", value: 2 },
    done: false,
  })
  controller.abort()
})

it("reports a possible delivery gap after each successful reconnect", async () => {
  const applicationName = `${postgresChannel}_gap_listener`
  const gapConnectionString = `${connectionString}${querySeparator}application_name=${applicationName}`
  let gapCount = 0
  const gapBus = createPgEventBus({
    connectionString: gapConnectionString,
    channel: `${postgresChannel}_gap`,
    publisher,
    onDeliveryGap: () => gapCount++,
  })

  try {
    await gapBus.ready
    expect(gapCount).toBe(0)

    let listenerPid = await waitForApplicationPid(applicationName)
    await sql`SELECT pg_terminate_backend(${listenerPid})`
    listenerPid = await waitForApplicationPid(applicationName, listenerPid)
    await waitFor(() => gapCount === 1)

    expect(gapCount).toBe(1)

    await sql`SELECT pg_terminate_backend(${listenerPid})`
    await waitForApplicationPid(applicationName, listenerPid)
    await waitFor(() => gapCount === 2)

    expect(gapCount).toBe(2)
  } finally {
    await gapBus.close()
  }

  expect(gapCount).toBe(2)
})

it("ignores malformed notifications without closing active streams", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(
    (key) => `malformed:${key}`,
  )
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const received = stream.next()

  await sql`SELECT pg_notify(${postgresChannel}, ${"not-json"})`
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
    publisher() {},
  })
  await closingBus.ready

  const events = createEventChannelFactory(closingBus)<TestEvent>((key) => key)
  const stream = events.on("pending")
  const received = stream.next()
  expect(closingBus[Symbol.asyncDispose]).toBe(closingBus.close)
  await closingBus[Symbol.asyncDispose]()

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
  return waitForApplicationPid(listenerApplicationName, excludedPid)
}

async function waitForApplicationPid(
  applicationName: string,
  excludedPid?: number,
): Promise<number> {
  const deadline = Date.now() + 5_000

  while (Date.now() < deadline) {
    const result = await sql<{ pid: number }[]>`
      SELECT pid
      FROM pg_stat_activity
      WHERE application_name = ${applicationName}
    `
    const pid = result[0]?.pid

    if (pid !== undefined && pid !== excludedPid) {
      return pid
    }

    await Bun.sleep(25)
  }

  throw new Error("PostgreSQL listener did not reconnect in time")
}

async function waitFor(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000

  while (Date.now() < deadline) {
    if (condition()) {
      return
    }

    await Bun.sleep(25)
  }

  throw new Error("Condition was not met in time")
}
