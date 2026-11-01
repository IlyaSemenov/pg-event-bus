import { afterAll, beforeAll, expect, it } from "bun:test"
import { type AddressInfo, connect, createServer, type Socket } from "node:net"

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
const publisher = createPublisher(({ text, values }) => sql.unsafe(text, values))
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
  const events = createEventChannelFactory(bus)<TestEvent>(key => `test:${key}`)
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

it("buffers notifications before the first read using a delivery barrier", async () => {
  const stream = bus.on<string>("eager-buffer")
  const barrier = bus.on("eager-barrier")
  const delivered = barrier.next()

  try {
    await bus.sendMany([
      { event: "eager-buffer", payload: "first" },
      { event: "eager-buffer", payload: "second" },
      { event: "eager-barrier", payload: null },
    ])
    await delivered
    expect(await stream.next()).toEqual({ value: "first", done: false })
    expect(await stream.next()).toEqual({ value: "second", done: false })
  } finally {
    await stream.return()
    await barrier.return()
  }
})

for (const kind of ["events", "gaps"] as const) {
  for (const started of [false, true]) {
    it(`returns a production ${kind} stream ${started ? "during" : "before"} reading`, async () => {
      const stream = kind === "events" ? bus.on("return") : bus.deliveryGaps()
      const pending = started ? stream.next() : undefined
      expect(await stream.return()).toEqual({ value: undefined, done: true })
      if (pending) expect(await pending).toEqual({ value: undefined, done: true })
      expect(await stream.next()).toEqual({ value: undefined, done: true })
    })
  }
}

it("delivers a publication batch in input order", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(key => `batch:${key}`)
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
  const events = createEventChannelFactory(bus)<TestEvent>(key => `commit:${key}`)
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
  const events = createEventChannelFactory(bus)<TestEvent>(key => `rollback:${key}`)
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
  const events = createEventChannelFactory(bus)<TestEvent>(key => `reconnect:${key}`)
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
  const gapBus = createPgEventBus({
    connectionString: gapConnectionString,
    channel: `${postgresChannel}_gap`,
    publisher,
  })
  const expectedConsumerError = new Error("delivery gap consumer failed")
  const failingConsumer = (async () => {
    for await (const deliveryGap of gapBus.deliveryGaps()) {
      void deliveryGap
      throw expectedConsumerError
    }
  })()
  const failingConsumerError = failingConsumer.catch((error: unknown) => error)
  const survivingStream = gapBus.deliveryGaps()
  const initialObserver = gapBus.deliveryGaps()
  const initialGap = initialObserver.next()

  try {
    await gapBus.ready
    expect(await settlesWithin(initialGap, 75)).toBe(false)

    let listenerPid = await waitForApplicationPid(applicationName)
    await sql`SELECT pg_terminate_backend(${listenerPid})`
    listenerPid = await waitForApplicationPid(applicationName, listenerPid)

    expect(await failingConsumerError).toBe(expectedConsumerError)
    expect(await initialGap).toEqual({ value: undefined, done: false })
    await initialObserver.return()
    expect(await survivingStream.next()).toEqual({
      value: undefined,
      done: false,
    })

    const secondSurvivingGap = survivingStream.next()
    await sql`SELECT pg_terminate_backend(${listenerPid})`
    await waitForApplicationPid(applicationName, listenerPid)

    expect(await withTimeout(secondSurvivingGap, 5_000)).toEqual({
      value: undefined,
      done: false,
    })
  } finally {
    await gapBus.close()
  }

  expect(await survivingStream.next()).toEqual({
    value: undefined,
    done: true,
  })
})

it("reconnects after the database refuses connections for a while", async () => {
  const proxy = await startConnectionProxy()
  const outageBus = createPgEventBus({
    connectionString: proxy.connectionString,
    channel: `${postgresChannel}_outage`,
    publisher,
  })
  const events = createEventChannelFactory(outageBus)<TestEvent>(key => `outage:${key}`)
  const controller = new AbortController()
  const stream = events.on("first", controller.signal)
  const gaps = outageBus.deliveryGaps(controller.signal)

  try {
    await outageBus.ready
    const gap = gaps.next()

    // Simulates a database restart: drop the listener connection, then refuse reconnection attempts.
    await proxy.stop()
    await Bun.sleep(250)
    await proxy.start()

    expect(await withTimeout(gap, 5_000)).toEqual({ value: undefined, done: false })
    const received = stream.next()
    await events.send("first", { id: "after-outage", value: 5 })
    expect(await withTimeout(received, 5_000)).toEqual({
      value: { id: "after-outage", value: 5 },
      done: false,
    })
  } finally {
    controller.abort()
    await outageBus.close()
    await proxy.stop()
  }
}, 15_000)

it("ignores malformed notifications without closing active streams", async () => {
  const events = createEventChannelFactory(bus)<TestEvent>(key => `malformed:${key}`)
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

  const events = createEventChannelFactory(closingBus)<TestEvent>(key => key)
  const stream = events.on("pending")
  const received = stream.next()
  expect(closingBus[Symbol.asyncDispose]).toBe(closingBus.close)
  await closingBus[Symbol.asyncDispose]()

  expect(await received).toEqual({ value: undefined, done: true })
  await expect(events.send("pending", { id: "after-close", value: 4 })).rejects.toThrow(
    "Cannot send an event after the PostgreSQL event bus is closed",
  )
  await closingBus.close()
})

async function settlesWithin<T>(promise: Promise<T>, milliseconds: number) {
  const marker = Symbol("pending")
  const result = await Promise.race([promise, Bun.sleep(milliseconds).then(() => marker)])
  return result !== marker
}

async function withTimeout<T>(promise: Promise<T>, milliseconds: number): Promise<T> {
  return Promise.race([
    promise,
    Bun.sleep(milliseconds).then(() => {
      throw new Error(`Timed out after ${milliseconds}ms`)
    }),
  ])
}

/**
 * Starts a TCP proxy to the test database that can drop its connections and refuse new ones.
 */
async function startConnectionProxy() {
  const target = new URL(connectionString)
  const targetHost = target.hostname || "localhost"
  const targetPort = Number(target.port || 5432)
  const sockets = new Set<Socket>()
  let port = 0
  const server = createServer(client => {
    const upstream = connect(targetPort, targetHost)
    const drop = () => {
      client.destroy()
      upstream.destroy()
    }
    sockets.add(client).add(upstream)
    client.on("error", drop).on("close", drop)
    upstream.on("error", drop).on("close", drop)
    client.pipe(upstream).pipe(client)
  })

  function start() {
    return new Promise<void>(resolve => server.listen(port, "127.0.0.1", resolve))
  }

  await start()
  port = (server.address() as AddressInfo).port
  const proxied = new URL(connectionString)
  proxied.host = `127.0.0.1:${port}`

  return {
    connectionString: proxied.toString(),
    start,
    /** Closes open connections and refuses new ones until `start()`. */
    stop() {
      for (const socket of sockets) socket.destroy()
      sockets.clear()
      return new Promise<void>(resolve =>
        server.listening ? server.close(() => resolve()) : resolve(),
      )
    },
  }
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
