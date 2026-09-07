import { expect, it } from "bun:test"
import { EventEmitter, getEventListeners } from "node:events"

import { streamEvents } from "./stream"

it("routes the selected event as an async stream", async () => {
  const eventEmitter = new EventEmitter()
  const busAbort = new AbortController()
  const stream = streamEvents<{ id: string }>(
    eventEmitter,
    "selected",
    busAbort.signal,
  )
  const received = stream.next()

  eventEmitter.emit("other", { id: "ignored" })
  eventEmitter.emit("selected", { id: "received" })

  expect(await received).toEqual({ value: { id: "received" }, done: false })
  busAbort.abort()
  expect(await stream.next()).toEqual({ value: undefined, done: true })
})

it("completes only the stream cancelled by its consumer", async () => {
  const eventEmitter = new EventEmitter()
  const busAbort = new AbortController()
  const controller1 = new AbortController()
  const controller2 = new AbortController()
  const stream1 = streamEvents(
    eventEmitter,
    "event",
    busAbort.signal,
    controller1.signal,
  )
  const stream2 = streamEvents(
    eventEmitter,
    "event",
    busAbort.signal,
    controller2.signal,
  )
  const result1 = stream1.next()
  const result2 = stream2.next()

  controller1.abort()
  eventEmitter.emit("event", "still-active")

  expect(await result1).toEqual({ value: undefined, done: true })
  expect(await result2).toEqual({ value: "still-active", done: false })
  controller2.abort()
})

it("fails active streams when the event bus fails", async () => {
  const eventEmitter = new EventEmitter()
  const busAbort = new AbortController()
  const expected = new Error("listener failed")
  const stream = streamEvents(eventEmitter, "event", busAbort.signal)
  const received = stream.next()

  busAbort.abort(expected)

  await expect(received).rejects.toBe(expected)
})

it("completes for an already aborted consumer signal", async () => {
  const eventEmitter = new EventEmitter()
  const busAbort = new AbortController()
  const consumerAbort = new AbortController()
  consumerAbort.abort()

  const stream = streamEvents(
    eventEmitter,
    "event",
    busAbort.signal,
    consumerAbort.signal,
  )

  expect(await stream.next()).toEqual({ value: undefined, done: true })
})

it("treats a custom consumer abort reason as cancellation", async () => {
  const eventEmitter = new EventEmitter()
  const busAbort = new AbortController()
  const consumerAbort = new AbortController()
  const stream = streamEvents(
    eventEmitter,
    "event",
    busAbort.signal,
    consumerAbort.signal,
  )
  const received = stream.next()

  consumerAbort.abort(new Error("consumer stopped"))

  expect(await received).toEqual({ value: undefined, done: true })
})

it("honors consumer cancellation before a bus failure", async () => {
  const eventEmitter = new EventEmitter()
  const busAbort = new AbortController()
  const consumerAbort = new AbortController()
  const stream = streamEvents(
    eventEmitter,
    "event",
    busAbort.signal,
    consumerAbort.signal,
  )
  const received = stream.next()

  consumerAbort.abort()
  busAbort.abort(new Error("listener failed"))

  expect(await received).toEqual({ value: undefined, done: true })
})

it("reports a bus failure before consumer cancellation", async () => {
  const eventEmitter = new EventEmitter()
  const busAbort = new AbortController()
  const consumerAbort = new AbortController()
  const expected = new Error("listener failed")
  const stream = streamEvents(
    eventEmitter,
    "event",
    busAbort.signal,
    consumerAbort.signal,
  )
  const received = stream.next()

  busAbort.abort(expected)
  consumerAbort.abort()

  await expect(received).rejects.toBe(expected)
})

it("registers immediately and buffers payloads in FIFO order", async () => {
  const emitter = new EventEmitter()
  const bus = new AbortController()
  const stream = streamEvents(emitter, "event", bus.signal)

  expect(emitter.listenerCount("event")).toBe(1)
  emitter.emit("event", undefined)
  emitter.emit("event", "second")
  emitter.emit("event", "third")

  expect(await stream.next()).toEqual({ value: undefined, done: false })
  expect(await stream.next()).toEqual({ value: "second", done: false })
  expect(await stream.next()).toEqual({ value: "third", done: false })
  await stream.return()
})

for (const stop of ["return", "consumer abort", "bus close"] as const) {
  for (const phase of ["unread", "pending", "buffered"] as const) {
    it(`${stop} cleans up an ${phase} stream immediately and idempotently`, async () => {
      const emitter = new EventEmitter()
      const bus = new AbortController()
      const consumer = new AbortController()
      const stream = streamEvents(emitter, "event", bus.signal, consumer.signal)
      const pending = phase === "pending" ? [stream.next(), stream.next()] : []
      if (phase === "buffered") {
        emitter.emit("event", "first")
        emitter.emit("event", "second")
      }

      expect(getEventListeners(bus.signal, "abort")).toHaveLength(1)
      expect(getEventListeners(consumer.signal, "abort")).toHaveLength(1)
      const returned = stop === "return" ? stream.return() : undefined
      if (stop === "consumer abort") consumer.abort(new Error("cancelled"))
      if (stop === "bus close") bus.abort()

      expect(emitter.eventNames()).toEqual([])
      expect(getEventListeners(bus.signal, "abort")).toHaveLength(0)
      expect(getEventListeners(consumer.signal, "abort")).toHaveLength(0)
      for (const result of await Promise.all(pending)) {
        expect(result).toEqual({ value: undefined, done: true })
      }
      if (returned) {
        expect(await returned).toEqual({ value: undefined, done: true })
      }

      emitter.emit("event", "too late")
      if (phase === "buffered" && stop !== "return") {
        expect(await stream.next()).toEqual({ value: "first", done: false })
        expect(await stream.next()).toEqual({ value: "second", done: false })
      }
      expect(await stream.next()).toEqual({ value: undefined, done: true })
      await stream.return()
      await stream.return()
      consumer.abort()
      bus.abort()
      expect(emitter.eventNames()).toEqual([])
    })
  }
}

for (const source of ["consumer", "bus"] as const) {
  it(`does not register with an already aborted ${source} signal`, async () => {
    const emitter = new EventEmitter()
    const bus = new AbortController()
    const consumer = new AbortController()
    const registrations: unknown[] = []
    emitter.on("newListener", (event) => registrations.push(event))
    if (source === "consumer") consumer.abort(new Error("cancelled"))
    else bus.abort()

    const stream = streamEvents(emitter, "event", bus.signal, consumer.signal)

    expect(registrations).toEqual([])
    expect(getEventListeners(bus.signal, "abort")).toHaveLength(0)
    expect(getEventListeners(consumer.signal, "abort")).toHaveLength(0)
    expect(await stream.next()).toEqual({ value: undefined, done: true })
  })
}

for (const source of ["bus", "emitter"] as const) {
  for (const buffered of [false, true]) {
    it(`reports ${source} failure once after ${buffered ? "buffered" : "pending"} reads`, async () => {
      const emitter = new EventEmitter()
      const bus = new AbortController()
      const consumer = new AbortController()
      const stream = streamEvents(emitter, "event", bus.signal, consumer.signal)
      const error = new Error("failed")
      const pending = buffered
        ? []
        : [stream.next(), stream.next(), stream.next()]
      if (buffered) {
        emitter.emit("event", "first")
        emitter.emit("event", "second")
      }
      if (source === "bus") bus.abort(error)
      else emitter.emit("error", error)
      consumer.abort()

      expect(emitter.eventNames()).toEqual([])
      expect(getEventListeners(bus.signal, "abort")).toHaveLength(0)
      expect(getEventListeners(consumer.signal, "abort")).toHaveLength(0)
      if (buffered) {
        expect(await stream.next()).toEqual({ value: "first", done: false })
        expect(await stream.next()).toEqual({ value: "second", done: false })
        await expect(stream.next()).rejects.toBe(error)
      } else {
        const results = await Promise.allSettled(pending)
        expect(results).toEqual([
          { status: "rejected", reason: error },
          { status: "fulfilled", value: { value: undefined, done: true } },
          { status: "fulfilled", value: { value: undefined, done: true } },
        ])
      }
      expect(await stream.next()).toEqual({ value: undefined, done: true })
    })
  }
}

it("reports an existing bus failure without registering listeners", async () => {
  const emitter = new EventEmitter()
  const bus = new AbortController()
  const error = new Error("startup failed")
  bus.abort(error)
  const stream = streamEvents(emitter, "event", bus.signal)

  expect(emitter.eventNames()).toEqual([])
  await expect(stream.next()).rejects.toBe(error)
  expect(await stream.next()).toEqual({ value: undefined, done: true })
})

it("satisfies concurrent reads in FIFO order", async () => {
  const emitter = new EventEmitter()
  const bus = new AbortController()
  const stream = streamEvents(emitter, "event", bus.signal)
  const pending = [stream.next(), stream.next(), stream.next()]
  for (const value of [1, 2, 3]) emitter.emit("event", value)

  expect(await Promise.all(pending)).toEqual([
    { value: 1, done: false },
    { value: 2, done: false },
    { value: 3, done: false },
  ])
  await stream.return()
})

it("throw discards the buffer and closes pending reads without affecting peers", async () => {
  const emitter = new EventEmitter()
  const bus = new AbortController()
  const stream = streamEvents(emitter, "event", bus.signal)
  const peer = streamEvents(emitter, "event", bus.signal)
  const pending = stream.next()
  const error = new Error("consumer failed")

  await expect(stream.throw(error)).rejects.toBe(error)
  expect(await pending).toEqual({ value: undefined, done: true })
  expect(emitter.listenerCount("event")).toBe(1)
  emitter.emit("event", "peer")
  expect(await peer.next()).toEqual({ value: "peer", done: false })
  await expect(peer.throw(error)).rejects.toBe(error)
  expect(await peer.next()).toEqual({ value: undefined, done: true })
  expect(emitter.eventNames()).toEqual([])
})
