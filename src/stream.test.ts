import { expect, it } from "bun:test"
import { EventEmitter } from "node:events"

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
