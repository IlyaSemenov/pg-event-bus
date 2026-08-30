import { EventEmitter } from "node:events"

import postgres from "postgres"

import type { EventBus, EventBusEvent } from "./channel"
import { decodeEventMessage, encodeEventMessage } from "./message"
import type { PgEventPublisher } from "./publisher"
import { streamEvents } from "./stream"

/** Configuration for a PostgreSQL event bus. */
export interface PgEventBusOptions {
  /** PostgreSQL connection string used by the dedicated LISTEN connection. */
  connectionString: string
  /**
   * PostgreSQL channel that carries every domain event of this bus.
   *
   * The package runs `LISTEN` on it, and the configured publisher runs `NOTIFY` on it.
   */
  channel: string
  /** Publishes encoded notifications through the application's current database connection. */
  publisher: PgEventPublisher
  /** Runs after a disconnected listener has successfully started listening again. */
  onDeliveryGap?: () => void
}

/**
 * Creates a PostgreSQL event bus and starts its dedicated listener connection.
 *
 * The returned bus can publish immediately, while `ready` reports when `LISTEN` has become active.
 */
export function createPgEventBus(options: PgEventBusOptions): EventBus {
  const listener = postgres(options.connectionString)
  const eventEmitter = new EventEmitter().setMaxListeners(0)
  const busAbort = new AbortController()
  let hasListened = false
  let closed = false

  function fail(error: Error) {
    busAbort.abort(error)
  }

  function receive(payload: string) {
    const message = decodeEventMessage(payload)

    if (message) {
      eventEmitter.emit(message.event, message.payload)
    }
  }

  const ready = initialize().catch((error: unknown) => {
    const failure = toError(error)
    fail(failure)
    throw failure
  })
  // `ready` is optional, so observe failures without changing what callers await.
  void ready.catch(() => {})

  async function initialize() {
    await listener.listen(options.channel, receive, handleListen)

    if (closed) {
      await listener.end()
    }
  }

  function handleListen() {
    if (!hasListened) {
      hasListened = true
      return
    }

    if (!closed) {
      options.onDeliveryGap?.()
    }
  }

  async function send(event: string, payload: unknown) {
    if (closed) {
      throw new Error(
        "Cannot send an event after the PostgreSQL event bus is closed",
      )
    }

    await options.publisher([
      {
        channel: options.channel,
        payload: encodeEventMessage(event, payload),
      },
    ])
  }

  async function sendMany(events: readonly EventBusEvent[]) {
    if (closed) {
      throw new Error(
        "Cannot send events after the PostgreSQL event bus is closed",
      )
    }

    if (events.length === 0) {
      return
    }

    await options.publisher(
      events.map(({ event, payload }) => ({
        channel: options.channel,
        payload: encodeEventMessage(event, payload),
      })),
    )
  }

  function onEvent<TPayload>(event: string, signal?: AbortSignal) {
    return streamEvents<TPayload>(eventEmitter, event, busAbort.signal, signal)
  }

  async function close() {
    if (closed) {
      return
    }

    closed = true
    busAbort.abort()
    await listener.end()
  }

  return {
    ready,
    send,
    sendMany,
    on: onEvent,
    close,
  }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}
