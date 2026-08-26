import { EventEmitter } from "node:events"

import postgres from "postgres"

import type { EventBus } from "./channel"
import { decodeEventMessage, encodeEventMessage } from "./message"
import { streamEvents } from "./stream"

/** Encoded PostgreSQL notification passed to an application's publisher. */
export interface PgNotification {
  /** PostgreSQL channel supplied when the event bus was created. */
  channel: string
  /** JSON-encoded event name and payload accepted by `pg_notify`. */
  payload: string
}

/**
 * Publishes one encoded notification through an application-owned database connection.
 *
 * The returned value is awaited so a transaction cannot finish before publication completes.
 */
export type PublishPgNotification = (
  notification: PgNotification,
) => unknown | PromiseLike<unknown>

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
  /** Publishes an encoded notification through the application's current database connection. */
  publish: PublishPgNotification
}

/**
 * Creates a PostgreSQL event bus and starts its dedicated listener connection.
 *
 * The returned bus can publish immediately, while `ready` reports when `LISTEN` has become active.
 */
export function createPgEventBus(options: PgEventBusOptions): EventBus {
  const listener = postgres(options.connectionString)
  const events = new EventEmitter().setMaxListeners(0)
  const busAbort = new AbortController()
  let closed = false

  function fail(error: Error) {
    busAbort.abort(error)
  }

  function receive(payload: string) {
    const message = decodeEventMessage(payload)

    if (message) {
      events.emit(message.event, message.payload)
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
    await listener.listen(options.channel, receive)

    if (closed) {
      await listener.end()
    }
  }

  async function send(event: string, payload: unknown) {
    if (closed) {
      throw new Error(
        "Cannot send an event after the PostgreSQL event bus is closed",
      )
    }

    await options.publish({
      channel: options.channel,
      payload: encodeEventMessage(event, payload),
    })
  }

  function onEvent<TPayload>(event: string, signal?: AbortSignal) {
    return streamEvents<TPayload>(events, event, busAbort.signal, signal)
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
    on: onEvent,
    close,
  }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}
