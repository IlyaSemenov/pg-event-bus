import { EventEmitter } from "node:events"

import postgres from "postgres"

import {
  createEventChannelFactory,
  type DefineEventChannel,
  type EventBus,
} from "./channel"
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
  /** PostgreSQL channel shared by all domain events on this bus. */
  channel: string
  /** Publishes an encoded notification through the application's current database connection. */
  publish: PublishPgNotification
}

/**
 * Event bus backed by one reconnecting PostgreSQL `LISTEN` connection.
 *
 * Publication is delegated to the application through the configured publisher.
 */
export interface PgEventBus extends EventBus {
  /** Resolves after the dedicated connection starts listening. */
  ready: Promise<void>
  /** Defines a typed channel backed by this event bus. */
  defineEventChannel: DefineEventChannel
  /** Stops the listener and completes active event streams. */
  close(): Promise<void>
}

/**
 * Creates a PostgreSQL event bus and starts its dedicated listener connection.
 *
 * The returned bus can publish immediately, while `ready` reports when `LISTEN` has become active.
 */
export function createPgEventBus(options: PgEventBusOptions): PgEventBus {
  const listener = postgres(options.connectionString)
  const events = new EventEmitter()
  const busAbort = new AbortController()
  let closed = false

  events.setMaxListeners(0)

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

  const eventBus: PgEventBus = {
    ready,
    send,
    on: onEvent,
    defineEventChannel: createEventChannelFactory(() => eventBus),
    close,
  }

  return eventBus
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}
