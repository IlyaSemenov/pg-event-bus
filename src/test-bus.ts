import { EventEmitter } from "node:events"

import type { EventBus, EventBusEvent, EventChannel } from "./channel"
import { resolveEventChannelName } from "./channel-name"
import { streamEvents } from "./stream"

/** One event sent through a test event bus. */
export interface TestEventBusCall {
  /** Fully built event name. */
  event: string
  /** Payload passed to the event bus. */
  payload: unknown
}

/**
 * In-memory event bus with test instrumentation.
 *
 * It can be passed directly to dependencies that accept an {@link EventBus}.
 */
export interface TestEventBus extends EventBus {
  /** Successful sends recorded in call order. */
  readonly calls: readonly TestEventBusCall[]
  /** Returns a snapshot of payloads sent to a channel created by {@link createEventChannelFactory} under `key`. */
  payloadsFor<TPayload, TKey>(
    channel: EventChannel<TPayload, TKey>,
    key: TKey,
  ): readonly TPayload[]
  /** Clears the recorded calls without affecting active subscriptions. */
  clearCalls(): void
  /** Returns the number of active event subscriptions. */
  getActiveSubscriptionCount(): number
}

/**
 * Creates an in-memory event bus that records sends and delivers them to active subscribers.
 *
 * The bus supports consumer cancellation and closes all active streams when closed.
 */
export function createTestEventBus(): TestEventBus {
  const eventEmitter = new EventEmitter().setMaxListeners(0)
  const busAbort = new AbortController()
  const calls: TestEventBusCall[] = []
  let activeSubscriptionCount = 0

  async function send(event: string, payload: unknown) {
    if (busAbort.signal.aborted) {
      throw new Error("Cannot send an event after the test event bus is closed")
    }

    deliver(event, payload)
  }

  async function sendMany(events: readonly EventBusEvent[]) {
    if (busAbort.signal.aborted) {
      throw new Error("Cannot send events after the test event bus is closed")
    }

    for (const { event, payload } of events) {
      deliver(event, payload)
    }
  }

  function deliver(event: string, payload: unknown) {
    calls.push({ event, payload })
    eventEmitter.emit(event, payload)
  }

  async function* onEvent<TPayload>(event: string, signal?: AbortSignal) {
    activeSubscriptionCount += 1

    try {
      yield* streamEvents<TPayload>(
        eventEmitter,
        event,
        busAbort.signal,
        signal,
      )
    } finally {
      activeSubscriptionCount -= 1
    }
  }

  async function close() {
    busAbort.abort()
  }

  function payloadsFor<TPayload, TKey>(
    channel: EventChannel<TPayload, TKey>,
    key: TKey,
  ): readonly TPayload[] {
    const event = resolveEventChannelName(channel, key)

    return calls
      .filter((call) => call.event === event)
      .map((call) => call.payload as TPayload)
  }

  return {
    ready: Promise.resolve(),
    send,
    sendMany,
    on: onEvent,
    close,
    calls,
    payloadsFor,
    clearCalls() {
      calls.length = 0
    },
    getActiveSubscriptionCount() {
      return activeSubscriptionCount
    },
  }
}
