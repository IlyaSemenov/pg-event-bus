import { EventEmitter } from "node:events"

import type { EventBus } from "./channel"
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
  const events = new EventEmitter().setMaxListeners(0)
  const busAbort = new AbortController()
  const calls: TestEventBusCall[] = []
  let activeSubscriptionCount = 0

  async function send(event: string, payload: unknown) {
    if (busAbort.signal.aborted) {
      throw new Error("Cannot send an event after the test event bus is closed")
    }

    calls.push({ event, payload })
    events.emit(event, payload)
  }

  async function* onEvent<TPayload>(event: string, signal?: AbortSignal) {
    activeSubscriptionCount += 1

    try {
      yield* streamEvents<TPayload>(events, event, busAbort.signal, signal)
    } finally {
      activeSubscriptionCount -= 1
    }
  }

  async function close() {
    busAbort.abort()
  }

  return {
    ready: Promise.resolve(),
    send,
    on: onEvent,
    close,
    calls,
    clearCalls() {
      calls.length = 0
    },
    getActiveSubscriptionCount() {
      return activeSubscriptionCount
    },
  }
}
