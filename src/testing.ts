import { EventEmitter } from "node:events"

import type {
  EventBus,
  EventBusEvent,
  EventChannel,
  KeyedEventChannel,
} from "./channel"
import { resolveEventChannelName } from "./channel-name"
import { createDeliveryGaps } from "./gaps"
import { streamEvents } from "./stream"

/** One event sent through a test event bus. */
export interface TestEventBusCall {
  /** Fully built event name. */
  event: string
  /** Payload passed to the event bus. */
  payload: unknown
}

/** Test instrumentation bound to one typed event channel with a fixed event name. */
export interface TestEventChannelInspector<TPayload> {
  /**
   * Returns recorded payloads, optionally narrowed to a compatible subtype.
   * An explicitly supplied subtype is trusted and is not validated at runtime.
   */
  payloadsFor<TEvent extends TPayload = TPayload>(): readonly TEvent[]
}

/** Test instrumentation bound to one typed keyed event channel. */
export interface TestKeyedEventChannelInspector<TPayload, TKey> {
  /**
   * Returns payloads recorded under `key`, optionally narrowed to a compatible subtype.
   * An explicitly supplied subtype is trusted and is not validated at runtime.
   */
  payloadsFor<TEvent extends TPayload = TPayload>(key: TKey): readonly TEvent[]
}

/**
 * In-memory event bus with test instrumentation.
 *
 * It can be passed directly to dependencies that accept an {@link EventBus}.
 */
export interface TestEventBus extends EventBus {
  /** Successful sends recorded in call order. */
  readonly calls: readonly TestEventBusCall[]
  /** Returns a snapshot of payloads sent to a fixed-name channel created by {@link createEventChannelFactory}. */
  payloadsFor<TPayload>(channel: EventChannel<TPayload>): readonly TPayload[]
  /** Returns a snapshot of payloads sent to a channel created by {@link createEventChannelFactory} under `key`. */
  payloadsFor<TPayload, TKey>(
    channel: KeyedEventChannel<TPayload, TKey>,
    key: TKey,
  ): readonly TPayload[]
  /** Returns test instrumentation bound to a fixed-name `channel`. */
  for<TPayload>(
    channel: EventChannel<TPayload>,
  ): TestEventChannelInspector<TPayload>
  /** Returns test instrumentation bound to `channel`. */
  for<TPayload, TKey>(
    channel: KeyedEventChannel<TPayload, TKey>,
  ): TestKeyedEventChannelInspector<TPayload, TKey>
  /** Clears the recorded calls without affecting active subscriptions. */
  clearCalls(): void
  /** Returns the number of active event subscriptions. */
  getActiveSubscriptionCount(): number
  /** Simulates a possible delivery gap after a PostgreSQL listener reconnects. */
  simulateDeliveryGap(): void
}

/**
 * Creates an in-memory event bus that records sends and delivers them to active subscribers.
 *
 * The bus supports consumer cancellation and closes all active streams when closed.
 */
export function createTestEventBus(): TestEventBus {
  const eventEmitter = new EventEmitter().setMaxListeners(0)
  const busAbort = new AbortController()
  const deliveryGaps = createDeliveryGaps(busAbort.signal)
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

  function recordedPayloadsFor<TPayload>(channel: object, key?: unknown) {
    const event = resolveEventChannelName(channel, key)

    return calls
      .filter((call) => call.event === event)
      .map((call) => call.payload as TPayload)
  }

  function payloadsFor<TPayload>(
    channel: EventChannel<TPayload>,
  ): readonly TPayload[]
  function payloadsFor<TPayload, TKey>(
    channel: KeyedEventChannel<TPayload, TKey>,
    key: TKey,
  ): readonly TPayload[]
  function payloadsFor<TPayload, TKey>(
    channel: EventChannel<TPayload> | KeyedEventChannel<TPayload, TKey>,
    key?: TKey,
  ): readonly TPayload[] {
    return recordedPayloadsFor<TPayload>(channel, key)
  }

  function inspect<TPayload>(
    channel: EventChannel<TPayload>,
  ): TestEventChannelInspector<TPayload>
  function inspect<TPayload, TKey>(
    channel: KeyedEventChannel<TPayload, TKey>,
  ): TestKeyedEventChannelInspector<TPayload, TKey>
  function inspect<TPayload, TKey>(
    channel: EventChannel<TPayload> | KeyedEventChannel<TPayload, TKey>,
  ):
    | TestEventChannelInspector<TPayload>
    | TestKeyedEventChannelInspector<TPayload, TKey> {
    const inspector = {
      payloadsFor: <TEvent extends TPayload = TPayload>(key?: TKey) =>
        recordedPayloadsFor<TEvent>(channel, key),
    }

    return inspector
  }

  return {
    ready: Promise.resolve(),
    send,
    sendMany,
    on: onEvent,
    deliveryGaps: deliveryGaps.stream,
    close,
    [Symbol.asyncDispose]: close,
    calls,
    payloadsFor,
    for: inspect,
    clearCalls() {
      calls.length = 0
    },
    getActiveSubscriptionCount() {
      return activeSubscriptionCount
    },
    simulateDeliveryGap() {
      if (busAbort.signal.aborted) {
        throw new Error(
          "Cannot simulate a delivery gap after the test event bus is closed",
        )
      }

      void deliveryGaps.emit()
    },
  }
}
