/**
 * Typed handle for one family of domain events.
 *
 * A key selects the concrete event name, while every event in the channel shares one payload type.
 */
export interface EventChannel<TPayload, TKey = string> {
  /** Publishes a payload under the event name built from `key`. */
  send(key: TKey, payload: TPayload): Promise<void>
  /** Streams payloads published under the event name built from `key` until the signal aborts. */
  on(key: TKey, signal?: AbortSignal): AsyncGenerator<TPayload, void, unknown>
}

/**
 * Transport-independent operations used by typed event channels.
 *
 * Implementations publish and consume fully built event names.
 */
export interface EventBus {
  /** Publishes a payload under a fully built event name. */
  send(event: string, payload: unknown): Promise<void>
  /** Streams payloads published under a fully built event name until the signal aborts. */
  on<TPayload>(
    event: string,
    signal?: AbortSignal,
  ): AsyncGenerator<TPayload, void, unknown>
}

/**
 * Defines a typed event channel from a function that maps its key to an event name.
 *
 * Event keys are strings unless a different `TKey` is supplied.
 */
export type DefineEventChannel = <TPayload, TKey = string>(
  buildName: (key: TKey) => string,
) => EventChannel<TPayload, TKey>

/**
 * Creates a channel factory backed by a lazily resolved event bus.
 *
 * The resolver runs for every `send()` and `on()` call so dependency-injection overrides apply to channels declared earlier.
 */
export function createEventChannelFactory(
  getEventBus: () => EventBus,
): DefineEventChannel {
  function defineEventChannel<TPayload, TKey = string>(
    buildName: (key: TKey) => string,
  ): EventChannel<TPayload, TKey> {
    return {
      send: (key, payload) => getEventBus().send(buildName(key), payload),
      on: (key, signal) => getEventBus().on<TPayload>(buildName(key), signal),
    }
  }

  return defineEventChannel
}
