/** One typed event passed to an {@link EventChannel} batch. */
export interface EventChannelEvent<TPayload, TKey = string> {
  /** Key used to build the concrete event name. */
  key: TKey
  /** Payload published under the built event name. */
  payload: TPayload
}

/**
 * Typed handle for one family of domain events.
 *
 * A key selects the concrete event name, while every event in the channel shares one payload type.
 */
export interface EventChannel<TPayload, TKey = string> {
  /** Publishes a payload under the event name built from `key`. */
  send(key: TKey, payload: TPayload): Promise<void>
  /** Publishes several typed events as one transport batch. */
  sendMany(events: readonly EventChannelEvent<TPayload, TKey>[]): Promise<void>
  /** Streams payloads published under the event name built from `key` until the signal aborts. */
  on(key: TKey, signal?: AbortSignal): AsyncGenerator<TPayload, void, unknown>
}

/** One event passed to an {@link EventBus} batch. */
export interface EventBusEvent {
  /** Fully built event name. */
  event: string
  /** Payload published under the event name. */
  payload: unknown
}

/**
 * Transport-independent event bus with lifecycle management.
 *
 * Implementations publish and consume fully built event names.
 */
export interface EventBus {
  /** Resolves when the event bus is ready to receive events. */
  ready: Promise<void>
  /** Publishes a payload under a fully built event name. */
  send(event: string, payload: unknown): Promise<void>
  /** Publishes several events as one transport batch. */
  sendMany(events: readonly EventBusEvent[]): Promise<void>
  /** Streams payloads published under a fully built event name until the signal aborts. */
  on<TPayload>(
    event: string,
    signal?: AbortSignal,
  ): AsyncGenerator<TPayload, void, unknown>
  /** Closes the event bus and completes its active event streams. */
  close(): Promise<void>
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
 * Creates a channel factory bound to one event bus.
 */
export function createEventChannelFactory(
  eventBus: EventBus,
): DefineEventChannel

/**
 * Creates a channel factory backed by a lazily resolved event bus.
 *
 * The resolver runs for every `send()`, `sendMany()`, and `on()` call so dependency-injection overrides apply to channels declared earlier.
 */
export function createEventChannelFactory(
  getEventBus: () => EventBus,
): DefineEventChannel

export function createEventChannelFactory(
  eventBusOrResolver: EventBus | (() => EventBus),
): DefineEventChannel {
  const getEventBus =
    typeof eventBusOrResolver === "function"
      ? eventBusOrResolver
      : () => eventBusOrResolver

  const defineEventChannel: DefineEventChannel = (buildName) => ({
    send: (key, payload) => getEventBus().send(buildName(key), payload),
    sendMany: (events) =>
      getEventBus().sendMany(
        events.map(({ key, payload }) => ({
          event: buildName(key),
          payload,
        })),
      ),
    on: (key, signal) => getEventBus().on(buildName(key), signal),
  })

  return defineEventChannel
}
