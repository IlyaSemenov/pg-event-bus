import { registerEventChannel } from "./channel-name"

/**
 * Typed handle for one domain event with a fixed transport name.
 *
 * Every operation uses the same event name, so the application does not supply a key.
 */
export interface EventChannel<TPayload> {
  /** Publishes one payload. */
  send(payload: TPayload): Promise<void>
  /** Publishes several payloads as one transport batch. */
  sendMany(payloads: readonly TPayload[]): Promise<void>
  /** Streams payloads until the signal aborts. */
  on<TEvent extends TPayload = TPayload>(
    signal?: AbortSignal,
  ): AsyncGenerator<TEvent, void, unknown>
}

/** One typed event passed to a {@link KeyedEventChannel} batch. */
export interface KeyedEventChannelEvent<TPayload, TKey = string> {
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
export interface KeyedEventChannel<TPayload, TKey = string> {
  /** Publishes a payload under the event name built from `key`. */
  send(key: TKey, payload: TPayload): Promise<void>
  /** Publishes several typed events as one transport batch. */
  sendMany(
    events: readonly KeyedEventChannelEvent<TPayload, TKey>[],
  ): Promise<void>
  /**
   * Streams payloads published under the event name built from `key` until the signal aborts.
   *
   * Pass a compatible subtype explicitly when the application contract associates the key with a narrower part of the channel payload.
   * This key-to-subtype relationship is trusted and is not validated at runtime.
   */
  on<TEvent extends TPayload = TPayload>(
    key: TKey,
    signal?: AbortSignal,
  ): AsyncGenerator<TEvent, void, unknown>
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
export interface EventBus extends AsyncDisposable {
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
  /** Streams possible delivery gaps reported after the listener reconnects until the signal aborts. */
  deliveryGaps(signal?: AbortSignal): AsyncGenerator<void, void, unknown>
  /** Closes the event bus and completes its active event streams. */
  close(): Promise<void>
}

/**
 * Defines a typed event channel with either a fixed event name or a function that maps keys to event names.
 */
export interface DefineEventChannel {
  /** Defines a channel whose operations always use the supplied event name. */
  <TPayload>(event: string): EventChannel<TPayload>
  /**
   * Defines a family of events whose key is mapped to a concrete event name.
   *
   * Event keys are strings unless a different `TKey` is supplied.
   */
  <TPayload, TKey = string>(
    buildName: (key: TKey) => string,
  ): KeyedEventChannel<TPayload, TKey>
}

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

  function defineEventChannel<TPayload>(event: string): EventChannel<TPayload>
  function defineEventChannel<TPayload, TKey = string>(
    buildName: (key: TKey) => string,
  ): KeyedEventChannel<TPayload, TKey>
  function defineEventChannel<TPayload, TKey = string>(
    eventOrBuildName: string | ((key: TKey) => string),
  ): EventChannel<TPayload> | KeyedEventChannel<TPayload, TKey> {
    const channel =
      typeof eventOrBuildName === "string"
        ? createEventChannel<TPayload>(getEventBus, eventOrBuildName)
        : createKeyedEventChannel<TPayload, TKey>(getEventBus, eventOrBuildName)

    registerEventChannel(channel, eventOrBuildName)

    return channel
  }

  return defineEventChannel
}

function createEventChannel<TPayload>(
  getEventBus: () => EventBus,
  event: string,
): EventChannel<TPayload> {
  return {
    send: (payload) => getEventBus().send(event, payload),
    sendMany: (payloads) =>
      getEventBus().sendMany(payloads.map((payload) => ({ event, payload }))),
    on: <TEvent extends TPayload = TPayload>(signal?: AbortSignal) =>
      getEventBus().on<TEvent>(event, signal),
  }
}

function createKeyedEventChannel<TPayload, TKey>(
  getEventBus: () => EventBus,
  buildName: (key: TKey) => string,
): KeyedEventChannel<TPayload, TKey> {
  return {
    send: (key, payload) => getEventBus().send(buildName(key), payload),
    sendMany: (events) =>
      getEventBus().sendMany(
        events.map(({ key, payload }) => ({
          event: buildName(key),
          payload,
        })),
      ),
    on: <TEvent extends TPayload = TPayload>(key: TKey, signal?: AbortSignal) =>
      getEventBus().on<TEvent>(buildName(key), signal),
  }
}
