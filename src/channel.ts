import { registerEventChannel } from "./channel-name"

/**
 * Typed handle for one domain event with a fixed event name.
 *
 * Every operation uses the same event name, so the application does not supply a key.
 * The factory's `scopeEventName` may still map it to a different bus name depending on the operation's context.
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
  /**
   * Streams possible delivery gaps reported after the listener reconnects until the signal aborts.
   * The initial connection does not produce a gap signal.
   */
  deliveryGaps(signal?: AbortSignal): AsyncGenerator<void, void, unknown>
  /** Closes the event bus and completes its active event streams. */
  close(): Promise<void>
}

/**
 * Defines a typed event channel with either a fixed event name or a function that maps keys to event names.
 */
export interface DefineEventChannel {
  /** Defines a channel whose operations use the supplied event name without a key. */
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

/** Options shared by every channel created by one channel factory. */
export interface EventChannelFactoryOptions {
  /**
   * Maps the event name of every channel to the name used by the bus.
   *
   * It runs for every `send()`, `sendMany()`, and `on()` call and when a test event bus inspects a channel, so it may read the current context, such as the active tenant, to isolate events of separate scopes.
   */
  scopeEventName?: (event: string) => string
}

/**
 * Creates a channel factory bound to one event bus.
 */
export function createEventChannelFactory(
  eventBus: EventBus,
  options?: EventChannelFactoryOptions,
): DefineEventChannel

/**
 * Creates a channel factory backed by a lazily resolved event bus.
 *
 * The resolver runs for every `send()`, `sendMany()`, and `on()` call so dependency-injection overrides apply to channels declared earlier.
 */
export function createEventChannelFactory(
  getEventBus: () => EventBus,
  options?: EventChannelFactoryOptions,
): DefineEventChannel

export function createEventChannelFactory(
  eventBusOrResolver: EventBus | (() => EventBus),
  { scopeEventName }: EventChannelFactoryOptions = {},
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
    const buildEventName =
      typeof eventOrBuildName === "string"
        ? () => eventOrBuildName
        : eventOrBuildName
    const buildName = scopeEventName
      ? (key: TKey) => scopeEventName(buildEventName(key))
      : buildEventName
    const channel =
      typeof eventOrBuildName === "string"
        ? createEventChannel<TPayload>(getEventBus, () =>
            buildName(undefined as TKey),
          )
        : createKeyedEventChannel<TPayload, TKey>(getEventBus, buildName)

    registerEventChannel(channel, buildName)

    return channel
  }

  return defineEventChannel
}

function createEventChannel<TPayload>(
  getEventBus: () => EventBus,
  getEvent: () => string,
): EventChannel<TPayload> {
  return {
    send: (payload) => getEventBus().send(getEvent(), payload),
    sendMany: (payloads) => {
      const event = getEvent()
      return getEventBus().sendMany(
        payloads.map((payload) => ({ event, payload })),
      )
    },
    on: <TEvent extends TPayload = TPayload>(signal?: AbortSignal) =>
      getEventBus().on<TEvent>(getEvent(), signal),
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
