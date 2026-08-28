import type { EventChannel } from "./channel"

const eventNameBuilders = new WeakMap<object, (key: never) => string>()

export function registerEventChannel<TPayload, TKey>(
  channel: EventChannel<TPayload, TKey>,
  buildName: (key: TKey) => string,
) {
  eventNameBuilders.set(channel, buildName)
}

export function resolveEventChannelName<TPayload, TKey>(
  channel: EventChannel<TPayload, TKey>,
  key: TKey,
) {
  const buildName = eventNameBuilders.get(channel)

  if (!buildName) {
    throw new TypeError(
      "Expected an event channel created by createEventChannelFactory",
    )
  }

  return buildName(key as never)
}
