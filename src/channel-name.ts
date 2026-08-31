type EventChannelNameDefinition = string | ((key: never) => string)

const eventNames = new WeakMap<object, EventChannelNameDefinition>()

export function registerEventChannel(
  channel: object,
  eventOrBuildName: EventChannelNameDefinition,
) {
  eventNames.set(channel, eventOrBuildName)
}

export function resolveEventChannelName(channel: object, key?: unknown) {
  const eventOrBuildName = eventNames.get(channel)

  if (eventOrBuildName === undefined) {
    throw new TypeError(
      "Expected an event channel created by createEventChannelFactory",
    )
  }

  return typeof eventOrBuildName === "string"
    ? eventOrBuildName
    : eventOrBuildName(key as never)
}
