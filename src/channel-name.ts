type BuildEventChannelName = (key: never) => string

const eventNames = new WeakMap<object, BuildEventChannelName>()

export function registerEventChannel(
  channel: object,
  buildName: BuildEventChannelName,
) {
  eventNames.set(channel, buildName)
}

export function resolveEventChannelName(channel: object, key?: unknown) {
  const buildName = eventNames.get(channel)

  if (buildName === undefined) {
    throw new TypeError(
      "Expected an event channel created by createEventChannelFactory",
    )
  }

  return buildName(key as never)
}
