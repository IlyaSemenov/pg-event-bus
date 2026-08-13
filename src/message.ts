export interface EventMessage {
  event: string
  payload?: unknown
}

export function encodeEventMessage(event: string, payload: unknown) {
  const message: EventMessage = { event, payload }
  return JSON.stringify(message)
}

export function decodeEventMessage(payload: string): EventMessage | undefined {
  let message: unknown

  try {
    message = JSON.parse(payload)
  } catch {
    return
  }

  return isEventMessage(message) ? message : undefined
}

export function isEventMessage(value: unknown): value is EventMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    "event" in value &&
    typeof value.event === "string"
  )
}
