import { type EventEmitter, on } from "node:events"

export async function* streamEvents<TPayload>(
  events: EventEmitter,
  event: string,
  busSignal: AbortSignal,
  userSignal?: AbortSignal,
): AsyncGenerator<TPayload, void, unknown> {
  const signal = userSignal
    ? AbortSignal.any([busSignal, userSignal])
    : busSignal

  try {
    for await (const [payload] of on(events, event, { signal })) {
      yield payload as TPayload
    }
  } catch (error) {
    if (busSignal.aborted && !isAbortError(busSignal.reason)) {
      throw busSignal.reason
    }

    if (isAbortError(error)) {
      return
    }

    throw error
  }
}

function isAbortError(error: unknown): error is Error {
  return error instanceof Error && error.name === "AbortError"
}
