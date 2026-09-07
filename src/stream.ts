import { addAbortListener, type EventEmitter } from "node:events"

/**
 * Subscribes immediately and queues payloads until the consumer reads them.
 * Cancellation and failure preserve buffered values; return discards them.
 * A bus failure rejects one read after the buffer drains, while consumer abort completes normally.
 */
export function streamEvents<TPayload>(
  eventEmitter: EventEmitter,
  event: string | symbol,
  busSignal: AbortSignal,
  userSignal?: AbortSignal,
): AsyncGenerator<TPayload, void, unknown> {
  // Values and waiting reads are mutually exclusive queues, both consumed in FIFO order.
  const buffer: TPayload[] = []
  const pending: {
    resolve: (result: IteratorResult<TPayload, void>) => void
    reject: (error: unknown) => void
  }[] = []
  // Detached streams may still have buffered values or one undelivered failure.
  let listening = false
  // A rejection reason can itself be undefined, so it cannot indicate failure presence.
  let failed = false
  let failure: unknown
  let busAbortListener: Disposable | undefined
  let userAbortListener: Disposable | undefined
  const done = { value: undefined, done: true } as const

  function cleanup() {
    if (!listening) {
      return
    }

    listening = false
    eventEmitter.off(event, receive)
    if (event !== "error") {
      eventEmitter.off("error", fail)
    }
    busAbortListener?.[Symbol.dispose]()
    userAbortListener?.[Symbol.dispose]()
  }

  function finish(error?: unknown, hasError = false) {
    // The first terminal signal wins; later cancellation must not overwrite a failure.
    if (!listening) {
      return
    }

    cleanup()
    failed = hasError
    failure = error
    // Reject at most one read, retaining the failure if no reader is waiting yet.
    const first = pending.shift()
    if (first) {
      if (failed) {
        failed = false
        failure = undefined
        first.reject(error)
      } else {
        first.resolve(done)
      }
    }
    for (const request of pending.splice(0)) {
      request.resolve(done)
    }
  }

  function receive(payload: TPayload) {
    // EventEmitter may have captured this callback before another listener cancelled us.
    if (!listening) {
      return
    }

    const request = pending.shift()
    if (request) {
      request.resolve({ value: payload, done: false })
    } else {
      buffer.push(payload)
    }
  }

  function fail(error: unknown) {
    finish(error, !isAbortError(error))
  }

  // An already cancelled consumer never subscribes, even if the bus has failed.
  if (!userSignal?.aborted) {
    if (busSignal.aborted) {
      failed = !isAbortError(busSignal.reason)
      failure = busSignal.reason
    } else {
      listening = true
      eventEmitter.on(event, receive)
      if (event !== "error") {
        eventEmitter.on("error", fail)
      }
      busAbortListener = addAbortListener(busSignal, () =>
        fail(busSignal.reason),
      )
      if (userSignal) {
        userAbortListener = addAbortListener(userSignal, () => finish())
      }
    }
  }

  // An async generator would defer registration and queue return() behind a pending next().
  const stream: AsyncGenerator<TPayload, void, unknown> = {
    async next() {
      // Drain accepted values before reporting failure or normal completion.
      if (buffer.length) {
        return { value: buffer.shift() as TPayload, done: false }
      }
      if (failed) {
        failed = false
        const error = failure
        failure = undefined
        throw error
      }
      if (!listening) {
        return done
      }
      return new Promise<IteratorResult<TPayload, void>>((resolve, reject) => {
        pending.push({ resolve, reject })
      })
    },
    async return(value) {
      // Explicit iterator exit also discards values and errors retained after an earlier abort.
      finish()
      buffer.length = 0
      failed = false
      failure = undefined
      return { value: await value, done: true }
    },
    async throw(error) {
      await stream.return()
      throw error
    },
    [Symbol.asyncIterator]() {
      return this
    },
    async [Symbol.asyncDispose]() {
      await stream.return()
    },
  }
  return stream
}

function isAbortError(error: unknown): error is Error {
  return error instanceof Error && error.name === "AbortError"
}
