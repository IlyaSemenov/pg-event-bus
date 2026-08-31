import { EventEmitter } from "node:events"

import { streamEvents } from "./stream"

export function createDeliveryGaps(busSignal: AbortSignal) {
  const eventEmitter = new EventEmitter().setMaxListeners(0)
  const deliveryGapEvent = Symbol("deliveryGap")

  return {
    emit: () => eventEmitter.emit(deliveryGapEvent),
    stream: (signal?: AbortSignal) =>
      streamEvents<void>(eventEmitter, deliveryGapEvent, busSignal, signal),
  }
}
