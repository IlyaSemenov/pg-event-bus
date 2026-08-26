import type { EventBus } from "./channel"

/** Event bus with transport lifecycle management. */
export interface EventBusResource extends EventBus {
  /** Resolves when the event bus is ready to receive events. */
  ready: Promise<void>
  /** Closes the event bus and completes its active event streams. */
  close(): Promise<void>
}
