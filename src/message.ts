import { randomUUID } from "node:crypto"

export interface EventMessage {
  event: string
  payload?: unknown
}

/** One part of an event message that does not fit into a single PostgreSQL notification. */
interface ChunkMessage {
  /** Identifier shared by all chunks of one event message. */
  chunk: string
  index: number
  count: number
  /** Consecutive part of the encoded event message. */
  data: string
}

/** Largest PostgreSQL notification payload in UTF-8 bytes with the default 8 KB block size. */
const maxNotificationBytes = 7999

/** Maximum encoded chunk data size in UTF-8 bytes, leaving room for the chunk envelope. */
const maxChunkDataBytes = 7800

/**
 * Largest number of chunks in one event, which bounds both event size and listener reassembly memory to about 1 MB of chunk data.
 *
 * Chunk data escapes the event JSON once more, so quotes and backslashes in the event JSON count twice toward that limit.
 */
const maxChunkCount = 128

export function encodeEventMessage(event: string, payload: unknown) {
  const message: EventMessage = { event, payload }
  return JSON.stringify(message)
}

/**
 * Encodes an event as PostgreSQL notification payloads.
 *
 * An event that does not fit into one notification is split into consecutive chunk notifications,
 * which must be published in this order within one transaction.
 * Throws for an event that needs more chunks than listeners reassemble.
 */
export function encodeEventNotifications(event: string, payload: unknown): string[] {
  const message = encodeEventMessage(event, payload)

  if (Buffer.byteLength(message) <= maxNotificationBytes) {
    return [message]
  }

  // A unique identifier also keeps PostgreSQL from coalescing equal chunks of repeated events.
  const id = randomUUID()
  const parts = splitByEncodedBytes(message, maxChunkDataBytes)

  if (parts.length > maxChunkCount) {
    throw new Error(
      `Cannot send event "${event}": its encoded size exceeds the limit of about 1 MB`,
    )
  }

  return parts.map((data, index) => {
    const chunk: ChunkMessage = { chunk: id, index, count: parts.length, data }
    return JSON.stringify(chunk)
  })
}

/**
 * Splits `text` into parts whose JSON string encodings, excluding the surrounding quotes, take at most `maxBytes` UTF-8 bytes each.
 *
 * Parts never split a code point, so every part remains a valid PostgreSQL text value.
 */
function splitByEncodedBytes(text: string, maxBytes: number) {
  const parts: string[] = []
  let start = 0
  let end = 0
  let bytes = 0

  for (const char of text) {
    const charBytes = getEncodedByteLength(char)

    if (bytes + charBytes > maxBytes) {
      parts.push(text.slice(start, end))
      start = end
      bytes = 0
    }

    bytes += charBytes
    end += char.length
  }

  parts.push(text.slice(start))
  return parts
}

/** Returns the UTF-8 byte length of one code point, or lone surrogate, inside a JSON string. */
function getEncodedByteLength(char: string) {
  const codePoint = char.codePointAt(0)!

  if (
    codePoint < 0x20 ||
    char === '"' ||
    char === "\\" ||
    (codePoint >= 0xd800 && codePoint < 0xe000)
  ) {
    // JSON escapes are ASCII, so their length equals their byte length.
    return JSON.stringify(char).length - 2
  }

  return codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4
}

export function decodeEventMessage(payload: string): EventMessage | undefined {
  const message = parseJson(payload)
  return isEventMessage(message) ? message : undefined
}

/**
 * Creates a decoder for consecutive notifications of one PostgreSQL channel.
 *
 * The decoder returns an event message for a single-notification event or the last chunk of a split one.
 * PostgreSQL delivers the notifications of one transaction contiguously and in order,
 * so any notification other than the next expected chunk discards an incomplete event.
 */
export function createNotificationDecoder() {
  let pending: { id: string; count: number; parts: string[] } | undefined

  return (payload: string): EventMessage | undefined => {
    const message = parseJson(payload)

    // An event message is whole even when it also has chunk fields.
    if (isEventMessage(message)) {
      pending = undefined
      return message
    }

    if (!isChunkMessage(message)) {
      pending = undefined
      return
    }

    if (message.index === 0) {
      pending =
        message.count > 0 && message.count <= maxChunkCount
          ? { id: message.chunk, count: message.count, parts: [] }
          : undefined
    }

    if (
      pending?.id !== message.chunk ||
      pending.count !== message.count ||
      pending.parts.length !== message.index
    ) {
      pending = undefined
      return
    }

    pending.parts.push(message.data)

    if (pending.parts.length < pending.count) {
      return
    }

    const encoded = pending.parts.join("")
    pending = undefined
    return decodeEventMessage(encoded)
  }
}

export function isEventMessage(value: unknown): value is EventMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    "event" in value &&
    typeof value.event === "string"
  )
}

function isChunkMessage(value: unknown): value is ChunkMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    "chunk" in value &&
    typeof value.chunk === "string" &&
    "index" in value &&
    Number.isInteger(value.index) &&
    "count" in value &&
    Number.isInteger(value.count) &&
    "data" in value &&
    typeof value.data === "string"
  )
}

/** Parses JSON text, returning `undefined` when parsing fails. */
function parseJson(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return undefined
  }
}
