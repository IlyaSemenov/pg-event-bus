import { describe, expect, it } from "bun:test"

import {
  createNotificationDecoder,
  decodeEventMessage,
  encodeEventMessage,
  encodeEventNotifications,
  isEventMessage,
} from "./message"

it("encodes the event name and payload as one PostgreSQL notification", () => {
  expect(encodeEventMessage("post:one", { kind: "updated" })).toBe(
    '{"event":"post:one","payload":{"kind":"updated"}}',
  )
})

it("recognizes only messages with a string event name", () => {
  expect(isEventMessage({ event: "post:one", payload: null })).toBe(true)
  expect(isEventMessage({ event: "without-payload" })).toBe(true)
  expect(isEventMessage({ event: 1, payload: null })).toBe(false)
  expect(isEventMessage(null)).toBe(false)
})

it("decodes a valid PostgreSQL notification", () => {
  expect(decodeEventMessage('{"event":"post:one","payload":{"kind":"updated"}}')).toEqual({
    event: "post:one",
    payload: { kind: "updated" },
  })
})

it("rejects malformed PostgreSQL notifications", () => {
  expect(decodeEventMessage("not-json")).toBeUndefined()
  expect(decodeEventMessage('{"event":1}')).toBeUndefined()
})

describe("split events", () => {
  const largePayloads = {
    ascii: "a".repeat(30_000),
    escaped: '"\\\n'.repeat(10_000),
    cyrillic: "привет ".repeat(5_000),
    cjk: "漢字".repeat(10_000),
    emoji: "😀".repeat(10_000),
    "lone surrogates": "a\ud800".repeat(10_000),
  }

  it("keeps an event of up to 7999 bytes whole and splits a larger one", () => {
    const envelopeBytes = encodeEventMessage("event", "").length
    const fitting = "x".repeat(7999 - envelopeBytes)
    const oversized = `${fitting}x`

    expect(encodeEventNotifications("event", fitting)).toEqual([
      encodeEventMessage("event", fitting),
    ])
    expect(encodeEventNotifications("event", oversized)).toHaveLength(2)
  })

  it("rejects an event whose chunks exceed about 1 MB, counting JSON quotes and backslashes twice", () => {
    const error = 'Cannot send event "event": its encoded size exceeds the limit of about 1 MB'

    expect(encodeEventNotifications("event", "x".repeat(990_000)).length).toBeGreaterThan(100)
    expect(() => encodeEventNotifications("event", "x".repeat(1_000_000))).toThrow(error)
    expect(() => encodeEventNotifications("event", '"'.repeat(300_000))).toThrow(error)
  })

  for (const [name, payload] of Object.entries(largePayloads)) {
    it(`splits a large ${name} event into notifications PostgreSQL accepts and reassembles it`, () => {
      const notifications = encodeEventNotifications("large", payload)
      const decode = createNotificationDecoder()

      expect(notifications.length).toBeGreaterThan(1)
      for (const notification of notifications) {
        expect(Buffer.byteLength(notification)).toBeLessThan(8000)
        expect(JSON.parse(notification).data.isWellFormed()).toBe(true)
      }
      expect(notifications.slice(0, -1).map(decode)).toEqual(
        notifications.slice(0, -1).map(() => undefined),
      )
      expect(decode(notifications.at(-1)!)).toEqual({ event: "large", payload })
    })
  }

  it("reassembles consecutive large events independently", () => {
    const decode = createNotificationDecoder()
    const notifications = [
      ...encodeEventNotifications("large", largePayloads.ascii),
      ...encodeEventNotifications("large", largePayloads.cyrillic),
    ]

    expect(notifications.map(decode).filter(Boolean)).toEqual([
      { event: "large", payload: largePayloads.ascii },
      { event: "large", payload: largePayloads.cyrillic },
    ])
  })

  it("discards a large event interrupted by another notification", () => {
    const decode = createNotificationDecoder()
    const chunks = encodeEventNotifications("large", largePayloads.ascii)
    const small = encodeEventMessage("small", null)

    expect(
      [...chunks.slice(0, -1), small, ...chunks.slice(-1)].map(decode).filter(Boolean),
    ).toEqual([{ event: "small", payload: null }])
  })

  const brokenSequences = {
    "chunks out of order": (chunks: string[]) => [chunks[0]!, ...chunks.slice(1).reverse()],
    "a tail without its head": (chunks: string[]) => chunks.slice(1),
    "a chunk with a zero count": (chunks: string[]) => [
      JSON.stringify({
        ...JSON.parse(chunks[0]!),
        count: 0,
        data: encodeEventMessage("other", null),
      }),
    ],
    "a chunk with a negative count": (chunks: string[]) => [
      JSON.stringify({
        ...JSON.parse(chunks[0]!),
        count: -1,
        data: encodeEventMessage("other", null),
      }),
    ],
    "the tail of another event": (chunks: string[]) => [
      ...chunks.slice(0, -1),
      encodeEventNotifications("other", "b".repeat(largePayloads.ascii.length)).at(-1)!,
    ],
  }

  for (const [name, breakSequence] of Object.entries(brokenSequences)) {
    it(`discards ${name} and reassembles the next event`, () => {
      const decode = createNotificationDecoder()
      const chunks = encodeEventNotifications("large", largePayloads.ascii)

      expect([...breakSequence(chunks), ...chunks].map(decode).filter(Boolean)).toEqual([
        { event: "large", payload: largePayloads.ascii },
      ])
    })
  }

  it("reassembles up to 128 chunks and discards longer sequences", () => {
    const decode = createNotificationDecoder()
    const encodeChunks = (count: number) => {
      const message = encodeEventMessage("event", "x".repeat(count * 10))
      const size = Math.ceil(message.length / count)
      return Array.from({ length: count }, (_, index) =>
        JSON.stringify({
          chunk: `id-${count}`,
          index,
          count,
          data: message.slice(index * size, (index + 1) * size),
        }),
      )
    }

    expect(encodeChunks(128).map(decode).filter(Boolean)).toEqual([
      { event: "event", payload: "x".repeat(1280) },
    ])
    expect(encodeChunks(129).map(decode).filter(Boolean)).toEqual([])
  })

  it("treats an event message with chunk fields as a whole event", () => {
    const decode = createNotificationDecoder()
    const notification = JSON.stringify({
      event: "whole",
      payload: 42,
      chunk: "id",
      index: 0,
      count: 1,
      data: encodeEventMessage("other", null),
    })

    expect(decode(notification)).toEqual(expect.objectContaining({ event: "whole", payload: 42 }))
  })
})
