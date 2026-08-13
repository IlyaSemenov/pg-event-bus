import { expect, it } from "bun:test"

import {
  decodeEventMessage,
  encodeEventMessage,
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
  expect(
    decodeEventMessage('{"event":"post:one","payload":{"kind":"updated"}}'),
  ).toEqual({ event: "post:one", payload: { kind: "updated" } })
})

it("rejects malformed PostgreSQL notifications", () => {
  expect(decodeEventMessage("not-json")).toBeUndefined()
  expect(decodeEventMessage('{"event":1}')).toBeUndefined()
})
