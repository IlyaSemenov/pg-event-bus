import { expect, it } from "bun:test"

import {
  createPublisher,
  createRawPublisher,
  type PgNotification,
  type PgPublisherQuery,
} from "./publisher"

it("builds one parameterized query for a notification batch", async () => {
  const queries: PgPublisherQuery[] = []
  const publisher = createPublisher((query) => {
    queries.push(query)
  })
  const notifications = [
    { channel: "application", payload: '{"event":"one"}' },
    { channel: "application", payload: '{"event":"two"}' },
  ]

  await publisher(notifications)

  expect(queries).toHaveLength(1)
  expect(queries[0]?.text).toContain("SELECT pg_notify")
  expect(queries[0]?.text).toContain("$1::text::jsonb")
  expect(queries[0]?.values).toEqual([JSON.stringify(notifications)])
})

it("passes encoded notifications directly to a raw publisher", async () => {
  let received: readonly PgNotification[] | undefined
  const publisher = createRawPublisher((notifications) => {
    received = notifications
  })
  const notifications = [{ channel: "application", payload: "payload" }]

  await publisher(notifications)

  expect(received).toBe(notifications)
})
