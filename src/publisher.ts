/** One encoded PostgreSQL notification passed to an application's publisher. */
export interface PgNotification {
  /** PostgreSQL channel supplied when the event bus was created. */
  channel: string
  /** JSON-encoded event name and payload accepted by `pg_notify`. */
  payload: string
}

/**
 * Publishes encoded notifications in one call through an application-owned database connection.
 *
 * Notifications are ordered as passed to `sendMany()`, and the publisher must preserve that order.
 * The returned value is awaited so a transaction cannot finish before publication completes.
 */
export type PgEventPublisher = (
  notifications: readonly PgNotification[],
) => unknown | PromiseLike<unknown>

/** Parameterized PostgreSQL query produced for a publisher. */
export interface PgPublisherQuery {
  /** SQL text containing PostgreSQL-style positional parameters. */
  text: string
  /** Values corresponding to the positional parameters in `text`. */
  values: string[]
}

/** Executes a publisher query through an application-owned database connection. */
export type ExecutePgPublisherQuery = (
  query: PgPublisherQuery,
) => unknown | PromiseLike<unknown>

const publishNotificationsText = `SELECT pg_notify(
  notification.value->>'channel',
  notification.value->>'payload'
)
FROM jsonb_array_elements($1::text::jsonb)
WITH ORDINALITY AS notification(value, position)
ORDER BY notification.position`

/**
 * Creates a publisher that runs the package-owned `pg_notify` query through the application's current database connection.
 */
export function createPublisher(
  executeQuery: ExecutePgPublisherQuery,
): PgEventPublisher {
  return (notifications) =>
    executeQuery({
      text: publishNotificationsText,
      values: [JSON.stringify(notifications)],
    })
}

/** Creates a publisher from a callback that receives encoded notifications directly. */
export function createRawPublisher(
  publisher: PgEventPublisher,
): PgEventPublisher {
  return publisher
}
