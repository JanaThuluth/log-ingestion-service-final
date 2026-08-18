import { pool } from "../db/pool";
import type { LogEntry } from "../schemas/logs";

export type LogQuery = {
  service?: string;
  level?: "debug" | "info" | "warn" | "error";
  since?: string;
  until?: string;
  attributes?: Record<string, string>;
  q?: string;
  limit: number;
  cursor?: {
    timestamp: string;
    id: string;
  };
};

export type LogRow = {
  id: string;
  timestamp: string;
  level: string;
  service: string;
  message: string;
  attributes: Record<string, string | number | boolean>;
};

export type QueryLogsResult = {
  logs: LogRow[];
  hasMore: boolean;
};

export async function insertLogs(logs: LogEntry[]): Promise<number> {
  if (logs.length === 0) {
    return 0;
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const values: unknown[] = [];
    const placeholders: string[] = [];

    let parameterIndex = 1;

    for (const log of logs) {
      placeholders.push(
        `($${parameterIndex}, $${parameterIndex + 1}, $${parameterIndex + 2}, $${parameterIndex + 3}, $${parameterIndex + 4})`,
      );

      values.push(
        log.timestamp,
        log.level,
        log.service,
        log.message,
        JSON.stringify(log.attributes),
      );

      parameterIndex += 5;
    }

    await client.query(
      `
        INSERT INTO logs (
          timestamp,
          level,
          service,
          message,
          attributes
        )
        VALUES ${placeholders.join(", ")}
      `,
      values,
    );

    await client.query("COMMIT");

    return logs.length;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function queryLogs(
  query: LogQuery,
): Promise<QueryLogsResult> {
  const conditions: string[] = [];
  const values: unknown[] = [];

  let parameterIndex = 1;

  if (query.service !== undefined) {
    conditions.push(`service = $${parameterIndex}`);
    values.push(query.service);
    parameterIndex++;
  }

  if (query.level !== undefined) {
    conditions.push(`level = $${parameterIndex}`);
    values.push(query.level);
    parameterIndex++;
  }

  if (query.since !== undefined) {
    conditions.push(`timestamp >= $${parameterIndex}`);
    values.push(query.since);
    parameterIndex++;
  }

  if (query.until !== undefined) {
    conditions.push(`timestamp < $${parameterIndex}`);
    values.push(query.until);
    parameterIndex++;
  }

  if (query.q !== undefined) {
    conditions.push(`message ILIKE $${parameterIndex}`);
    values.push(`%${query.q}%`);
    parameterIndex++;
  }

  if (query.attributes) {
    for (const [key, value] of Object.entries(query.attributes)) {
      conditions.push(
        `attributes ->> $${parameterIndex} = $${parameterIndex + 1}`,
      );

      values.push(key, value);
      parameterIndex += 2;
    }
  }

  if (query.cursor) {
    conditions.push(
      `(timestamp, id) < ($${parameterIndex}, $${parameterIndex + 1})`,
    );

    values.push(query.cursor.timestamp, query.cursor.id);
    parameterIndex += 2;
  }

  const whereClause =
    conditions.length > 0
      ? `WHERE ${conditions.join(" AND ")}`
      : "";

  const result = await pool.query<LogRow>(
    `
      SELECT
        id::text AS id,
        timestamp,
        level,
        service,
        message,
        attributes
      FROM logs
      ${whereClause}
      ORDER BY timestamp DESC, id DESC
      LIMIT $${parameterIndex}
    `,
    [...values, query.limit + 1],
  );

  const hasMore = result.rows.length > query.limit;

  return {
    logs: hasMore
      ? result.rows.slice(0, query.limit)
      : result.rows,
    hasMore,
  };
}