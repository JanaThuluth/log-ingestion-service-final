import { from as copyFrom } from "pg-copy-streams"; 
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
 
function escapeCsv(value: string): string { 
  return `"${value.replace(/"/g, '""')}"`; 
} 
 
export async function insertLogs(logs: LogEntry[]): Promise<number> { 
  if (logs.length === 0) { 
    return 0; 
  } 
 
  const client = await pool.connect(); 
 
  try { 
    await client.query("BEGIN"); 
 
    const copyStream = client.query( 
      copyFrom(` 
        COPY logs ( 
          timestamp, 
          level, 
          service, 
          message, 
          attributes 
        ) 
        FROM STDIN WITH (FORMAT csv) 
      `), 
    ); 
 
    for (const log of logs) { 
      const row = [ 
        escapeCsv(log.timestamp), 
        escapeCsv(log.level), 
        escapeCsv(log.service), 
        escapeCsv(log.message), 
        escapeCsv(JSON.stringify(log.attributes)), 
      ].join(","); 
 
      if (!copyStream.write(`${row}\n`)) { 
        await new Promise<void>((resolve, reject) => { 
          copyStream.once("drain", resolve); 
          copyStream.once("error", reject); 
        }); 
      } 
    } 
 
    copyStream.end(); 
 
    await new Promise<void>((resolve, reject) => { 
      copyStream.once("finish", resolve); 
      copyStream.once("error", reject); 
    }); 
 
    await client.query("COMMIT"); 
 
    return logs.length; 
  } catch (error) { 
    try { 
      await client.query("ROLLBACK"); 
    } catch { 
      // Keep the original error. 
    } 
 
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
      `attributes @> $${parameterIndex}::jsonb`, 
    ); 
 
    values.push( 
      JSON.stringify({ 
        [key]: value, 
      }), 
    ); 
 
    parameterIndex++; 
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

export type AggregateQuery = {
  service?: string;
  level?: "debug" | "info" | "warn" | "error";
  since: string;
  until: string;
  bucket: "1m" | "5m" | "1h" | "1d";
  groupBy?: "service" | "level";
  attributes?: Record<string, string>;
  q?: string;
};

export type AggregateRow = {
  start: string;
  group: string | null;
  count: number;
};

export async function aggregateLogs(
  query: AggregateQuery,
): Promise<AggregateRow[]> {
  const conditions: string[] = [];
  const values: unknown[] = [];

  let parameterIndex = 1;

  conditions.push(`timestamp >= $${parameterIndex}`);
  values.push(query.since);
  parameterIndex++;

  conditions.push(`timestamp < $${parameterIndex}`);
  values.push(query.until);
  parameterIndex++;

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

  if (query.q !== undefined) {
    conditions.push(`message ILIKE $${parameterIndex}`);
    values.push(`%${query.q}%`);
    parameterIndex++;
  }

  if (query.attributes) {
    for (const [key, value] of Object.entries(query.attributes)) {
      conditions.push(
        `attributes @> $${parameterIndex}::jsonb`,
      );

      values.push(
        JSON.stringify({
          [key]: value,
        }),
      );

      parameterIndex++;
    }
  }

  let bucketExpression: string;

  switch (query.bucket) {
    case "1m":
      bucketExpression = `date_trunc('minute', timestamp)`;
      break;

    case "5m":
      bucketExpression = `
        date_trunc('hour', timestamp)
        + floor(extract(minute from timestamp) / 5) * interval '5 minutes'
      `;
      break;

    case "1h":
      bucketExpression = `date_trunc('hour', timestamp)`;
      break;

    case "1d":
      bucketExpression = `date_trunc('day', timestamp)`;
      break;
  }

  let groupExpression: string | null = null;

  if (query.groupBy === "service") {
    groupExpression = "service";
  } else if (query.groupBy === "level") {
    groupExpression = "level";
  }

  const whereClause = `WHERE ${conditions.join(" AND ")}`;

  const selectGroupExpression =
    groupExpression ?? "NULL::text";

  const groupByClause = groupExpression
    ? `GROUP BY ${bucketExpression}, ${groupExpression}`
    : `GROUP BY ${bucketExpression}`;

  const result = await pool.query<AggregateRow>(
    `
      SELECT
        ${bucketExpression} AS start,
        ${selectGroupExpression} AS "group",
        COUNT(*)::int AS count
      FROM logs
      ${whereClause}
      ${groupByClause}
      ORDER BY start ASC, "group" ASC
    `,
    values,
  );

  return result.rows;
}