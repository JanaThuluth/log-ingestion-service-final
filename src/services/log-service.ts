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

function truncateToMinuteISO(isoTimestamp: string): string {
  const d = new Date(isoTimestamp);
  d.setUTCSeconds(0, 0);
  return d.toISOString();
}

function buildRollupUpsertRows(
  logs: LogEntry[],
): Array<{
  bucket: string;
  service: string;
  level: string;
  count: number;
}> {
  const map = new Map<
    string,
    {
      bucket: string;
      service: string;
      level: string;
      count: number;
    }
  >();
  for (const log of logs) {
    const bucket = truncateToMinuteISO(log.timestamp);
    const key = `${bucket}|${log.service}|${log.level}`;
    const existing = map.get(key);

    if (existing) {
      existing.count++;
    } else {
      map.set(key, { bucket, service: log.service, level: log.level, count: 1 });
    }
  }

  return Array.from(map.values()).sort(
    (a, b) =>
      a.bucket.localeCompare(b.bucket) ||
      a.service.localeCompare(b.service) ||
      a.level.localeCompare(b.level),
  );
}

export async function insertLogs(logs: LogEntry[]): Promise<number> {  
  if (logs.length === 0) {  
    return 0;  
  }  
  
  const client = await pool.connect();  
  
  try {  
    for (let attempt = 1; ; attempt++) {
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

        // تحديث rollup بنفس الـ transaction — يضمن تزامن ذري كامل مع اللوجز
        // الخام: إما الاثنان ينكتبوا سوا، أو ما ينكتب أي منهم
        const rollupRows = buildRollupUpsertRows(logs);

        if (rollupRows.length > 0) {
          await client.query(
            `
              INSERT INTO logs_rollup_1m (bucket_start, service, level, count)
              SELECT b::timestamptz, s, l, c::bigint
              FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[]) AS x(b, s, l, c)
              ON CONFLICT (bucket_start, service, level)
              DO UPDATE SET count = logs_rollup_1m.count + EXCLUDED.count;
            `,
            [
              rollupRows.map((r) => r.bucket),
              rollupRows.map((r) => r.service),
              rollupRows.map((r) => r.level),
              rollupRows.map((r) => r.count),
            ],
          );
        }

        await client.query("COMMIT");

        return logs.length;
      } catch (error) {
        await client.query("ROLLBACK");

        const isDeadlock =
          attempt < 3 &&
          typeof error === "object" &&
          error !== null &&
          (error as { code?: string }).code === "40P01";

        if (!isDeadlock) {
          throw error;
        }
      }
    }
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
    // مقارنة كنص دائمًا (حسب متطلب الـ spec) عبر logs_attributes_kv بدل
    // مقارنة JSONB خام — القيمة القادمة من query params دايمًا نص، بينما
    // القيمة المخزنة ممكن تكون رقم أو boolean، فالمقارنة الخام كانت بتفشل
    // بصمت لأي نوع غير نصي.
    for (const [key, value] of Object.entries(query.attributes)) {
      const kv = `${key.length}:${key}=${value}`;

      conditions.push(
        `logs_attributes_kv(attributes) @> ARRAY[$${parameterIndex}]::text[]`,
      );

      values.push(kv);

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

const BUCKET_INTERVALS: Record<AggregateQuery["bucket"], string> = {
  "1m": "1 minute",
  "5m": "5 minutes",
  "1h": "1 hour",
  "1d": "1 day",
};

export async function aggregateLogs(
  query: AggregateQuery,
): Promise<AggregateRow[]> {
  const groupCol =
    query.groupBy === "service" || query.groupBy === "level"
      ? query.groupBy
      : null;

  const attributeEntries = query.attributes
    ? Object.entries(query.attributes)
    : [];

  const needsRawTable = query.q !== undefined || attributeEntries.length > 0;

  const interval = BUCKET_INTERVALS[query.bucket];
  const selectGroup = groupCol ? `${groupCol} AS "group"` : `NULL::text AS "group"`;

  if (!needsRawTable) {
    // المسار السريع: قراءة من جدول الـ rollup المُجمّع مسبقًا (دقيقة بدقيقة) —
    // أسرع بمراحل من الحساب على الجدول الخام تحت حمل كتابة مستمر
    const conditions: string[] = [`bucket_start >= $1`, `bucket_start < $2`];
    const values: unknown[] = [query.since, query.until];
    let parameterIndex = 3;

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

    const whereClause = `WHERE ${conditions.join(" AND ")}`;
    const groupByClause = groupCol ? `, ${groupCol}` : "";

    const result = await pool.query<AggregateRow>(
      `
        SELECT
          date_bin($${parameterIndex}::interval, bucket_start, '2000-01-01 00:00:00Z'::timestamptz)::text AS start,
          ${selectGroup},
          SUM(count)::int AS count
        FROM logs_rollup_1m
        ${whereClause}
        GROUP BY 1 ${groupByClause}
        ORDER BY 1 ASC, "group" ASC
      `,
      [...values, interval],
    );

    return result.rows;
  }

  // مسار بديل: فلاتر q أو attr.<key> — نعزل المرشحين عبر GIN بواسطة CTE
  // مجمّد (MATERIALIZED) أولًا، لضمان خطة تنفيذ واضحة تعتمد على الفهرس
  const values: unknown[] = [];
  let parameterIndex = 1;

  const attributeClauses = attributeEntries.map(([key, value]) => {
    const kv = `${key.length}:${key}=${value}`;
    values.push(kv);
    return `logs_attributes_kv(attributes) @> ARRAY[$${parameterIndex++}]::text[]`;
  });

  const restConditions: string[] = [
    `timestamp >= $${parameterIndex++}`,
    `timestamp < $${parameterIndex++}`,
  ];
  values.push(query.since, query.until);

  if (query.service !== undefined) {
    restConditions.push(`service = $${parameterIndex++}`);
    values.push(query.service);
  }

  if (query.level !== undefined) {
    restConditions.push(`level = $${parameterIndex++}`);
    values.push(query.level);
  }

  if (query.q !== undefined) {
    restConditions.push(`message ILIKE $${parameterIndex++}`);
    values.push(`%${query.q}%`);
  }

  const restWhere = `WHERE ${restConditions.join(" AND ")}`;
  const groupByClause = groupCol ? `, ${groupCol}` : "";

  let sql: string;

  if (attributeClauses.length > 0) {
    sql = `
      WITH attribute_matches AS MATERIALIZED (
        SELECT timestamp, service, level
        FROM logs
        WHERE ${attributeClauses.join(" AND ")}
      )
      SELECT
        date_bin($${parameterIndex}::interval, timestamp, '2000-01-01 00:00:00Z'::timestamptz)::text AS start,
        ${selectGroup},
        COUNT(*)::int AS count
      FROM attribute_matches
      ${restWhere}
      GROUP BY 1 ${groupByClause}
      ORDER BY 1 ASC, "group" ASC
    `;
  } else {
    sql = `
      SELECT
        date_bin($${parameterIndex}::interval, timestamp, '2000-01-01 00:00:00Z'::timestamptz)::text AS start,
        ${selectGroup},
        COUNT(*)::int AS count
      FROM logs
      ${restWhere}
      GROUP BY 1 ${groupByClause}
      ORDER BY 1 ASC, "group" ASC
    `;
  }

  values.push(interval);

  const result = await pool.query<AggregateRow>(sql, values);

  return result.rows;
}