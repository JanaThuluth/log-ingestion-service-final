import type { FastifyInstance } from "fastify";
import { logEntrySchema } from "../schemas/logs";
import {
  insertLogs,
  queryLogs,
} from "../services/log-service";

export async function logsRoutes(app: FastifyInstance) {
  app.post("/logs", async (request, reply) => {
    const body = request.body as unknown;

    if (
      typeof body !== "object" ||
      body === null ||
      !("logs" in body) ||
      !Array.isArray((body as { logs?: unknown }).logs)
    ) {
      return reply.status(400).send({
        error: "Request body must contain a logs array",
      });
    }

    const logs = (body as { logs: unknown[] }).logs;

    const validLogs = [];
    const rejected = [];

    for (let index = 0; index < logs.length; index++) {
      const result = logEntrySchema.safeParse(logs[index]);

      if (result.success) {
        const timestamp = new Date(result.data.timestamp);
        const fiveMinutesFromNow = new Date(
          Date.now() + 5 * 60 * 1000,
        );

        if (timestamp > fiveMinutesFromNow) {
          rejected.push({
            index,
            reason:
              "timestamp is more than five minutes in the future",
          });
          continue;
        }

        validLogs.push(result.data);
      } else {
        rejected.push({
          index,
          reason:
            result.error.issues[0]?.message ??
            "invalid log entry",
        });
      }
    }

    if (validLogs.length === 0) {
      return reply.status(400).send({
        accepted: 0,
        rejected,
      });
    }

    await insertLogs(validLogs);

    return reply.status(200).send({
      accepted: validLogs.length,
      rejected,
    });
  });

  app.get("/logs", async (request, reply) => {
    const query = request.query as Record<string, string | undefined>;

    let cursor:
      | {
          timestamp: string;
          id: string;
        }
      | undefined;

    if (query.cursor !== undefined) {
      try {
        const decoded = JSON.parse(
          Buffer.from(query.cursor, "base64url").toString("utf8"),
        );

        if (
          typeof decoded !== "object" ||
          decoded === null ||
          typeof decoded.timestamp !== "string" ||
          typeof decoded.id !== "string"
        ) {
          throw new Error("invalid cursor");
        }

        cursor = {
          timestamp: decoded.timestamp,
          id: decoded.id,
        };
      } catch {
        return reply.status(400).send({
          error: "invalid cursor",
        });
      }
    }

    const limitValue = query.limit ?? "100";
    const limit = Number(limitValue);

    if (
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 1000
    ) {
      return reply.status(400).send({
        error: "limit must be between 1 and 1000",
      });
    }

    const level = query.level;

    if (
      level !== undefined &&
      !["debug", "info", "warn", "error"].includes(level)
    ) {
      return reply.status(400).send({
        error: `invalid level: '${level}'`,
      });
    }

    let since: string | undefined;
    let until: string | undefined;

    if (query.since !== undefined) {
      const parsedSince = new Date(query.since);

      if (Number.isNaN(parsedSince.getTime())) {
        return reply.status(400).send({
          error: "invalid since timestamp",
        });
      }

      since = parsedSince.toISOString();
    }

    if (query.until !== undefined) {
      const parsedUntil = new Date(query.until);

      if (Number.isNaN(parsedUntil.getTime())) {
        return reply.status(400).send({
          error: "invalid until timestamp",
        });
      }

      until = parsedUntil.toISOString();
    }

    if (
      since !== undefined &&
      until !== undefined &&
      new Date(until) < new Date(since)
    ) {
      return reply.status(400).send({
        error: "until must not be earlier than since",
      });
    }

    const attributes: Record<string, string> = {};

    for (const [key, value] of Object.entries(query)) {
      if (key.startsWith("attr.") && value !== undefined) {
        attributes[key.slice(5)] = value;
      }
    }

    const result = await queryLogs({
      service: query.service,
      level: level as
        | "debug"
        | "info"
        | "warn"
        | "error"
        | undefined,
      since,
      until,
      attributes,
      q: query.q,
      limit,
      cursor,
    });

    let nextCursor: string | null = null;

    if (result.hasMore && result.logs.length > 0) {
      const lastLog = result.logs[result.logs.length - 1];

      nextCursor = Buffer.from(
        JSON.stringify({
          timestamp: lastLog.timestamp,
          id: lastLog.id,
        }),
      ).toString("base64url");
    }

    return reply.status(200).send({
      logs: result.logs,
      next_cursor: nextCursor,
    });
  });
}