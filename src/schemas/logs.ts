import { z } from "zod";

const attributeValueSchema = z.union([
  z.string(),
  z.number(),
  z.boolean(),
]);

export const logEntrySchema = z.object({
  timestamp: z.string().datetime(),
  level: z.enum(["debug", "info", "warn", "error"]),
  service: z.string().min(1),
  message: z.string().min(1),
  attributes: z.record(z.string(), attributeValueSchema).default({}),
});

export const logsRequestSchema = z.object({
  logs: z.array(logEntrySchema),
});

export type LogEntry = z.infer<typeof logEntrySchema>;