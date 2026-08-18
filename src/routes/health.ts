import { FastifyInstance } from "fastify";
import { pool } from "../db/pool";

export async function healthRoute(app: FastifyInstance) {
  app.get("/health", async (_request, reply) => {
    try {
      await pool.query("SELECT 1");

      return reply.code(200).send({
        status: "ok"
      });
    } catch {
      return reply.code(503).send({
        status: "unavailable"
      });
    }
  });
}