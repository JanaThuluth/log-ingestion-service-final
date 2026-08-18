import { buildApp } from "./app";
import { runMigrations } from "./db/migrate";
import { pool } from "./db/pool";

const PORT = Number(process.env.PORT ?? 8080);
const HOST = process.env.HOST ?? "0.0.0.0";

async function start(): Promise<void> {
  try {
    await runMigrations();

    const app = buildApp();

    await app.listen({
      host: HOST,
      port: PORT
    });

    console.log(`Server running on ${HOST}:${PORT}`);
  } catch (error) {
    console.error("Failed to start server:", error);

    await pool.end();

    process.exit(1);
  }
}

start();