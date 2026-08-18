import fs from "node:fs/promises";
import path from "node:path";
import { pool } from "./pool";

export async function runMigrations(): Promise<void> {
  const migrationsDir = path.join(__dirname, "migrations");

  const files = (await fs.readdir(migrationsDir))
    .filter((file) => file.endsWith(".sql"))
    .sort();

  for (const file of files) {
    const sql = await fs.readFile(
      path.join(migrationsDir, file),
      "utf8"
    );

    await pool.query(sql);
  }
}