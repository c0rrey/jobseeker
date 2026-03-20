/**
 * SQLite database connection for the jseeker web dashboard.
 *
 * Uses better-sqlite3 (synchronous) which is compatible with Next.js
 * server components and server actions but NOT with the edge runtime.
 * WAL mode is enabled on every connection to match pipeline/src/database.py.
 */

import Database from "better-sqlite3";
import path from "path";

// Resolve the database path relative to this file's location (__dirname = web/lib/).
// Navigate two levels up to reach the monorepo root where data/ lives.
// Respects DB_PATH env var so developers can override the path at startup.
const DB_PATH = process.env.DB_PATH ?? path.resolve(__dirname, "../../data/jobs.db");

let db: Database.Database | null = null;

/**
 * Returns a singleton better-sqlite3 Database instance connected to
 * data/jobs.db with WAL mode enabled.
 *
 * The singleton pattern avoids opening multiple connections during a single
 * server process lifetime. WAL mode allows concurrent reads alongside the
 * Python pipeline's write transactions.
 */
export function getDb(): Database.Database {
  if (db) {
    return db;
  }

  db = new Database(DB_PATH);

  // Enable WAL mode to match database.py and allow concurrent readers.
  db.pragma("journal_mode = WAL");
  // Wait up to 5 s on lock contention instead of failing immediately.
  // Matches pipeline/src/database.py:221 which also sets busy_timeout = 5000.
  db.pragma("busy_timeout = 5000");

  return db;
}
