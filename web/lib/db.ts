/**
 * SQLite database connection for the jseeker web dashboard.
 *
 * Uses better-sqlite3 (synchronous) which is compatible with Next.js
 * server components and server actions but NOT with the edge runtime.
 * WAL mode is enabled on every connection to match pipeline/src/database.py.
 */

import Database from "better-sqlite3";
import path from "path";

// Resolve the database path relative to the monorepo root.
// The web/ directory is one level below the project root where data/ lives.
const DB_PATH = path.resolve(process.cwd(), "..", "data", "jobs.db");

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

  return db;
}
