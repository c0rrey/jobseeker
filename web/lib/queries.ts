/**
 * Shared query functions for the jseeker web dashboard.
 *
 * All queries use better-sqlite3 (synchronous) and run only in server
 * components / server actions — never in the edge runtime or client bundle.
 *
 * Column names match web/lib/types.ts exactly so better-sqlite3 row objects
 * can be cast directly to the return types.
 */

import { getDb } from "@/lib/db";

// ---------------------------------------------------------------------------
// Dashboard queries (seek-18)
// ---------------------------------------------------------------------------

export interface DashboardStats {
  totalJobs: number;
  scoredJobs: number;
  avgOverallScore: number | null;
}

/**
 * Returns summary statistics for the dashboard header cards.
 *
 * - totalJobs: total rows in jobs table
 * - scoredJobs: jobs with a pass=2 score_dimensions row
 * - avgOverallScore: average overall score (pass=2 only), null when no scores exist
 */
export function getDashboardStats(): DashboardStats {
  const db = getDb();

  const totalJobs = (
    db.prepare<[], { count: number }>("SELECT COUNT(*) AS count FROM jobs").get()
  )?.count ?? 0;

  const scoredJobs = (
    db
      .prepare<[], { count: number }>(
        "SELECT COUNT(*) AS count FROM score_dimensions WHERE pass = 2"
      )
      .get()
  )?.count ?? 0;

  const avgRow = db
    .prepare<[], { avg: number | null }>(
      "SELECT AVG(overall) AS avg FROM score_dimensions WHERE pass = 2"
    )
    .get();
  const avgOverallScore = avgRow?.avg ?? null;

  return { totalJobs, scoredJobs, avgOverallScore };
}

// ---------------------------------------------------------------------------

/**
 * Returns the count of career_page_configs rows with status = 'broken'.
 * A count > 0 means an alert should be displayed on the dashboard.
 */
export function getBrokenConfigCount(): number {
  const db = getDb();
  const row = db
    .prepare<[], { count: number }>(
      "SELECT COUNT(*) AS count FROM career_page_configs WHERE status = 'broken'"
    )
    .get();
  return row?.count ?? 0;
}

// ---------------------------------------------------------------------------

/**
 * Returns the count of profile_suggestions rows with status = 'pending'.
 * A count > 0 means an alert should be displayed on the dashboard.
 */
export function getPendingSuggestionCount(): number {
  const db = getDb();
  const row = db
    .prepare<[], { count: number }>(
      "SELECT COUNT(*) AS count FROM profile_suggestions WHERE status = 'pending'"
    )
    .get();
  return row?.count ?? 0;
}

// ---------------------------------------------------------------------------

export interface TopMatch {
  id: number;
  title: string;
  company: string;
  overall: number;
}

/**
 * Returns the top N jobs by overall score (pass=2), ordered descending.
 * Used for the "Top Matches" section on the dashboard.
 */
export function getTopMatches(limit = 5): TopMatch[] {
  const db = getDb();
  return db
    .prepare<[number], TopMatch>(
      `SELECT j.id, j.title, j.company, sd.overall
       FROM jobs j
       INNER JOIN score_dimensions sd ON sd.job_id = j.id AND sd.pass = 2
       ORDER BY sd.overall DESC
       LIMIT ?`
    )
    .all(limit);
}

// ---------------------------------------------------------------------------
// Jobs list queries (seek-19)
// ---------------------------------------------------------------------------

export interface JobRow {
  id: number;
  title: string;
  company: string;
  source: string;
  location: string | null;
  salary_min: number | null;
  salary_max: number | null;
  posted_at: string | null;
  overall: number | null;
  role_fit: number | null;
  skills_gap: number | null;
  culture_signals: number | null;
  growth_potential: number | null;
  comp_alignment: number | null;
  feedback_signal: string | null; // 'thumbs_up' | 'thumbs_down' | null
}

export type SortField = "overall" | "salary" | "posted_at";
export type SortDir = "asc" | "desc";
export type FeedbackFilter = "all" | "thumbs_up" | "thumbs_down" | "no_feedback";

export interface JobListFilters {
  scoreMin?: number;
  scoreMax?: number;
  company?: string;
  source?: string;
  feedbackStatus?: FeedbackFilter;
  sortField?: SortField;
  sortDir?: SortDir;
}

/**
 * Returns a filtered, sorted list of all jobs joined with their latest
 * pass=2 score and most-recent feedback signal.
 *
 * All filtering and sorting is done in SQLite — no in-memory processing needed.
 *
 * NOTE: The query uses a correlated sub-select for feedback rather than a JOIN
 * so that jobs without feedback still appear (LEFT JOIN equivalent in SQLite).
 */
export function getJobList(filters: JobListFilters = {}): JobRow[] {
  const db = getDb();

  const {
    scoreMin,
    scoreMax,
    company,
    source,
    feedbackStatus = "all",
    sortField = "overall",
    sortDir = "desc",
  } = filters;

  const conditions: string[] = [];
  const params: (string | number)[] = [];

  // Score range (only filters rows that have a score)
  if (scoreMin !== undefined && !isNaN(scoreMin)) {
    conditions.push("sd.overall >= ?");
    params.push(scoreMin);
  }
  if (scoreMax !== undefined && !isNaN(scoreMax)) {
    conditions.push("sd.overall <= ?");
    params.push(scoreMax);
  }

  // Company name (case-insensitive partial match)
  if (company && company.trim().length > 0) {
    conditions.push("LOWER(j.company) LIKE LOWER(?)");
    params.push(`%${company.trim()}%`);
  }

  // Source dropdown
  if (source && source.trim().length > 0) {
    conditions.push("j.source = ?");
    params.push(source.trim());
  }

  // Feedback status
  if (feedbackStatus === "thumbs_up") {
    conditions.push("f.signal = 'thumbs_up'");
  } else if (feedbackStatus === "thumbs_down") {
    conditions.push("f.signal = 'thumbs_down'");
  } else if (feedbackStatus === "no_feedback") {
    conditions.push("f.signal IS NULL");
  }

  // Build the ORDER BY clause. Salary sort falls back to salary_min.
  const allowedSortFields: Record<SortField, string> = {
    overall: "sd.overall",
    salary: "j.salary_min",
    posted_at: "j.posted_at",
  };
  const sortColumn = allowedSortFields[sortField] ?? "sd.overall";
  // Only allow literal 'asc' or 'desc' — prevent injection
  const direction = sortDir === "asc" ? "ASC" : "DESC";

  const whereClause =
    conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  const sql = `
    SELECT
      j.id,
      j.title,
      j.company,
      j.source,
      j.location,
      j.salary_min,
      j.salary_max,
      j.posted_at,
      sd.overall,
      sd.role_fit,
      sd.skills_gap,
      sd.culture_signals,
      sd.growth_potential,
      sd.comp_alignment,
      f.signal AS feedback_signal
    FROM jobs j
    LEFT JOIN score_dimensions sd ON sd.job_id = j.id AND sd.pass = 2
    LEFT JOIN (
      SELECT job_id, signal
      FROM feedback
      WHERE id IN (
        SELECT MAX(id) FROM feedback GROUP BY job_id
      )
    ) f ON f.job_id = j.id
    ${whereClause}
    ORDER BY ${sortColumn} ${direction} NULLS LAST
  `;

  return db.prepare<(string | number)[], JobRow>(sql).all(...params);
}

/**
 * Returns the list of distinct company names for the filter dropdown.
 */
export function getCompanyNames(): string[] {
  const db = getDb();
  return db
    .prepare<[], { company: string }>(
      "SELECT DISTINCT company FROM jobs ORDER BY company ASC"
    )
    .all()
    .map((r) => r.company);
}
