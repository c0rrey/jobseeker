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
import type {
  Job,
  ScoreDimension,
  Company,
  Feedback,
  ProfileSnapshot,
  ProfileSuggestion,
} from "@/lib/types";

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
  skills_match: number | null;
  culture_signals: number | null;
  growth_potential: number | null;
  comp_alignment: number | null;
  feedback_signal: string | null; // 'thumbs_up' | 'thumbs_down' | null
}

export type SortField = "overall" | "salary" | "posted_at";
export type SortDir = "asc" | "desc";
export type FeedbackFilter = "all" | "thumbs_up" | "thumbs_down" | "no_feedback";
export type LocationFilter = "all" | "remote" | "florida";

export interface JobListFilters {
  scoreMin?: number;
  scoreMax?: number;
  company?: string;
  source?: string;
  location?: LocationFilter;
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
    location = "all",
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

  // Location filter — Adzuna uses "City, County" format with no state.
  // "Remote" jobs come through as "US" or "State, US".  Florida jobs are
  // identified by county name.  Some county names are ambiguous (Orange,
  // Lake, Marion, Polk) so we match "County, Florida" patterns too.
  if (location === "remote") {
    conditions.push(
      "(j.location = 'US' OR (LOWER(j.location) LIKE '%, us' AND LOWER(j.location) NOT LIKE '%florida%')" +
      " OR LOWER(j.location) LIKE '%remote%' OR LOWER(j.location) LIKE '%work from home%'" +
      " OR LOWER(j.location) LIKE '%wfh%' OR LOWER(j.location) LIKE '%telecommute%'" +
      " OR LOWER(j.location) LIKE '%anywhere%' OR j.location IS NULL OR j.location = '')"
    );
  } else if (location === "florida") {
    conditions.push(
      "(LOWER(j.location) LIKE '%florida%'" +
      " OR LOWER(j.location) LIKE '%miami-dade%'" +
      " OR LOWER(j.location) LIKE '%broward%'" +
      " OR LOWER(j.location) LIKE '%palm beach%'" +
      " OR LOWER(j.location) LIKE '%hillsborough%'" +
      " OR LOWER(j.location) LIKE '%pinellas%'" +
      " OR LOWER(j.location) LIKE '%duval%'" +
      " OR LOWER(j.location) LIKE '%lee county%'" +
      " OR LOWER(j.location) LIKE '%brevard%'" +
      " OR LOWER(j.location) LIKE '%volusia%'" +
      " OR LOWER(j.location) LIKE '%pasco%'" +
      " OR LOWER(j.location) LIKE '%seminole%'" +
      " OR LOWER(j.location) LIKE '%sarasota%'" +
      " OR LOWER(j.location) LIKE '%manatee%'" +
      " OR LOWER(j.location) LIKE '%collier%'" +
      " OR LOWER(j.location) LIKE '%escambia%'" +
      " OR LOWER(j.location) LIKE '%osceola%'" +
      " OR LOWER(j.location) LIKE '%st. johns%'" +
      " OR LOWER(j.location) LIKE '%st. lucie%'" +
      " OR LOWER(j.location) LIKE '%leon county%'" +
      " OR LOWER(j.location) LIKE '%alachua%'" +
      " OR LOWER(j.location) LIKE '%hernando%'" +
      " OR LOWER(j.location) LIKE '%charlotte county%'" +
      // Ambiguous counties — match on known FL cities instead
      " OR LOWER(j.location) LIKE 'orlando,%'" +
      " OR LOWER(j.location) LIKE 'maitland,%'" +
      " OR LOWER(j.location) LIKE 'edgewood, orange%'" +
      " OR LOWER(j.location) LIKE 'sand lake,%'" +
      " OR LOWER(j.location) LIKE 'altamonte springs,%'" +
      " OR LOWER(j.location) LIKE 'lake mary,%'" +
      " OR LOWER(j.location) LIKE 'bonita springs,%'" +
      " OR LOWER(j.location) LIKE 'naples,%'" +
      " OR LOWER(j.location) LIKE 'boca raton,%'" +
      " OR LOWER(j.location) LIKE 'delray beach,%'" +
      " OR LOWER(j.location) LIKE 'west palm beach,%'" +
      " OR LOWER(j.location) LIKE 'jupiter,%'" +
      " OR LOWER(j.location) LIKE 'celebration,%'" +
      " OR LOWER(j.location) LIKE 'cape canaveral,%'" +
      " OR LOWER(j.location) LIKE 'cocoa,%'" +
      " OR LOWER(j.location) LIKE 'melbourne, brevard%'" +
      " OR LOWER(j.location) LIKE 'titusville,%'" +
      " OR LOWER(j.location) LIKE 'merritt island,%'" +
      " OR LOWER(j.location) LIKE 'gainesville, alachua%'" +
      " OR LOWER(j.location) LIKE 'tallahassee,%'" +
      " OR LOWER(j.location) LIKE 'pensacola,%'" +
      " OR LOWER(j.location) LIKE 'fort lauderdale,%'" +
      " OR LOWER(j.location) LIKE 'sunrise,%'" +
      " OR LOWER(j.location) LIKE 'plantation,%'" +
      " OR LOWER(j.location) LIKE 'deerfield beach,%'" +
      " OR LOWER(j.location) LIKE 'oakland park,%')"
    );
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

  // Exclude prefiltered jobs (pass=0) and Pass 1 rejections (pass=1, overall=0)
  conditions.unshift("sd0.job_id IS NULL");
  conditions.unshift("sd1_fail.job_id IS NULL");

  // Hide non-representative duplicates (show only reps + ungrouped jobs)
  conditions.unshift("(j.is_representative = 1 OR j.dup_group_id IS NULL)");

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
      sd.skills_match,
      sd.culture_signals,
      sd.growth_potential,
      sd.comp_alignment,
      f.signal AS feedback_signal
    FROM jobs j
    LEFT JOIN score_dimensions sd ON sd.job_id = j.id AND sd.pass = 2
    LEFT JOIN score_dimensions sd0 ON sd0.job_id = j.id AND sd0.pass = 0
    LEFT JOIN score_dimensions sd1_fail ON sd1_fail.job_id = j.id AND sd1_fail.pass = 1 AND sd1_fail.overall = 0
    LEFT JOIN (
      SELECT job_id, signal
      FROM feedback
      WHERE id IN (
        SELECT MAX(id) FROM feedback GROUP BY job_id
      )
    ) f ON f.job_id = j.id
    WHERE ${conditions.join(" AND ")}
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

// ---------------------------------------------------------------------------
// Job detail queries (seek-20)
// ---------------------------------------------------------------------------

/**
 * Returns a single job row by id, or null if not found.
 */
export function getJobById(id: number): Job | null {
  const db = getDb();
  return (
    db.prepare<[number], Job>("SELECT * FROM jobs WHERE id = ?").get(id) ?? null
  );
}

/**
 * Returns the most recent pass=2 score_dimensions row for a job, or null.
 */
export function getScoreDimensionForJob(jobId: number): ScoreDimension | null {
  const db = getDb();
  return (
    db
      .prepare<[number], ScoreDimension>(
        "SELECT * FROM score_dimensions WHERE job_id = ? AND pass = 2 ORDER BY scored_at DESC LIMIT 1"
      )
      .get(jobId) ?? null
  );
}

/**
 * Returns the company record linked to a job's company_id, or null.
 */
export function getCompanyById(id: number): Company | null {
  const db = getDb();
  return (
    db
      .prepare<[number], Company>("SELECT * FROM companies WHERE id = ?")
      .get(id) ?? null
  );
}

/**
 * Returns the most recent feedback for a job, or null.
 */
export function getLatestFeedbackForJob(jobId: number): Feedback | null {
  const db = getDb();
  return (
    db
      .prepare<[number], Feedback>(
        "SELECT * FROM feedback WHERE job_id = ? ORDER BY created_at DESC LIMIT 1"
      )
      .get(jobId) ?? null
  );
}

// ---------------------------------------------------------------------------
// Company queries (seek-21)
// ---------------------------------------------------------------------------

export interface CompanyRow {
  id: number;
  name: string;
  domain: string | null;
  career_page_url: string | null;
  ats_platform: string | null;
  size_range: string | null;
  industry: string | null;
  funding_stage: string | null;
  glassdoor_rating: number | null;
  enriched_at: string | null;
  is_target: number;
  created_at: string;
  job_count: number;
  crawl_status: string | null; // latest career_page_configs.status or null
}

/**
 * Returns all companies with their job count and latest career page crawl status.
 * Ordered by is_target DESC then name ASC.
 */
export function getCompanyList(): CompanyRow[] {
  const db = getDb();
  return db
    .prepare<[], CompanyRow>(
      `SELECT
        c.id,
        c.name,
        c.domain,
        c.career_page_url,
        c.ats_platform,
        c.size_range,
        c.industry,
        c.funding_stage,
        c.glassdoor_rating,
        c.enriched_at,
        c.is_target,
        c.created_at,
        COUNT(j.id) AS job_count,
        (
          SELECT cpc.status
          FROM career_page_configs cpc
          WHERE cpc.company_id = c.id
          ORDER BY cpc.created_at DESC
          LIMIT 1
        ) AS crawl_status
      FROM companies c
      LEFT JOIN jobs j ON j.company_id = c.id
      GROUP BY c.id
      ORDER BY c.is_target DESC, c.name ASC`
    )
    .all();
}

// ---------------------------------------------------------------------------
// Profile / suggestion queries (seek-22)
// ---------------------------------------------------------------------------

/**
 * Returns the most recent profile snapshot row, or null if none exists.
 */
export function getLatestProfileSnapshot(): ProfileSnapshot | null {
  const db = getDb();
  return (
    db
      .prepare<[], ProfileSnapshot>(
        "SELECT * FROM profile_snapshots ORDER BY created_at DESC LIMIT 1"
      )
      .get() ?? null
  );
}

/**
 * Returns all profile suggestions ordered by created_at DESC.
 * Pending suggestions appear first (via status ordering).
 */
export function getProfileSuggestions(): ProfileSuggestion[] {
  const db = getDb();
  return db
    .prepare<[], ProfileSuggestion>(
      `SELECT * FROM profile_suggestions
       ORDER BY
         CASE status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
         created_at DESC`
    )
    .all();
}

// ---------------------------------------------------------------------------
// Feedback history queries (seek-23)
// ---------------------------------------------------------------------------

export interface FeedbackRow {
  id: number;
  job_id: number;
  signal: "thumbs_up" | "thumbs_down";
  note: string | null;
  created_at: string;
  job_title: string;
  job_company: string;
  overall: number | null;
}

export type FeedbackSignalFilter = "all" | "thumbs_up" | "thumbs_down";

/**
 * Returns feedback entries joined with job title, company, and overall score.
 * Ordered newest first. Optionally filtered by signal type.
 */
export function getFeedbackHistory(
  signal: FeedbackSignalFilter = "all"
): FeedbackRow[] {
  const db = getDb();

  const whereClause =
    signal === "thumbs_up"
      ? "WHERE f.signal = 'thumbs_up'"
      : signal === "thumbs_down"
        ? "WHERE f.signal = 'thumbs_down'"
        : "";

  return db
    .prepare<[], FeedbackRow>(
      `SELECT
        f.id,
        f.job_id,
        f.signal,
        f.note,
        f.created_at,
        j.title AS job_title,
        j.company AS job_company,
        sd.overall
      FROM feedback f
      INNER JOIN jobs j ON j.id = f.job_id
      LEFT JOIN score_dimensions sd ON sd.job_id = f.job_id AND sd.pass = 2
      ${whereClause}
      ORDER BY f.created_at DESC`
    )
    .all();
}

/**
 * Returns counts of all feedback, thumbs_up, and thumbs_down.
 */
export interface FeedbackCounts {
  total: number;
  thumbs_up: number;
  thumbs_down: number;
}

export function getFeedbackCounts(): FeedbackCounts {
  const db = getDb();
  const row = db
    .prepare<
      [],
      { total: number; thumbs_up: number; thumbs_down: number }
    >(
      `SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN signal = 'thumbs_up' THEN 1 ELSE 0 END) AS thumbs_up,
        SUM(CASE WHEN signal = 'thumbs_down' THEN 1 ELSE 0 END) AS thumbs_down
      FROM feedback`
    )
    .get();
  return {
    total: row?.total ?? 0,
    thumbs_up: row?.thumbs_up ?? 0,
    thumbs_down: row?.thumbs_down ?? 0,
  };
}
