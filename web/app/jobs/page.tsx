/**
 * Jobs list page — server component that reads filtered/sorted jobs from
 * SQLite and passes them to the JobTable client component for rendering.
 *
 * Filtering and sorting state live in URL searchParams. The server component
 * reads them, passes filter values to getJobList(), and passes the resulting
 * rows plus the parsed filter state to JobTable as props.
 *
 * No 'use client' — this component is a React Server Component.
 *
 * dynamic = 'force-dynamic' prevents static prerendering at build time —
 * the SQLite DB is a runtime dependency and will not exist during build.
 * searchParams also opts the page into dynamic rendering automatically in
 * Next.js 15+, but the explicit export makes the intent clear.
 */

// Opt out of static prerendering — this page requires the live SQLite DB.
export const dynamic = "force-dynamic";

import { JobTable } from "@/components/job-table";
import {
  getJobList,
  type SortField,
  type SortDir,
  type FeedbackFilter,
  type LocationFilter,
} from "@/lib/queries";

const VALID_SORT_FIELDS: SortField[] = ["overall", "salary", "posted_at"];
const VALID_SORT_DIRS: SortDir[] = ["asc", "desc"];
const VALID_FEEDBACK: FeedbackFilter[] = [
  "all",
  "thumbs_up",
  "thumbs_down",
  "no_feedback",
];
const VALID_LOCATIONS: LocationFilter[] = ["all", "remote", "florida"];

function parseSortField(raw: string | undefined): SortField {
  if (raw && (VALID_SORT_FIELDS as string[]).includes(raw)) {
    return raw as SortField;
  }
  return "overall";
}

function parseSortDir(raw: string | undefined): SortDir {
  if (raw && (VALID_SORT_DIRS as string[]).includes(raw)) {
    return raw as SortDir;
  }
  return "desc";
}

function parseFeedback(raw: string | undefined): FeedbackFilter {
  if (raw && (VALID_FEEDBACK as string[]).includes(raw)) {
    return raw as FeedbackFilter;
  }
  return "all";
}

function parseLocation(raw: string | undefined): LocationFilter {
  if (raw && (VALID_LOCATIONS as string[]).includes(raw)) {
    return raw as LocationFilter;
  }
  return "all";
}

export default async function JobsPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const sp = await searchParams;

  // Parse search params — all values arrive as string | string[] | undefined
  const scoreMinRaw = Array.isArray(sp.score_min) ? sp.score_min[0] : sp.score_min;
  const scoreMaxRaw = Array.isArray(sp.score_max) ? sp.score_max[0] : sp.score_max;
  const companyRaw = Array.isArray(sp.company) ? sp.company[0] : sp.company;
  const sourceRaw = Array.isArray(sp.source) ? sp.source[0] : sp.source;
  const feedbackRaw = Array.isArray(sp.feedback) ? sp.feedback[0] : sp.feedback;
  const locationRaw = Array.isArray(sp.location) ? sp.location[0] : sp.location;
  const sortRaw = Array.isArray(sp.sort) ? sp.sort[0] : sp.sort;
  const dirRaw = Array.isArray(sp.dir) ? sp.dir[0] : sp.dir;

  const scoreMin = scoreMinRaw ? parseFloat(scoreMinRaw) : undefined;
  const scoreMax = scoreMaxRaw ? parseFloat(scoreMaxRaw) : undefined;
  const sortField = parseSortField(sortRaw);
  const sortDir = parseSortDir(dirRaw);
  const feedbackStatus = parseFeedback(feedbackRaw);
  const locationStatus = parseLocation(locationRaw);

  const jobs = getJobList({
    scoreMin,
    scoreMax,
    company: companyRaw,
    source: sourceRaw,
    location: locationStatus,
    feedbackStatus,
    sortField,
    sortDir,
  });

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Jobs</h1>
        <p className="text-sm text-muted-foreground mt-1">
          {jobs.length} {jobs.length === 1 ? "job" : "jobs"} found
        </p>
      </div>

      {/* Filterable, sortable table */}
      <JobTable
        jobs={jobs}
        sortField={sortField}
        sortDir={sortDir}
        scoreMin={scoreMinRaw ?? ""}
        scoreMax={scoreMaxRaw ?? ""}
        company={companyRaw ?? ""}
        source={sourceRaw ?? ""}
        location={locationStatus}
        feedbackStatus={feedbackStatus}
      />
    </div>
  );
}
