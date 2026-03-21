"use client";

/**
 * JobTable — interactive client component for the jobs list page.
 *
 * Renders filter controls and a sortable table. Filter state lives in the URL
 * via URLSearchParams so filtering triggers a server-side re-fetch (the parent
 * server page re-renders with new searchParams). Sorting also updates the URL.
 *
 * The table itself is purely presentational — all rows are passed from the
 * server page as props after being filtered/sorted by SQLite.
 */

import { useRouter, usePathname, useSearchParams } from "next/navigation";
import { useCallback } from "react";
import { DimensionBars } from "@/components/dimension-bars";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { JobRow, SortField, SortDir, FeedbackFilter, LocationFilter } from "@/lib/queries";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface JobTableProps {
  jobs: JobRow[];
  sortField: SortField;
  sortDir: SortDir;
  scoreMin: string;
  scoreMax: string;
  company: string;
  source: string;
  location: LocationFilter;
  feedbackStatus: FeedbackFilter;
}

const SOURCES = ["adzuna", "remoteok", "linkedin", "career_page", "greenhouse", "lever", "ashby"] as const;

const LOCATION_OPTIONS: { value: LocationFilter; label: string }[] = [
  { value: "all", label: "All locations" },
  { value: "remote", label: "Remote" },
  { value: "florida", label: "Florida" },
];

const FEEDBACK_OPTIONS: { value: FeedbackFilter; label: string }[] = [
  { value: "all", label: "All feedback" },
  { value: "thumbs_up", label: "Liked" },
  { value: "thumbs_down", label: "Disliked" },
  { value: "no_feedback", label: "No feedback" },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatSalary(min: number | null, max: number | null): string {
  if (min === null && max === null) return "—";
  const fmt = (n: number) =>
    n >= 1000 ? `$${Math.round(n / 1000)}k` : `$${n}`;
  if (min !== null && max !== null) return `${fmt(min)}–${fmt(max)}`;
  if (min !== null) return `${fmt(min)}+`;
  return `up to ${fmt(max!)}`;
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleDateString("en-AU", {
      day: "numeric",
      month: "short",
      year: "numeric",
    });
  } catch {
    return iso;
  }
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function JobTable({
  jobs,
  sortField,
  sortDir,
  scoreMin,
  scoreMax,
  company,
  source,
  location,
  feedbackStatus,
}: JobTableProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  /**
   * Builds a new URLSearchParams from the current ones plus the given overrides,
   * then pushes the resulting URL.
   */
  const updateSearch = useCallback(
    (overrides: Record<string, string>) => {
      const params = new URLSearchParams(searchParams.toString());
      for (const [key, val] of Object.entries(overrides)) {
        if (val === "" || val === "all") {
          params.delete(key);
        } else {
          params.set(key, val);
        }
      }
      router.push(`${pathname}?${params.toString()}`);
    },
    [router, pathname, searchParams]
  );

  /** Toggles sort: same field flips direction, new field defaults to desc. */
  function handleSort(field: SortField) {
    if (field === sortField) {
      updateSearch({ sort: field, dir: sortDir === "desc" ? "asc" : "desc" });
    } else {
      updateSearch({ sort: field, dir: "desc" });
    }
  }

  function sortIndicator(field: SortField): string {
    if (field !== sortField) return "";
    return sortDir === "desc" ? " ↓" : " ↑";
  }

  return (
    <div className="space-y-4">
      {/* Filter controls */}
      <div className="flex flex-wrap items-end gap-3 rounded-lg border border-border bg-card px-4 py-3">
        {/* Score range */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-muted-foreground whitespace-nowrap">
            Score
          </label>
          <input
            type="number"
            min={0}
            max={100}
            placeholder="Min"
            defaultValue={scoreMin}
            className="w-16 rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
            onBlur={(e) => updateSearch({ score_min: e.target.value })}
            onKeyDown={(e) => {
              if (e.key === "Enter")
                updateSearch({ score_min: e.currentTarget.value });
            }}
          />
          <span className="text-xs text-muted-foreground">–</span>
          <input
            type="number"
            min={0}
            max={100}
            placeholder="Max"
            defaultValue={scoreMax}
            className="w-16 rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
            onBlur={(e) => updateSearch({ score_max: e.target.value })}
            onKeyDown={(e) => {
              if (e.key === "Enter")
                updateSearch({ score_max: e.currentTarget.value });
            }}
          />
        </div>

        {/* Company filter */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-muted-foreground whitespace-nowrap">
            Company
          </label>
          <input
            type="text"
            placeholder="Filter…"
            defaultValue={company}
            className="w-32 rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
            onBlur={(e) => updateSearch({ company: e.target.value })}
            onKeyDown={(e) => {
              if (e.key === "Enter")
                updateSearch({ company: e.currentTarget.value });
            }}
          />
        </div>

        {/* Source dropdown */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-muted-foreground whitespace-nowrap">
            Source
          </label>
          <select
            value={source}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
            onChange={(e) => updateSearch({ source: e.target.value })}
          >
            <option value="">All sources</option>
            {SOURCES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>

        {/* Location dropdown */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-muted-foreground whitespace-nowrap">
            Location
          </label>
          <select
            value={location}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
            onChange={(e) => updateSearch({ location: e.target.value })}
          >
            {LOCATION_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        {/* Feedback dropdown */}
        <div className="flex items-center gap-1.5">
          <label className="text-xs text-muted-foreground whitespace-nowrap">
            Feedback
          </label>
          <select
            value={feedbackStatus}
            className="rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
            onChange={(e) =>
              updateSearch({ feedback: e.target.value })
            }
          >
            {FEEDBACK_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Table */}
      {jobs.length === 0 ? (
        <div className="rounded-lg border border-dashed border-border p-12 text-center">
          <p className="text-sm text-muted-foreground">
            No jobs match your filters.
          </p>
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Title</TableHead>
              <TableHead>Company</TableHead>
              <TableHead
                className="cursor-pointer select-none"
                onClick={() => handleSort("overall")}
              >
                Score{sortIndicator("overall")}
              </TableHead>
              <TableHead>Dimensions</TableHead>
              <TableHead
                className="cursor-pointer select-none"
                onClick={() => handleSort("salary")}
              >
                Salary{sortIndicator("salary")}
              </TableHead>
              <TableHead>Location</TableHead>
              <TableHead
                className="cursor-pointer select-none"
                onClick={() => handleSort("posted_at")}
              >
                Posted{sortIndicator("posted_at")}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {jobs.map((job) => (
              <TableRow
                key={job.id}
                className="cursor-pointer"
                onClick={() => router.push(`/jobs/${job.id}`)}
              >
                <TableCell className="font-medium max-w-56 truncate">
                  {job.title}
                </TableCell>
                <TableCell className="text-muted-foreground whitespace-nowrap">
                  {job.company}
                </TableCell>
                <TableCell>
                  {job.overall !== null ? (
                    <span className="inline-flex h-6 min-w-10 items-center justify-center rounded-full bg-primary/10 px-2 text-xs font-bold text-primary">
                      {job.overall}
                    </span>
                  ) : (
                    <span className="text-muted-foreground text-xs">—</span>
                  )}
                </TableCell>
                <TableCell>
                  <DimensionBars
                    role_fit={job.role_fit}
                    skills_match={job.skills_match}
                    culture_signals={job.culture_signals}
                    growth_potential={job.growth_potential}
                    comp_alignment={job.comp_alignment}
                  />
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  {formatSalary(job.salary_min, job.salary_max)}
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {job.location ?? "—"}
                </TableCell>
                <TableCell className="text-muted-foreground whitespace-nowrap">
                  {formatDate(job.posted_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}
