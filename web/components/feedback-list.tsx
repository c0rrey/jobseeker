"use client";

/**
 * FeedbackList — interactive client component for the feedback history page.
 *
 * Renders a signal filter dropdown and a chronological list of feedback entries.
 * Filter state lives in the URL via searchParams — changing the filter triggers
 * a server re-fetch (the parent server page re-renders with new signal).
 *
 * Each entry links to the corresponding job detail page.
 */

import { useRouter, usePathname, useSearchParams } from "next/navigation";
import { ThumbsUp, ThumbsDown } from "lucide-react";
import Link from "next/link";
import type { FeedbackRow, FeedbackSignalFilter, FeedbackCounts } from "@/lib/queries";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDate(iso: string): string {
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

const FILTER_OPTIONS: { value: FeedbackSignalFilter; label: string }[] = [
  { value: "all", label: "All feedback" },
  { value: "thumbs_up", label: "Thumbs up" },
  { value: "thumbs_down", label: "Thumbs down" },
];

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface FeedbackListProps {
  entries: FeedbackRow[];
  signal: FeedbackSignalFilter;
  counts: FeedbackCounts;
}

export function FeedbackList({ entries, signal, counts }: FeedbackListProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  function handleSignalChange(value: FeedbackSignalFilter) {
    const params = new URLSearchParams(searchParams.toString());
    if (value === "all") {
      params.delete("signal");
    } else {
      params.set("signal", value);
    }
    router.push(`${pathname}?${params.toString()}`);
  }

  return (
    <div className="space-y-4">
      {/* Summary counts */}
      <div className="flex flex-wrap gap-4 rounded-lg border border-border bg-card px-4 py-3 text-sm">
        <span className="text-muted-foreground">
          Total:{" "}
          <span className="font-medium text-foreground">{counts.total}</span>
        </span>
        <span className="inline-flex items-center gap-1 text-muted-foreground">
          <ThumbsUp className="size-3.5 text-green-600" />
          <span className="font-medium text-foreground">{counts.thumbs_up}</span>
        </span>
        <span className="inline-flex items-center gap-1 text-muted-foreground">
          <ThumbsDown className="size-3.5 text-red-500" />
          <span className="font-medium text-foreground">
            {counts.thumbs_down}
          </span>
        </span>
      </div>

      {/* Filter dropdown */}
      <div className="flex items-center gap-2">
        <label
          htmlFor="signal-filter"
          className="text-xs text-muted-foreground whitespace-nowrap"
        >
          Filter:
        </label>
        <select
          id="signal-filter"
          value={signal}
          onChange={(e) =>
            handleSignalChange(e.target.value as FeedbackSignalFilter)
          }
          className="rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
        >
          {FILTER_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
      </div>

      {/* Entries list */}
      {entries.length === 0 ? (
        <div className="rounded-lg border border-dashed border-border p-12 text-center">
          <p className="text-sm text-muted-foreground">
            {counts.total === 0
              ? "No feedback submitted yet. Visit a job detail page to leave feedback."
              : "No feedback matches your filter."}
          </p>
        </div>
      ) : (
        <div className="space-y-2">
          {entries.map((entry) => (
            <div
              key={entry.id}
              className="flex items-start gap-3 rounded-lg border border-border bg-card px-4 py-3"
            >
              {/* Signal icon */}
              <div className="shrink-0 mt-0.5">
                {entry.signal === "thumbs_up" ? (
                  <ThumbsUp className="size-4 text-green-600" />
                ) : (
                  <ThumbsDown className="size-4 text-red-500" />
                )}
              </div>

              {/* Content */}
              <div className="flex-1 min-w-0 space-y-0.5">
                <Link
                  href={`/jobs/${entry.job_id}`}
                  className="text-sm font-medium hover:text-primary transition-colors truncate block"
                >
                  {entry.job_title}
                </Link>
                <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                  <span>{entry.job_company}</span>
                  {entry.overall !== null && (
                    <span className="inline-flex h-5 min-w-8 items-center justify-center rounded-full bg-primary/10 px-1.5 text-xs font-bold text-primary">
                      {entry.overall}
                    </span>
                  )}
                </div>
                {entry.note && (
                  <p className="text-xs text-muted-foreground italic mt-1">
                    &ldquo;{entry.note}&rdquo;
                  </p>
                )}
              </div>

              {/* Timestamp */}
              <div className="shrink-0 text-xs text-muted-foreground whitespace-nowrap">
                {formatDate(entry.created_at)}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
