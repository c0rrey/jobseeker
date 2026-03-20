/**
 * Dashboard page — server component that reads SQLite stats and renders
 * summary cards, alerts, and top matches.
 *
 * No 'use client' directive — rendered entirely on the server via
 * better-sqlite3. All data is fetched synchronously and passed as props
 * to presentational components.
 *
 * dynamic = 'force-dynamic' prevents Next.js from attempting to statically
 * prerender this page at build time. The SQLite DB is a runtime dependency
 * (populated by the pipeline) and will not exist during the build.
 */

// Opt out of static prerendering — this page requires the live SQLite DB.
export const dynamic = "force-dynamic";

import Link from "next/link";
import { AlertBanner } from "@/components/alert-banner";
import { StatCard } from "@/components/stat-card";
import {
  getDashboardStats,
  getBrokenConfigCount,
  getPendingSuggestionCount,
  getTopMatches,
} from "@/lib/queries";

export default function DashboardPage() {
  const stats = getDashboardStats();
  const brokenConfigs = getBrokenConfigCount();
  const pendingSuggestions = getPendingSuggestionCount();
  const topMatches = getTopMatches(5);

  // Empty state: database has no jobs yet
  if (stats.totalJobs === 0) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Your job search overview.
          </p>
        </div>
        <div className="rounded-lg border border-dashed border-border p-12 text-center">
          <p className="text-muted-foreground text-sm">
            No jobs have been fetched yet. Run the pipeline to populate your
            dashboard.
          </p>
        </div>
      </div>
    );
  }

  const avgScoreDisplay =
    stats.avgOverallScore !== null
      ? `${Math.round(stats.avgOverallScore)}/100`
      : "—";

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Your job search overview.
        </p>
      </div>

      {/* Alerts section */}
      {(brokenConfigs > 0 || pendingSuggestions > 0) && (
        <section aria-label="Alerts" className="space-y-2">
          <AlertBanner
            count={brokenConfigs}
            label={
              brokenConfigs === 1
                ? "1 career page config is broken and may not be scraping correctly."
                : `${brokenConfigs} career page configs are broken and may not be scraping correctly.`
            }
          />
          <AlertBanner
            count={pendingSuggestions}
            label={
              pendingSuggestions === 1
                ? "1 profile suggestion is pending your review."
                : `${pendingSuggestions} profile suggestions are pending your review.`
            }
          />
        </section>
      )}

      {/* Summary stat cards */}
      <section aria-label="Summary statistics">
        <div className="grid gap-4 sm:grid-cols-3">
          <StatCard
            label="Total Jobs"
            value={stats.totalJobs.toLocaleString()}
            description="All fetched jobs in the database"
          />
          <StatCard
            label="Jobs Scored"
            value={stats.scoredJobs.toLocaleString()}
            description="Jobs with a full pass-2 score"
          />
          <StatCard
            label="Avg Score"
            value={avgScoreDisplay}
            description="Average overall score (pass-2)"
          />
        </div>
      </section>

      {/* Top matches */}
      <section aria-label="Top matches">
        <h2 className="text-lg font-semibold tracking-tight mb-4">
          Top Matches
        </h2>
        {topMatches.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No scored jobs yet. Run the scoring pipeline to see your top
            matches.
          </p>
        ) : (
          <div className="space-y-2">
            {topMatches.map((job) => (
              <Link
                key={job.id}
                href={`/jobs/${job.id}`}
                className="flex items-center justify-between rounded-lg border border-border bg-card px-4 py-3 text-sm hover:bg-muted/50 transition-colors"
              >
                <span className="font-medium truncate mr-4">{job.title}</span>
                <span className="text-muted-foreground shrink-0 mr-4">
                  {job.company}
                </span>
                <span className="inline-flex h-7 min-w-12 items-center justify-center rounded-full bg-primary/10 px-2 text-xs font-bold text-primary shrink-0">
                  {job.overall}/100
                </span>
              </Link>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
