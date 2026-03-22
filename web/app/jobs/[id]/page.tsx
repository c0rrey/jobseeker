/**
 * Job detail page — server component showing full job info, score radar chart,
 * dimension reasoning, company sidebar, and feedback buttons.
 *
 * Dynamic route: /jobs/[id]
 * params.id is resolved as a Promise in Next.js 15+ app router.
 *
 * dynamic = 'force-dynamic' prevents static prerendering at build time.
 */

export const dynamic = "force-dynamic";

import Link from "next/link";
import { notFound } from "next/navigation";
import { ExternalLink, ChevronDown } from "lucide-react";
import ReactMarkdown from "react-markdown";
import rehypeSanitize from "rehype-sanitize";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScoreRadar } from "@/components/score-radar";
import { CompComparison } from "@/components/comp-comparison";
import { FeedbackButtons } from "@/components/feedback-buttons";
import {
  getJobById,
  getScoreDimensionForJob,
  getCompanyById,
  getLatestFeedbackForJob,
} from "@/lib/queries";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

function formatSalary(min: number | null, max: number | null): string {
  if (min === null && max === null) return "Not disclosed";
  const fmt = (n: number) =>
    n >= 1000 ? `$${Math.round(n / 1000)}k` : `$${n}`;
  if (min !== null && max !== null) return `${fmt(min)} – ${fmt(max)}`;
  if (min !== null) return `${fmt(min)}+`;
  return `Up to ${fmt(max!)}`;
}

type ReasoningMap = Record<string, string>;

function parseReasoning(raw: string | null): ReasoningMap {
  if (!raw) return {};
  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return parsed as ReasoningMap;
    }
  } catch {
    // not valid JSON — ignore
  }
  return {};
}

// ---------------------------------------------------------------------------
// Dimension section
// ---------------------------------------------------------------------------

interface DimensionRowProps {
  label: string;
  score: number | null;
  color: string;
  reasoning: string | undefined;
}

function DimensionRow({ label, score, color, reasoning }: DimensionRowProps) {
  return (
    <details className="group border border-border rounded-lg overflow-hidden">
      <summary className="flex items-center justify-between px-4 py-3 cursor-pointer list-none select-none hover:bg-muted/50 transition-colors">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div
              className={`h-2.5 w-2.5 rounded-full ${color}`}
              aria-hidden="true"
            />
            <span className="text-sm font-medium">{label}</span>
          </div>
          {score !== null ? (
            <span className="inline-flex h-6 min-w-10 items-center justify-center rounded-full bg-primary/10 px-2 text-xs font-bold text-primary">
              {score}/100
            </span>
          ) : (
            <span className="text-xs text-muted-foreground">N/A</span>
          )}
        </div>
        <ChevronDown
          className="size-4 text-muted-foreground transition-transform group-open:rotate-180"
          aria-hidden="true"
        />
      </summary>
      <div className="px-4 pb-3 pt-1 text-sm text-muted-foreground">
        {reasoning ? (
          <p className="leading-relaxed">{reasoning}</p>
        ) : (
          <p className="italic">No reasoning available for this dimension.</p>
        )}
      </div>
    </details>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

const DIMENSION_CONFIGS = [
  { key: "role_fit" as const, label: "Role Fit", color: "bg-blue-500" },
  { key: "skills_match" as const, label: "Skills Match", color: "bg-green-500" },
  {
    key: "culture_signals" as const,
    label: "Culture Signals",
    color: "bg-purple-500",
  },
  {
    key: "growth_potential" as const,
    label: "Growth Potential",
    color: "bg-orange-500",
  },
  {
    key: "comp_alignment" as const,
    label: "Comp Alignment",
    color: "bg-pink-500",
  },
] as const;

export default async function JobDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const jobId = parseInt(id, 10);

  if (isNaN(jobId)) {
    notFound();
  }

  const job = getJobById(jobId);
  if (!job) {
    notFound();
  }

  const score = getScoreDimensionForJob(jobId);
  const company = job.company_id ? getCompanyById(job.company_id) : null;
  const feedback = getLatestFeedbackForJob(jobId);

  const reasoning = parseReasoning(score?.reasoning ?? null);

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <nav aria-label="Breadcrumb" className="text-sm text-muted-foreground">
        <Link href="/jobs" className="hover:text-foreground transition-colors">
          Jobs
        </Link>
        <span className="mx-2">/</span>
        <span className="text-foreground">{job.title}</span>
      </nav>

      {/* Header */}
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold tracking-tight">{job.title}</h1>
          <p className="text-muted-foreground">
            {job.company}
            {job.location && (
              <span className="ml-2 text-sm">&bull; {job.location}</span>
            )}
          </p>
          <div className="flex flex-wrap gap-2 mt-2">
            <Badge variant="outline">{job.source}</Badge>
            {job.ats_platform && (
              <Badge variant="outline">{job.ats_platform}</Badge>
            )}
            {job.posted_at && (
              <span className="text-xs text-muted-foreground self-center">
                Posted {formatDate(job.posted_at)}
              </span>
            )}
          </div>
        </div>

        <div className="flex items-center gap-3 shrink-0">
          {score && (
            <div className="flex flex-col items-center rounded-xl border border-border bg-card px-5 py-3">
              <span className="text-3xl font-bold text-primary">
                {score.overall}
              </span>
              <span className="text-xs text-muted-foreground">/100 overall</span>
            </div>
          )}
          <a
            href={job.url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-muted transition-colors"
          >
            <ExternalLink className="size-4" />
            View Original
          </a>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-[1fr_320px]">
        {/* Main content */}
        <div className="space-y-6">
          {/* Radar chart + dimensions */}
          {score ? (
            <Card>
              <CardHeader>
                <CardTitle>Score Breakdown</CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                <ScoreRadar
                  role_fit={score.role_fit}
                  skills_match={score.skills_match}
                  culture_signals={score.culture_signals}
                  growth_potential={score.growth_potential}
                  comp_alignment={score.comp_alignment}
                />

                <div className="space-y-2">
                  {DIMENSION_CONFIGS.map(({ key, label, color }) => (
                    <DimensionRow
                      key={key}
                      label={label}
                      score={score[key]}
                      color={color}
                      reasoning={reasoning[key]}
                    />
                  ))}
                </div>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="py-8 text-center text-sm text-muted-foreground">
                This job has not been scored yet.
              </CardContent>
            </Card>
          )}

          {/* Job description */}
          {(job.formatted_description || job.full_description || job.description) && (
            <Card>
              <CardHeader>
                <CardTitle>Job Description</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="prose prose-sm max-w-none text-muted-foreground">
                  <ReactMarkdown rehypePlugins={[rehypeSanitize]}>
                    {job.formatted_description ?? job.full_description ?? job.description ?? ""}
                  </ReactMarkdown>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Feedback */}
          <Card>
            <CardHeader>
              <CardTitle>Your Feedback</CardTitle>
            </CardHeader>
            <CardContent>
              <FeedbackButtons
                jobId={job.id}
                currentSignal={feedback?.signal ?? null}
              />
            </CardContent>
          </Card>
        </div>

        {/* Sidebar */}
        <div className="space-y-4">
          {/* Company card */}
          <Card>
            <CardHeader>
              <CardTitle>Company</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div>
                <p className="text-sm font-medium">{job.company}</p>
                {company?.domain && (
                  <a
                    href={`https://${company.domain}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-muted-foreground hover:text-foreground transition-colors"
                  >
                    {company.domain}
                  </a>
                )}
              </div>

              {company && (
                <>
                  {company.industry && (
                    <div>
                      <p className="text-xs text-muted-foreground uppercase tracking-wide">
                        Industry
                      </p>
                      <p className="text-sm">{company.industry}</p>
                    </div>
                  )}
                  <CompComparison
                    salary_min={job.salary_min}
                    salary_max={job.salary_max}
                    size_range={company.size_range}
                    funding_stage={company.funding_stage}
                    glassdoor_rating={company.glassdoor_rating}
                  />
                  {company.glassdoor_url && (
                    <a
                      href={company.glassdoor_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                    >
                      <ExternalLink className="size-3" />
                      Glassdoor
                    </a>
                  )}
                </>
              )}

              {!company && (
                <CompComparison
                  salary_min={job.salary_min}
                  salary_max={job.salary_max}
                  size_range={null}
                  funding_stage={null}
                  glassdoor_rating={null}
                />
              )}
            </CardContent>
          </Card>

          {/* Job metadata */}
          <Card>
            <CardHeader>
              <CardTitle>Details</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Source</span>
                <span>{job.source}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Salary</span>
                <span>{formatSalary(job.salary_min, job.salary_max)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Posted</span>
                <span>{formatDate(job.posted_at)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Fetched</span>
                <span>{formatDate(job.fetched_at)}</span>
              </div>
              {score && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Scored</span>
                  <span>{formatDate(score.scored_at)}</span>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
