/**
 * CompComparison — shows the job's salary range alongside company enrichment data.
 *
 * Pure server component. Renders the compensation range for a job and,
 * if available, contextual company details (size, funding, industry).
 */

interface CompComparisonProps {
  salary_min: number | null;
  salary_max: number | null;
  size_range: string | null;
  funding_stage: string | null;
  glassdoor_rating: number | null;
}

function formatSalary(min: number | null, max: number | null): string {
  if (min === null && max === null) return "Not disclosed";
  const fmt = (n: number) =>
    n >= 1000 ? `$${Math.round(n / 1000)}k` : `$${n}`;
  if (min !== null && max !== null) return `${fmt(min)} – ${fmt(max)}`;
  if (min !== null) return `${fmt(min)}+`;
  return `Up to ${fmt(max!)}`;
}

export function CompComparison({
  salary_min,
  salary_max,
  size_range,
  funding_stage,
  glassdoor_rating,
}: CompComparisonProps) {
  const hasEnrichment = size_range || funding_stage || glassdoor_rating !== null;

  return (
    <div className="space-y-3">
      <div>
        <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1">
          Salary Range
        </p>
        <p className="text-sm font-medium">
          {formatSalary(salary_min, salary_max)}
        </p>
      </div>

      {hasEnrichment && (
        <div className="space-y-2 border-t border-border pt-3">
          {size_range && (
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">
                Company Size
              </p>
              <p className="text-sm">{size_range} employees</p>
            </div>
          )}
          {funding_stage && (
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">
                Funding Stage
              </p>
              <p className="text-sm capitalize">
                {funding_stage.replace(/_/g, " ")}
              </p>
            </div>
          )}
          {glassdoor_rating !== null && (
            <div>
              <p className="text-xs text-muted-foreground uppercase tracking-wide">
                Glassdoor Rating
              </p>
              <p className="text-sm">{glassdoor_rating.toFixed(1)} / 5.0</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
