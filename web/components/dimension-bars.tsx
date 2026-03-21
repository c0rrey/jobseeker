/**
 * DimensionBars — renders 5 mini horizontal bars for the 5 score dimensions.
 *
 * Pure server component (no 'use client'). Accepts individual dimension scores
 * (0-100 or null) and renders proportional bars with tooltips via title attrs.
 *
 * Dimensions rendered in order:
 *   role_fit, skills_match, culture_signals, growth_potential, comp_alignment
 */

interface DimensionBarsProps {
  role_fit: number | null;
  skills_match: number | null;
  culture_signals: number | null;
  growth_potential: number | null;
  comp_alignment: number | null;
}

interface DimConfig {
  key: keyof DimensionBarsProps;
  label: string;
  color: string;
}

const DIMENSIONS: DimConfig[] = [
  { key: "role_fit", label: "Role fit", color: "bg-blue-500" },
  { key: "skills_match", label: "Skills Match", color: "bg-green-500" },
  { key: "culture_signals", label: "Culture", color: "bg-purple-500" },
  { key: "growth_potential", label: "Growth", color: "bg-orange-500" },
  { key: "comp_alignment", label: "Comp", color: "bg-pink-500" },
];

export function DimensionBars(props: DimensionBarsProps) {
  return (
    <div
      className="flex flex-col gap-0.5"
      aria-label="Score dimensions"
    >
      {DIMENSIONS.map(({ key, label, color }) => {
        const score = props[key];
        const pct = score !== null ? score : 0;

        return (
          <div
            key={key}
            className="flex items-center gap-1"
            title={score !== null ? `${label}: ${score}/100` : `${label}: N/A`}
          >
            <div className="w-16 h-1.5 rounded-full bg-muted overflow-hidden">
              <div
                className={`h-full rounded-full ${color} transition-[width]`}
                style={{ width: `${pct}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
