"use client";

/**
 * ScoreRadar — Recharts radar chart for the 5 score dimensions.
 *
 * Client component because Recharts requires the DOM/browser environment.
 * Renders a radar chart with 5 axes: Role Fit, Skills Match, Culture,
 * Growth Potential, Comp Alignment.
 *
 * Each dimension value is 0-100. Null values are displayed as 0.
 */

import {
  RadarChart,
  Radar,
  PolarGrid,
  PolarAngleAxis,
  ResponsiveContainer,
  Tooltip,
} from "recharts";

interface ScoreRadarProps {
  role_fit: number | null;
  skills_match: number | null;
  culture_signals: number | null;
  growth_potential: number | null;
  comp_alignment: number | null;
}

export function ScoreRadar({
  role_fit,
  skills_match,
  culture_signals,
  growth_potential,
  comp_alignment,
}: ScoreRadarProps) {
  const data = [
    { dimension: "Role Fit", score: role_fit ?? 0 },
    { dimension: "Skills Match", score: skills_match ?? 0 },
    { dimension: "Culture", score: culture_signals ?? 0 },
    { dimension: "Growth", score: growth_potential ?? 0 },
    { dimension: "Comp", score: comp_alignment ?? 0 },
  ];

  return (
    <ResponsiveContainer width="100%" height={260}>
      <RadarChart data={data} outerRadius={90}>
        <PolarGrid />
        <PolarAngleAxis
          dataKey="dimension"
          tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }}
        />
        <Radar
          dataKey="score"
          stroke="hsl(var(--primary))"
          fill="hsl(var(--primary))"
          fillOpacity={0.2}
          dot={{ r: 3, fill: "hsl(var(--primary))" }}
        />
        <Tooltip
          formatter={(value) =>
            value !== undefined ? [`${value}/100`, "Score"] : ["N/A", "Score"]
          }
          contentStyle={{
            fontSize: 12,
            borderRadius: 8,
            border: "1px solid hsl(var(--border))",
            background: "hsl(var(--card))",
            color: "hsl(var(--card-foreground))",
          }}
        />
      </RadarChart>
    </ResponsiveContainer>
  );
}
