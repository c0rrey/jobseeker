/**
 * Profile page — server component showing profile.yaml contents, skills
 * comparison, pending suggestions with approve/reject, and history.
 *
 * dynamic = 'force-dynamic' prevents static prerendering at build time.
 */

export const dynamic = "force-dynamic";

import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { SkillsComparison } from "@/components/skills-comparison";
import { SuggestionCard } from "@/components/suggestion-card";
import { getLatestProfileSnapshot, getProfileSuggestions } from "@/lib/queries";

function formatDate(iso: string | null | undefined): string {
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

export default function ProfilePage() {
  const snapshot = getLatestProfileSnapshot();
  const suggestions = getProfileSuggestions();

  const pendingSuggestions = suggestions.filter((s) => s.status === "pending");
  const historySuggestions = suggestions.filter((s) => s.status !== "pending");

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Profile</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Your profile snapshot, skills comparison, and evolution suggestions.
        </p>
      </div>

      {/* No snapshot state */}
      {!snapshot ? (
        <div className="rounded-lg border border-dashed border-border p-12 text-center">
          <p className="text-sm text-muted-foreground">
            No profile snapshot found. Run the pipeline to extract your profile.
          </p>
        </div>
      ) : (
        <>
          {/* Profile YAML */}
          <Card>
            <CardHeader>
              <CardTitle>Profile YAML</CardTitle>
              <CardDescription>
                Snapshot from {formatDate(snapshot.created_at)}
                {snapshot.resume_hash && (
                  <span className="ml-2 font-mono text-xs">
                    resume: {snapshot.resume_hash.slice(0, 8)}…
                  </span>
                )}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <pre className="rounded-lg bg-muted px-4 py-3 text-xs leading-relaxed overflow-x-auto whitespace-pre-wrap max-h-96 overflow-y-auto">
                {snapshot.profile_yaml}
              </pre>
            </CardContent>
          </Card>

          {/* Skills comparison */}
          <Card>
            <CardHeader>
              <CardTitle>Skills Comparison</CardTitle>
              <CardDescription>
                Resume skills vs profile skills — gaps shown in blue (resume only)
                and orange (profile only).
              </CardDescription>
            </CardHeader>
            <CardContent>
              <SkillsComparison
                extractedSkills={snapshot.extracted_skills}
                profileYaml={snapshot.profile_yaml}
              />
            </CardContent>
          </Card>
        </>
      )}

      {/* Pending suggestions */}
      <section aria-label="Pending suggestions">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold tracking-tight">
            Pending Suggestions
          </h2>
          {pendingSuggestions.length > 0 && (
            <span className="inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-primary/10 px-2 text-xs font-bold text-primary">
              {pendingSuggestions.length}
            </span>
          )}
        </div>

        {pendingSuggestions.length === 0 ? (
          <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center">
            <p className="text-sm text-muted-foreground">
              No pending suggestions. The pipeline will generate suggestions as
              it finds patterns in your job feedback.
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            {pendingSuggestions.map((s) => (
              <SuggestionCard key={s.id} suggestion={s} />
            ))}
          </div>
        )}
      </section>

      {/* Suggestion history */}
      {historySuggestions.length > 0 && (
        <section aria-label="Suggestion history">
          <h2 className="text-lg font-semibold tracking-tight mb-4">
            Suggestion History
          </h2>
          <div className="space-y-3">
            {historySuggestions.map((s) => (
              <SuggestionCard key={s.id} suggestion={s} />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
