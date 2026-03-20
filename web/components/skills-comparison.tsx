/**
 * SkillsComparison — displays resume skills vs profile skills with gap highlighting.
 *
 * Pure server component. Parses extracted_skills (JSON array from resume PDF)
 * and profile_yaml to identify skills present in one but not the other.
 *
 * Gap logic:
 *   - Skills in resume but not in profile YAML text → "resume only" (blue)
 *   - Skills in profile YAML but not in resume → "profile only" (orange)
 *   - Skills in both → "matched" (green)
 */

interface SkillsComparisonProps {
  extractedSkills: string | null; // JSON array string
  profileYaml: string | null;
}

function parseSkills(raw: string | null): string[] {
  if (!raw) return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed.filter((s): s is string => typeof s === "string");
    }
    if (
      parsed !== null &&
      typeof parsed === "object" &&
      "skills" in parsed &&
      Array.isArray((parsed as { skills: unknown }).skills)
    ) {
      return (parsed as { skills: unknown[] }).skills.filter(
        (s): s is string => typeof s === "string"
      );
    }
  } catch {
    // not valid JSON
  }
  return [];
}

/**
 * Very light skill extraction from YAML: looks for lines under "skills:" section
 * that start with "  - " and extracts the value.
 */
function extractProfileSkills(yaml: string | null): string[] {
  if (!yaml) return [];
  const lines = yaml.split("\n");
  const skills: string[] = [];
  let inSkillsSection = false;

  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.toLowerCase().startsWith("skills:")) {
      inSkillsSection = true;
      continue;
    }
    if (inSkillsSection) {
      if (trimmed.startsWith("- ")) {
        skills.push(trimmed.slice(2).trim());
      } else if (trimmed && !trimmed.startsWith("#") && !line.startsWith(" ") && !line.startsWith("\t")) {
        // new top-level section
        inSkillsSection = false;
      }
    }
  }

  return skills;
}

function normalize(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9+#.]/g, " ").trim();
}

interface SkillBadgeProps {
  skill: string;
  variant: "matched" | "resume-only" | "profile-only";
}

function SkillBadge({ skill, variant }: SkillBadgeProps) {
  const classes = {
    matched:
      "bg-green-50 text-green-700 border-green-200 dark:bg-green-950 dark:text-green-300 dark:border-green-800",
    "resume-only":
      "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950 dark:text-blue-300 dark:border-blue-800",
    "profile-only":
      "bg-orange-50 text-orange-700 border-orange-200 dark:bg-orange-950 dark:text-orange-300 dark:border-orange-800",
  }[variant];

  return (
    <span
      className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${classes}`}
    >
      {skill}
    </span>
  );
}

export function SkillsComparison({
  extractedSkills,
  profileYaml,
}: SkillsComparisonProps) {
  const resumeSkills = parseSkills(extractedSkills);
  const profileSkills = extractProfileSkills(profileYaml);

  if (resumeSkills.length === 0 && profileSkills.length === 0) {
    return (
      <p className="text-sm text-muted-foreground italic">
        No skills data available. Run the pipeline to extract resume skills.
      </p>
    );
  }

  const resumeNorm = new Set(resumeSkills.map(normalize));
  const profileNorm = new Set(profileSkills.map(normalize));

  const matched = resumeSkills.filter((s) => profileNorm.has(normalize(s)));
  const resumeOnly = resumeSkills.filter((s) => !profileNorm.has(normalize(s)));
  const profileOnly = profileSkills.filter(
    (s) => !resumeNorm.has(normalize(s))
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
        <span className="inline-flex items-center gap-1">
          <span className="inline-block w-2.5 h-2.5 rounded-full bg-green-500" />
          Matched ({matched.length})
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="inline-block w-2.5 h-2.5 rounded-full bg-blue-500" />
          Resume only ({resumeOnly.length})
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="inline-block w-2.5 h-2.5 rounded-full bg-orange-500" />
          Profile only ({profileOnly.length})
        </span>
      </div>

      <div className="flex flex-wrap gap-1.5">
        {matched.map((s) => (
          <SkillBadge key={s} skill={s} variant="matched" />
        ))}
        {resumeOnly.map((s) => (
          <SkillBadge key={s} skill={s} variant="resume-only" />
        ))}
        {profileOnly.map((s) => (
          <SkillBadge key={s} skill={s} variant="profile-only" />
        ))}
      </div>
    </div>
  );
}
