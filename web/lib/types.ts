/**
 * TypeScript interfaces mirroring the jseeker V2 SQLite schema.
 *
 * All column names match the DB column names exactly so that
 * better-sqlite3 row objects can be cast directly to these types.
 * Nullable columns use `string | null` (not undefined) to match
 * SQLite's NULL semantics.
 */

export interface Job {
  id: number;
  source: string; // 'adzuna' | 'remoteok' | 'linkedin' | 'career_page' | 'ats_feed'
  source_type: string; // 'api' | 'career_page' | 'ats_feed'
  external_id: string | null;
  url: string;
  title: string;
  company: string;
  company_id: number | null;
  location: string | null;
  description: string | null;
  salary_min: number | null;
  salary_max: number | null;
  posted_at: string | null;
  fetched_at: string;
  last_seen_at: string;
  ats_platform: string | null; // 'greenhouse' | 'lever' | 'workday' | 'ashby' | null
  raw_json: string | null;
  dedup_hash: string | null;
}

export interface Company {
  id: number;
  name: string;
  domain: string | null;
  career_page_url: string | null;
  ats_platform: string | null; // 'greenhouse' | 'lever' | 'workday' | 'ashby' | null
  size_range: string | null; // '1-50' | '51-200' | '201-1000' | '1001-5000' | '5000+'
  industry: string | null;
  funding_stage: string | null; // 'seed' | 'series_a' | 'series_b' | ... | 'public'
  glassdoor_rating: number | null;
  glassdoor_url: string | null;
  tech_stack: string | null; // JSON array
  crunchbase_data: string | null; // JSON blob
  enriched_at: string | null;
  is_target: number; // SQLite boolean: 0 | 1
  created_at: string;
}

export interface ScoreDimension {
  id: number;
  job_id: number;
  pass: number; // 1 = fast filter, 2 = deep analysis
  role_fit: number | null; // 0-100
  skills_match: number | null; // 0-100
  culture_signals: number | null; // 0-100
  growth_potential: number | null; // 0-100
  comp_alignment: number | null; // 0-100
  overall: number; // weighted composite 0-100
  reasoning: string | null; // JSON: per-dimension explanations
  scored_at: string;
  profile_hash: string | null; // SHA256 of profile.yaml + latest profile_snapshot
}

export interface Feedback {
  id: number;
  job_id: number;
  signal: "thumbs_up" | "thumbs_down";
  note: string | null;
  created_at: string;
}

export interface ProfileSnapshot {
  id: number;
  profile_yaml: string;
  resume_hash: string | null;
  extracted_skills: string | null; // JSON: skills parsed from resume PDF
  created_at: string;
}

export interface CareerPageConfig {
  id: number;
  company_id: number;
  url: string;
  discovery_method: string; // 'auto' | 'manual'
  scrape_strategy: string | null; // JSON: LLM-generated extraction instructions
  last_crawled_at: string | null;
  status: "active" | "broken" | "disabled";
  created_at: string;
}

export interface ProfileSuggestion {
  id: number;
  suggestion_type: string; // 'add_skill' | 'remove_skill' | 'adjust_weight' | 'add_keyword' | etc.
  description: string;
  reasoning: string;
  suggested_change: string; // JSON: the specific YAML diff
  status: "pending" | "approved" | "rejected";
  created_at: string;
  resolved_at: string | null;
}
