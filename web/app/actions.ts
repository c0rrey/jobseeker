"use server";

/**
 * Server actions for jseeker web dashboard.
 *
 * Feedback actions (submitFeedback)
 * Company CRUD actions (addCompany, updateCompanyCareerUrl, toggleCompanyTarget)
 * Profile suggestion actions (approveSuggestion, rejectSuggestion)
 *
 * All actions use better-sqlite3 synchronous writes and call revalidatePath
 * to invalidate the Next.js cache so the UI reflects the latest data.
 */

import { revalidatePath } from "next/cache";
import { getDb } from "@/lib/db";

// ---------------------------------------------------------------------------
// Feedback actions
// ---------------------------------------------------------------------------

/**
 * Submits thumbs_up or thumbs_down feedback for a job.
 * Inserts a new feedback row — multiple feedback entries per job are allowed.
 * Revalidates the job detail page and the feedback history page.
 */
export async function submitFeedback(formData: FormData): Promise<void> {
  const jobId = Number(formData.get("job_id"));
  const signal = formData.get("signal") as string;
  const note = (formData.get("note") as string | null) || null;

  if (!jobId || !["thumbs_up", "thumbs_down"].includes(signal)) {
    throw new Error("Invalid feedback parameters");
  }

  const db = getDb();
  db.prepare<[number, string, string | null]>(
    "INSERT INTO feedback (job_id, signal, note) VALUES (?, ?, ?)"
  ).run(jobId, signal, note);

  revalidatePath(`/jobs/${jobId}`);
  revalidatePath("/feedback");
}

// ---------------------------------------------------------------------------
// Company CRUD actions
// ---------------------------------------------------------------------------

/**
 * Adds a new company. name is required; career_page_url is optional.
 * Revalidates the companies page.
 */
export async function addCompany(formData: FormData): Promise<void> {
  const name = (formData.get("name") as string | null)?.trim();
  const careerUrl =
    (formData.get("career_page_url") as string | null)?.trim() || null;

  if (!name) {
    throw new Error("Company name is required");
  }

  const db = getDb();
  db.prepare<[string, string | null]>(
    "INSERT INTO companies (name, career_page_url) VALUES (?, ?)"
  ).run(name, careerUrl);

  revalidatePath("/companies");
}

/**
 * Updates the career_page_url for an existing company.
 * Revalidates the companies page.
 */
export async function updateCompanyCareerUrl(formData: FormData): Promise<void> {
  const id = Number(formData.get("id"));
  const careerUrl =
    (formData.get("career_page_url") as string | null)?.trim() || null;

  if (!id) {
    throw new Error("Company id is required");
  }

  const db = getDb();
  db.prepare<[string | null, number]>(
    "UPDATE companies SET career_page_url = ? WHERE id = ?"
  ).run(careerUrl, id);

  revalidatePath("/companies");
}

/**
 * Toggles the is_target flag for a company (0 -> 1, 1 -> 0).
 * Revalidates the companies page.
 */
export async function toggleCompanyTarget(formData: FormData): Promise<void> {
  const id = Number(formData.get("id"));

  if (!id) {
    throw new Error("Company id is required");
  }

  const db = getDb();
  db.prepare<[number]>(
    "UPDATE companies SET is_target = CASE WHEN is_target = 1 THEN 0 ELSE 1 END WHERE id = ?"
  ).run(id);

  revalidatePath("/companies");
}

// ---------------------------------------------------------------------------
// Profile suggestion actions
// ---------------------------------------------------------------------------

/**
 * Approves a profile suggestion: sets status='approved' and resolved_at=now.
 * Revalidates the profile page.
 */
export async function approveSuggestion(formData: FormData): Promise<void> {
  const id = Number(formData.get("id"));

  if (!id) {
    throw new Error("Suggestion id is required");
  }

  const db = getDb();
  db.prepare<[number]>(
    "UPDATE profile_suggestions SET status = 'approved', resolved_at = datetime('now') WHERE id = ?"
  ).run(id);

  revalidatePath("/profile");
}

/**
 * Rejects a profile suggestion: sets status='rejected' and resolved_at=now.
 * Revalidates the profile page.
 */
export async function rejectSuggestion(formData: FormData): Promise<void> {
  const id = Number(formData.get("id"));

  if (!id) {
    throw new Error("Suggestion id is required");
  }

  const db = getDb();
  db.prepare<[number]>(
    "UPDATE profile_suggestions SET status = 'rejected', resolved_at = datetime('now') WHERE id = ?"
  ).run(id);

  revalidatePath("/profile");
}
