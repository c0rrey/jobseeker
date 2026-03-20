/**
 * Feedback history page — server component.
 *
 * Reads the signal filter from searchParams, fetches feedback entries from
 * SQLite, and passes them to the FeedbackList client component.
 *
 * dynamic = 'force-dynamic' prevents static prerendering at build time.
 */

export const dynamic = "force-dynamic";

import { FeedbackList } from "@/components/feedback-list";
import {
  getFeedbackHistory,
  getFeedbackCounts,
  type FeedbackSignalFilter,
} from "@/lib/queries";

const VALID_SIGNALS: FeedbackSignalFilter[] = [
  "all",
  "thumbs_up",
  "thumbs_down",
];

function parseSignal(raw: string | undefined): FeedbackSignalFilter {
  if (raw && (VALID_SIGNALS as string[]).includes(raw)) {
    return raw as FeedbackSignalFilter;
  }
  return "all";
}

export default async function FeedbackPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const sp = await searchParams;
  const signalRaw = Array.isArray(sp.signal) ? sp.signal[0] : sp.signal;
  const signal = parseSignal(signalRaw);

  const entries = getFeedbackHistory(signal);
  const counts = getFeedbackCounts();

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">
          Feedback History
        </h1>
        <p className="text-sm text-muted-foreground mt-1">
          Your job feedback signals and notes.
        </p>
      </div>

      <FeedbackList entries={entries} signal={signal} counts={counts} />
    </div>
  );
}
