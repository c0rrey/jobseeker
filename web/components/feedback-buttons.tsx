"use client";

/**
 * FeedbackButtons — thumbs up / thumbs down buttons with optional note.
 *
 * Client component because it manages local expand/collapse state for
 * the note textarea. Submits to the submitFeedback server action via
 * a hidden form with the job_id and signal values.
 *
 * Shows the current feedback signal (if any) as a highlighted active state.
 */

import { useState, useTransition } from "react";
import { ThumbsUp, ThumbsDown } from "lucide-react";
import { submitFeedback } from "@/app/actions";

interface FeedbackButtonsProps {
  jobId: number;
  currentSignal: "thumbs_up" | "thumbs_down" | null;
}

export function FeedbackButtons({
  jobId,
  currentSignal,
}: FeedbackButtonsProps) {
  const [note, setNote] = useState("");
  const [showNote, setShowNote] = useState(false);
  const [pendingSignal, setPendingSignal] = useState<
    "thumbs_up" | "thumbs_down" | null
  >(null);
  const [isPending, startTransition] = useTransition();

  function handleFeedback(signal: "thumbs_up" | "thumbs_down") {
    setPendingSignal(signal);
    const formData = new FormData();
    formData.set("job_id", String(jobId));
    formData.set("signal", signal);
    if (note.trim()) {
      formData.set("note", note.trim());
    }
    startTransition(async () => {
      await submitFeedback(formData);
      setNote("");
      setShowNote(false);
      setPendingSignal(null);
    });
  }

  const activeSignal = isPending ? pendingSignal : currentSignal;

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={() => handleFeedback("thumbs_up")}
          disabled={isPending}
          aria-label="Thumbs up"
          className={[
            "inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-sm font-medium transition-colors disabled:opacity-50",
            activeSignal === "thumbs_up"
              ? "border-green-500 bg-green-50 text-green-700 dark:bg-green-950 dark:text-green-300"
              : "border-border bg-background hover:bg-muted",
          ].join(" ")}
        >
          <ThumbsUp className="size-4" />
          Good fit
        </button>

        <button
          type="button"
          onClick={() => handleFeedback("thumbs_down")}
          disabled={isPending}
          aria-label="Thumbs down"
          className={[
            "inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-sm font-medium transition-colors disabled:opacity-50",
            activeSignal === "thumbs_down"
              ? "border-red-500 bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-300"
              : "border-border bg-background hover:bg-muted",
          ].join(" ")}
        >
          <ThumbsDown className="size-4" />
          Not a fit
        </button>

        <button
          type="button"
          onClick={() => setShowNote((v) => !v)}
          className="text-xs text-muted-foreground hover:text-foreground transition-colors ml-1"
        >
          {showNote ? "Hide note" : "Add note"}
        </button>
      </div>

      {showNote && (
        <textarea
          value={note}
          onChange={(e) => setNote(e.target.value)}
          placeholder="Optional note (saved with your next feedback)..."
          rows={2}
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-ring"
        />
      )}
    </div>
  );
}
