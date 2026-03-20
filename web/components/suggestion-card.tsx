"use client";

/**
 * SuggestionCard — displays a profile suggestion with Approve/Reject buttons.
 *
 * Client component because it manages optimistic disabled state while the
 * server action is pending (via useTransition).
 *
 * For pending suggestions: shows Approve and Reject buttons.
 * For resolved suggestions: shows the status as a read-only badge.
 */

import { useTransition } from "react";
import { CheckCircle, XCircle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { approveSuggestion, rejectSuggestion } from "@/app/actions";
import type { ProfileSuggestion } from "@/lib/types";

interface SuggestionCardProps {
  suggestion: ProfileSuggestion;
}

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

export function SuggestionCard({ suggestion }: SuggestionCardProps) {
  const [isPending, startTransition] = useTransition();

  function handleApprove() {
    const formData = new FormData();
    formData.set("id", String(suggestion.id));
    startTransition(async () => {
      await approveSuggestion(formData);
    });
  }

  function handleReject() {
    const formData = new FormData();
    formData.set("id", String(suggestion.id));
    startTransition(async () => {
      await rejectSuggestion(formData);
    });
  }

  const statusBadge = {
    pending: (
      <Badge variant="secondary" className="text-xs">
        Pending
      </Badge>
    ),
    approved: (
      <Badge variant="default" className="text-xs bg-green-600 text-white border-green-700">
        Approved
      </Badge>
    ),
    rejected: (
      <Badge variant="destructive" className="text-xs">
        Rejected
      </Badge>
    ),
  }[suggestion.status];

  return (
    <div className="rounded-lg border border-border bg-card p-4 space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="space-y-1 flex-1">
          <div className="flex items-center gap-2 flex-wrap">
            {statusBadge}
            <span className="text-xs text-muted-foreground capitalize">
              {suggestion.suggestion_type.replace(/_/g, " ")}
            </span>
            <span className="text-xs text-muted-foreground">
              {formatDate(suggestion.created_at)}
            </span>
          </div>
          <p className="text-sm font-medium">{suggestion.description}</p>
          <p className="text-xs text-muted-foreground leading-relaxed">
            {suggestion.reasoning}
          </p>
        </div>
      </div>

      {suggestion.suggested_change && (
        <details className="text-xs">
          <summary className="cursor-pointer text-muted-foreground hover:text-foreground transition-colors select-none">
            View suggested change
          </summary>
          <pre className="mt-2 rounded-md bg-muted px-3 py-2 text-xs overflow-x-auto whitespace-pre-wrap">
            {(() => {
              try {
                return JSON.stringify(
                  JSON.parse(suggestion.suggested_change),
                  null,
                  2
                );
              } catch {
                return suggestion.suggested_change;
              }
            })()}
          </pre>
        </details>
      )}

      {suggestion.status === "pending" && (
        <div className="flex items-center gap-2 pt-1">
          <button
            type="button"
            onClick={handleApprove}
            disabled={isPending}
            className="inline-flex items-center gap-1.5 rounded-lg border border-green-300 bg-green-50 px-3 py-1.5 text-xs font-medium text-green-700 hover:bg-green-100 disabled:opacity-50 transition-colors dark:border-green-700 dark:bg-green-950 dark:text-green-300"
          >
            <CheckCircle className="size-3.5" />
            Approve
          </button>
          <button
            type="button"
            onClick={handleReject}
            disabled={isPending}
            className="inline-flex items-center gap-1.5 rounded-lg border border-red-300 bg-red-50 px-3 py-1.5 text-xs font-medium text-red-700 hover:bg-red-100 disabled:opacity-50 transition-colors dark:border-red-700 dark:bg-red-950 dark:text-red-300"
          >
            <XCircle className="size-3.5" />
            Reject
          </button>
        </div>
      )}

      {suggestion.resolved_at && (
        <p className="text-xs text-muted-foreground">
          Resolved {formatDate(suggestion.resolved_at)}
        </p>
      )}
    </div>
  );
}
