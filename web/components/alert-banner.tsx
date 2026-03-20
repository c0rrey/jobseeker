/**
 * AlertBanner — displays a warning banner with a count badge.
 *
 * Pure server component (no 'use client'). Used on the dashboard for
 * broken career page configs and pending profile suggestions.
 */

interface AlertBannerProps {
  count: number;
  label: string;
  /** Optional href to navigate when the banner is clicked/linked. Not used in initial implementation. */
  href?: string;
}

export function AlertBanner({ count, label }: AlertBannerProps) {
  if (count === 0) {
    return null;
  }

  return (
    <div
      role="alert"
      className="flex items-center gap-3 rounded-lg border border-yellow-200 bg-yellow-50 px-4 py-3 text-sm text-yellow-800 dark:border-yellow-800 dark:bg-yellow-950 dark:text-yellow-200"
    >
      <span className="inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-yellow-400 px-1.5 text-xs font-bold text-yellow-900 dark:bg-yellow-700 dark:text-yellow-100">
        {count}
      </span>
      <span>{label}</span>
    </div>
  );
}
