"use client";

/**
 * CompanyTable — interactive table for the companies management page.
 *
 * Client component because it manages inline edit state for career_page_url
 * and submits toggle/edit actions via server actions.
 *
 * Displays: name (links to filtered jobs), domain, enrichment status,
 * crawl status, job count, is_target toggle, career URL edit.
 */

import { useState, useTransition } from "react";
import Link from "next/link";
import { Star, Pencil, Check, X } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { toggleCompanyTarget, updateCompanyCareerUrl } from "@/app/actions";
import type { CompanyRow } from "@/lib/queries";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function crawlStatusVariant(
  status: string | null
): "default" | "destructive" | "secondary" | "outline" {
  if (!status) return "outline";
  if (status === "active") return "default";
  if (status === "broken") return "destructive";
  return "secondary";
}

// ---------------------------------------------------------------------------
// Inline edit row cell
// ---------------------------------------------------------------------------

function CareerUrlCell({
  companyId,
  initialUrl,
}: {
  companyId: number;
  initialUrl: string | null;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(initialUrl ?? "");
  const [isPending, startTransition] = useTransition();

  function handleSave() {
    const formData = new FormData();
    formData.set("id", String(companyId));
    formData.set("career_page_url", value);
    startTransition(async () => {
      await updateCompanyCareerUrl(formData);
      setEditing(false);
    });
  }

  if (editing) {
    return (
      <div className="flex items-center gap-1">
        <input
          type="url"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder="https://..."
          className="w-48 rounded-md border border-input bg-background px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
          autoFocus
          disabled={isPending}
          onKeyDown={(e) => {
            if (e.key === "Enter") handleSave();
            if (e.key === "Escape") setEditing(false);
          }}
        />
        <button
          type="button"
          onClick={handleSave}
          disabled={isPending}
          aria-label="Save"
          className="text-green-600 hover:text-green-700 disabled:opacity-50"
        >
          <Check className="size-4" />
        </button>
        <button
          type="button"
          onClick={() => setEditing(false)}
          disabled={isPending}
          aria-label="Cancel"
          className="text-muted-foreground hover:text-foreground"
        >
          <X className="size-4" />
        </button>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-1 group">
      <span className="text-xs text-muted-foreground truncate max-w-40">
        {initialUrl ? (
          <a
            href={initialUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="hover:text-foreground transition-colors"
          >
            {initialUrl.replace(/^https?:\/\//, "")}
          </a>
        ) : (
          "—"
        )}
      </span>
      <button
        type="button"
        onClick={() => setEditing(true)}
        aria-label="Edit career URL"
        className="opacity-0 group-hover:opacity-100 text-muted-foreground hover:text-foreground transition-opacity"
      >
        <Pencil className="size-3" />
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Target toggle cell
// ---------------------------------------------------------------------------

function TargetToggle({
  companyId,
  isTarget,
}: {
  companyId: number;
  isTarget: boolean;
}) {
  const [isPending, startTransition] = useTransition();

  function handleToggle() {
    const formData = new FormData();
    formData.set("id", String(companyId));
    startTransition(async () => {
      await toggleCompanyTarget(formData);
    });
  }

  return (
    <button
      type="button"
      onClick={handleToggle}
      disabled={isPending}
      aria-label={isTarget ? "Remove target" : "Mark as target"}
      title={isTarget ? "Remove from targets" : "Mark as target company"}
      className={[
        "transition-colors disabled:opacity-50",
        isTarget
          ? "text-yellow-500 hover:text-yellow-400"
          : "text-muted-foreground hover:text-yellow-500",
      ].join(" ")}
    >
      <Star
        className="size-4"
        fill={isTarget ? "currentColor" : "none"}
      />
    </button>
  );
}

// ---------------------------------------------------------------------------
// Table component
// ---------------------------------------------------------------------------

interface CompanyTableProps {
  companies: CompanyRow[];
}

export function CompanyTable({ companies }: CompanyTableProps) {
  if (companies.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-border p-12 text-center">
        <p className="text-sm text-muted-foreground">
          No companies tracked yet. Add one above.
        </p>
      </div>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-8"></TableHead>
          <TableHead>Name</TableHead>
          <TableHead>Domain</TableHead>
          <TableHead>Enrichment</TableHead>
          <TableHead>Crawl Status</TableHead>
          <TableHead>Jobs</TableHead>
          <TableHead>Career URL</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {companies.map((company) => (
          <TableRow key={company.id}>
            <TableCell>
              <TargetToggle
                companyId={company.id}
                isTarget={company.is_target === 1}
              />
            </TableCell>
            <TableCell className="font-medium">
              <Link
                href={`/jobs?company=${encodeURIComponent(company.name)}`}
                className="hover:text-primary transition-colors"
              >
                {company.name}
              </Link>
            </TableCell>
            <TableCell className="text-muted-foreground text-sm">
              {company.domain ?? "—"}
            </TableCell>
            <TableCell>
              {company.enriched_at ? (
                <Badge variant="default" className="text-xs">
                  Enriched
                </Badge>
              ) : (
                <Badge variant="outline" className="text-xs">
                  Pending
                </Badge>
              )}
            </TableCell>
            <TableCell>
              {company.crawl_status ? (
                <Badge
                  variant={crawlStatusVariant(company.crawl_status)}
                  className="text-xs capitalize"
                >
                  {company.crawl_status}
                </Badge>
              ) : (
                <span className="text-xs text-muted-foreground">—</span>
              )}
            </TableCell>
            <TableCell className="text-sm">{company.job_count}</TableCell>
            <TableCell>
              <CareerUrlCell
                companyId={company.id}
                initialUrl={company.career_page_url}
              />
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
