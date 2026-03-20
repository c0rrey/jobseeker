"use client";

/**
 * AddCompanyForm — form to add a new company to the tracking list.
 *
 * Client component for controlled form state and UX (show/hide form toggle).
 * Submits to the addCompany server action.
 * After successful submission the server action calls revalidatePath('/companies')
 * so the table updates automatically.
 */

import { useState, useTransition, useRef } from "react";
import { Plus } from "lucide-react";
import { addCompany } from "@/app/actions";

export function AddCompanyForm() {
  const [isOpen, setIsOpen] = useState(false);
  const [isPending, startTransition] = useTransition();
  const formRef = useRef<HTMLFormElement>(null);

  function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    startTransition(async () => {
      await addCompany(formData);
      formRef.current?.reset();
      setIsOpen(false);
    });
  }

  return (
    <div className="space-y-3">
      {!isOpen ? (
        <button
          type="button"
          onClick={() => setIsOpen(true)}
          className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-background px-3 py-2 text-sm font-medium hover:bg-muted transition-colors"
        >
          <Plus className="size-4" />
          Add Company
        </button>
      ) : (
        <form
          ref={formRef}
          onSubmit={handleSubmit}
          className="flex flex-wrap items-end gap-3 rounded-lg border border-border bg-card px-4 py-3"
        >
          <div className="flex flex-col gap-1">
            <label
              htmlFor="company-name"
              className="text-xs text-muted-foreground"
            >
              Company Name <span className="text-destructive">*</span>
            </label>
            <input
              id="company-name"
              name="name"
              type="text"
              required
              placeholder="Acme Corp"
              className="w-48 rounded-md border border-input bg-background px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="career-url"
              className="text-xs text-muted-foreground"
            >
              Career Page URL
            </label>
            <input
              id="career-url"
              name="career_page_url"
              type="url"
              placeholder="https://acme.com/careers"
              className="w-64 rounded-md border border-input bg-background px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>

          <div className="flex items-center gap-2">
            <button
              type="submit"
              disabled={isPending}
              className="rounded-lg bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
            >
              {isPending ? "Adding…" : "Add"}
            </button>
            <button
              type="button"
              onClick={() => setIsOpen(false)}
              disabled={isPending}
              className="rounded-lg border border-border px-3 py-1.5 text-sm font-medium hover:bg-muted disabled:opacity-50 transition-colors"
            >
              Cancel
            </button>
          </div>
        </form>
      )}
    </div>
  );
}
