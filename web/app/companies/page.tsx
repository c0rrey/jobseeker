/**
 * Companies management page — server component.
 *
 * Shows a table of all tracked companies with enrichment status, crawl status,
 * job count, and is_target flag. Includes an AddCompanyForm and allows inline
 * editing of career URLs and toggling of target status.
 *
 * dynamic = 'force-dynamic' prevents static prerendering at build time.
 */

export const dynamic = "force-dynamic";

import { CompanyTable } from "@/components/company-table";
import { AddCompanyForm } from "@/components/add-company-form";
import { getCompanyList } from "@/lib/queries";

export default function CompaniesPage() {
  const companies = getCompanyList();
  const targetCount = companies.filter((c) => c.is_target === 1).length;
  const enrichedCount = companies.filter((c) => c.enriched_at !== null).length;

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Companies</h1>
          <p className="text-sm text-muted-foreground mt-1">
            {companies.length}{" "}
            {companies.length === 1 ? "company" : "companies"} tracked
            {targetCount > 0 && ` · ${targetCount} targeted`}
            {enrichedCount > 0 && ` · ${enrichedCount} enriched`}
          </p>
        </div>
      </div>

      {/* Add company form */}
      <AddCompanyForm />

      {/* Companies table */}
      <CompanyTable companies={companies} />
    </div>
  );
}
