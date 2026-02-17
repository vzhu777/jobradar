import time
from app.db import fetch_companies, upsert_jobs
from app.sources.workday_playwright import fetch_workday_jobs_sync
from app.sources.workday import normalize_workday
from app.sources.greenhouse import scrape_greenhouse
from app.sources.lever import scrape_lever
from app.emailer import send_email

# ─── Relevance filters ───────────────────────────────────────────────────────
ROLE_KEYWORDS = [
    "chief information", "chief technology", "chief digital", "chief data",
    "cio", "cto", "cdo", "cdao",
    "general manager", " gm ",
    "head of technology", "head of digital", "head of it", "head of data",
    "head of transformation", "head of engineering",
    "director", "program director", "programme director",
    "vp technology", "vp engineering", "vice president",
    "transformation", "technology", "digital", "information technology",
]

AU_LOCATIONS = [
    "australia", "melbourne", "sydney", "brisbane",
    "perth", "adelaide", "canberra", "remote", "au",
]


def is_relevant(job: dict) -> bool:
    title = (job.get("title") or "").lower()
    loc = (job.get("location") or "").lower()
    title_match = any(k in title for k in ROLE_KEYWORDS)
    loc_match = any(a in loc for a in AU_LOCATIONS) or loc == ""
    return title_match and loc_match


def deduplicate_jobs(jobs: list[dict]) -> list[dict]:
    """Remove duplicate jobs based on (source, source_job_id)."""
    seen = set()
    deduped = []
    
    for job in jobs:
        key = (job.get("source"), job.get("source_job_id"))
        if key not in seen:
            seen.add(key)
            deduped.append(job)
    
    return deduped


# ─── Source dispatchers ───────────────────────────────────────────────────────

def ingest_workday(company: dict) -> list[dict]:
    name = company["name"]
    board_url = company["ats_board_url"]
    page_size = 20
    offset = 0
    raw = []
    seen_ids = set()

    print(f"  [Workday] Starting pagination for {name}...")

    while True:
        print(f"    Fetching page at offset {offset}...")
        batch = fetch_workday_jobs_sync(
            board_url,
            search_text="",
            limit=page_size,
            offset=offset,
        )

        if not batch:
            print("    No more jobs returned → stopping pagination")
            break

        new_items = []
        for j in batch:
            jid = j.get("externalPath") or j.get("id") or str(j)
            if jid not in seen_ids:
                seen_ids.add(jid)
                new_items.append(j)

        if not new_items:
            print("    Pagination repeating same jobs → stopping")
            break

        raw.extend(new_items)
        print(f"    Collected {len(new_items)} new jobs (total: {len(raw)})")

        if len(batch) < page_size:
            print("    Last page reached")
            break

        offset += page_size
        if offset > 2000:
            print("    Safety stop triggered (offset > 2000)")
            break

    print(f"  [Workday] {name}: {len(raw)} total jobs fetched")
    return [normalize_workday(name, board_url, j) for j in raw]


def ingest_greenhouse(company: dict) -> list[dict]:
    return scrape_greenhouse(company["name"], company["ats_board_url"])


def ingest_lever(company: dict) -> list[dict]:
    return scrape_lever(company["name"], company["ats_board_url"])


# ─── Source registry ──────────────────────────────────────────────────────────

SOURCE_HANDLERS = {
    "workday": ingest_workday,
    "greenhouse": ingest_greenhouse,
    "lever": ingest_lever,
}


# ─── Main run ─────────────────────────────────────────────────────────────────

def run():
    print("=" * 80)
    print("JobRadar Ingestion Starting")
    print("=" * 80)
    
    companies = fetch_companies()
    print(f"\n✓ Found {len(companies)} total companies in database")

    # Filter to companies with recognized ATS
    companies_with_ats = [
        c for c in companies 
        if c.get("ats_type") in SOURCE_HANDLERS and c.get("ats_board_url")
    ]
    
    print(f"✓ {len(companies_with_ats)} companies have recognized ATS systems:")
    for c in companies_with_ats:
        print(f"  - {c['ticker']:6} {c['name']:40} [{c['ats_type']}]")
    
    if not companies_with_ats:
        print("\n⚠ No companies with supported ATS found. Run discovery first!")
        print("   python -m app.discover")
        return
    
    print(f"\n{'='*80}")
    print("Starting job ingestion...")
    print("=" * 80)

    all_relevant_new = []
    stats = {"total_jobs": 0, "new_jobs": 0, "relevant_new": 0, "companies_processed": 0}

    for c in companies_with_ats:
        ats_type = c["ats_type"]
        name = c["name"]
        ticker = c.get("ticker", "")

        handler = SOURCE_HANDLERS[ats_type]

        print(f"\n{'─'*80}")
        print(f"→ [{ticker}] {name} ({ats_type.upper()})")
        print(f"{'─'*80}")

        try:
            normalized = handler(c)
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            import traceback
            traceback.print_exc()
            continue

        # Attach company_id
        for n in normalized:
            n["company_id"] = c["id"]

        if not normalized:
            print(f"  ⚠ No jobs returned")
            continue

        print(f"  ✓ Normalized {len(normalized)} jobs")
        
        # CRITICAL FIX: Deduplicate before upserting
        normalized = deduplicate_jobs(normalized)
        print(f"  ✓ Deduplicated to {len(normalized)} unique jobs")
        
        stats["total_jobs"] += len(normalized)

        saved = upsert_jobs(normalized)
        print(f"  ✓ Upserted {len(saved)} rows to database")

        # Detect new jobs (created_at == updated_at means freshly inserted)
        new_jobs = [j for j in saved if j.get("created_at") == j.get("updated_at")]
        relevant = [j for j in new_jobs if is_relevant(j)]

        stats["new_jobs"] += len(new_jobs)
        stats["relevant_new"] += len(relevant)
        stats["companies_processed"] += 1

        print(f"  → New jobs this run: {len(new_jobs)}")
        print(f"  → Relevant new jobs: {len(relevant)}")
        
        if relevant:
            print(f"  → Relevant roles found:")
            for j in relevant[:5]:  # Show first 5
                print(f"     • {j['title']} ({j.get('location', 'N/A')})")
            if len(relevant) > 5:
                print(f"     ... and {len(relevant) - 5} more")
        
        all_relevant_new.extend(relevant)
        time.sleep(1.0)  # be polite between companies

    # ── Summary and Email ─────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("INGESTION COMPLETE")
    print(f"{'='*80}")
    print(f"Companies processed:     {stats['companies_processed']}")
    print(f"Total jobs scraped:      {stats['total_jobs']}")
    print(f"New jobs found:          {stats['new_jobs']}")
    print(f"Relevant new jobs:       {stats['relevant_new']}")
    print(f"{'='*80}\n")

    if all_relevant_new:
        print(f"📧 Sending email with {len(all_relevant_new)} relevant roles...\n")
        
        items = "\n".join(
            f"<li><b>{j['company']}</b> — <a href='{j['url']}'>{j['title']}</a> "
            f"<span style='color:#666'>({j.get('location','')}) [{j.get('source','')}]</span></li>"
            for j in all_relevant_new
        )
        html = f"""
        <html><body>
        <h2 style='color:#1a1a2e'>JobRadar — New Relevant Roles</h2>
        <p>Found <b>{len(all_relevant_new)}</b> new relevant roles matching your criteria:</p>
        <ul style='line-height:1.8'>{items}</ul>
        <hr>
        <p style='color:#999;font-size:12px'>JobRadar • ASX200 job monitor</p>
        </body></html>
        """
        try:
            send_email(
                subject=f"JobRadar — {len(all_relevant_new)} new relevant roles found",
                html_body=html,
            )
            print("✓ Email sent successfully!")
        except Exception as e:
            print(f"✗ Email failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("ℹ No relevant new jobs found this run — no email sent.")
    
    print("\nDone! 🎯\n")


if __name__ == "__main__":
    run()
