import time
from app.db import fetch_companies, upsert_jobs
from app.sources.workday_playwright import fetch_workday_jobs_sync
from app.sources.workday import normalize_workday
from app.sources.greenhouse import scrape_greenhouse
from app.sources.lever import scrape_lever
from app.sources.linkedin import scrape_linkedin_senior_tech_roles
from app.sources.seek import scrape_seek_senior_tech_roles
from app.sources.efinancialcareers import scrape_efc_jobs, APAC_CHINA_SIGNALS
from app.emailer import send_email

# ─── EXPANDED Relevance filters ──────────────────────────────────────────────
ROLE_KEYWORDS = [
    # C-Level & Executive
    "chief information", "chief technology", "chief digital", "chief data", "chief innovation",
    "cio", "cto", "cdo", "cdao", "chief",
    
    # Director Level
    "director", "program director", "programme director", "project director",
    "director of technology", "director of engineering", "director of it", "director of digital",
    
    # VP & Head Level
    "vice president", "vp technology", "vp engineering", "vp digital", "vp it",
    "head of technology", "head of digital", "head of it", "head of data",
    "head of engineering", "head of transformation", "head of innovation",
    
    # General Manager Level
    "general manager", " gm ", "gm technology", "gm digital", "gm it",
    
    # Senior Manager / Principal Level
    "senior manager technology", "senior manager it", "senior manager digital",
    "senior program manager", "senior project manager",
    "principal engineer", "principal architect", "principal consultant",
    "senior engineering manager", "senior product manager",
    
    # Program/Project Management
    "program manager", "programme manager",
    
    # Functional Keywords
    "transformation", "technology", "digital", "information technology",
    "enterprise architecture", "platform engineering", "infrastructure",
    "apac", "global",

    # ── APAC & China expansion ─────────────────────────────────────────────
    # These are imported from efinancialcareers.py and appended below.
    # Defined here as empty placeholder; populated at module load via
    # ROLE_KEYWORDS.extend(APAC_CHINA_SIGNALS) after imports resolve.
]

# Extend with APAC/China signals so is_relevant() catches eFC roles from
# both the technology and apac_china tracks.
ROLE_KEYWORDS.extend(APAC_CHINA_SIGNALS)

AU_LOCATIONS = [
    "australia", "melbourne", "sydney", "brisbane",
    "perth", "adelaide", "canberra", "hobart", "darwin",
    "remote", "au", "nsw", "vic", "qld", "wa", "sa", "act", "tas", "nt",
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
    
    print(f"\n{'='*80}")
    print("Starting job ingestion...")
    print("=" * 80)

    all_relevant_new = []
    stats = {"total_jobs": 0, "new_jobs": 0, "relevant_new": 0, "companies_processed": 0}

    # ─── STEP 1: Scrape company career sites ─────────────────────────────────
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
        
        normalized = deduplicate_jobs(normalized)
        print(f"  ✓ Deduplicated to {len(normalized)} unique jobs")
        
        stats["total_jobs"] += len(normalized)

        saved = upsert_jobs(normalized)
        print(f"  ✓ Upserted {len(saved)} rows to database")

        new_jobs = [j for j in saved if j.get("created_at") == j.get("updated_at")]
        relevant = [j for j in new_jobs if is_relevant(j)]

        stats["new_jobs"] += len(new_jobs)
        stats["relevant_new"] += len(relevant)
        stats["companies_processed"] += 1

        print(f"  → New jobs this run: {len(new_jobs)}")
        print(f"  → Relevant new jobs: {len(relevant)}")
        
        if relevant:
            print(f"  → Relevant roles found:")
            for j in relevant[:5]:
                print(f"     • {j['title']} ({j.get('location', 'N/A')})")
            if len(relevant) > 5:
                print(f"     ... and {len(relevant) - 5} more")
        
        all_relevant_new.extend(relevant)
        time.sleep(1.0)

    # ─── STEP 2: Scrape LinkedIn ─────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("LINKEDIN JOB SEARCH")
    print(f"{'='*80}")
    
    try:
        linkedin_jobs = scrape_linkedin_senior_tech_roles()
        
        if linkedin_jobs:
            print(f"\n✓ Scraped {len(linkedin_jobs)} jobs from LinkedIn")
            
            linkedin_jobs = deduplicate_jobs(linkedin_jobs)
            print(f"✓ Deduplicated to {len(linkedin_jobs)} unique LinkedIn jobs")
            
            stats["total_jobs"] += len(linkedin_jobs)
            
            saved_linkedin = upsert_jobs(linkedin_jobs)
            print(f"✓ Upserted {len(saved_linkedin)} LinkedIn jobs to database")
            
            new_linkedin = [j for j in saved_linkedin if j.get("created_at") == j.get("updated_at")]
            relevant_linkedin = [j for j in new_linkedin if is_relevant(j)]
            
            stats["new_jobs"] += len(new_linkedin)
            stats["relevant_new"] += len(relevant_linkedin)
            
            print(f"→ New LinkedIn jobs this run: {len(new_linkedin)}")
            print(f"→ Relevant new LinkedIn jobs: {len(relevant_linkedin)}")
            
            if relevant_linkedin:
                print(f"→ Sample LinkedIn roles:")
                for j in relevant_linkedin[:5]:
                    print(f"   • {j['title']} at {j['company']} ({j.get('location', 'N/A')})")
                if len(relevant_linkedin) > 5:
                    print(f"   ... and {len(relevant_linkedin) - 5} more")
            
            all_relevant_new.extend(relevant_linkedin)
        else:
            print("⚠ No jobs returned from LinkedIn")
    
    except Exception as e:
        print(f"✗ LinkedIn scraping failed: {e}")
        import traceback
        traceback.print_exc()

    # ─── STEP 3: Scrape Seek ─────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("SEEK JOB SEARCH")
    print(f"{'='*80}")
    
    try:
        seek_jobs = scrape_seek_senior_tech_roles()
        
        if seek_jobs:
            print(f"\n✓ Scraped {len(seek_jobs)} jobs from Seek")
            
            seek_jobs = deduplicate_jobs(seek_jobs)
            print(f"✓ Deduplicated to {len(seek_jobs)} unique Seek jobs")
            
            stats["total_jobs"] += len(seek_jobs)
            
            saved_seek = upsert_jobs(seek_jobs)
            print(f"✓ Upserted {len(saved_seek)} Seek jobs to database")
            
            new_seek = [j for j in saved_seek if j.get("created_at") == j.get("updated_at")]
            relevant_seek = [j for j in new_seek if is_relevant(j)]
            
            stats["new_jobs"] += len(new_seek)
            stats["relevant_new"] += len(relevant_seek)
            
            print(f"→ New Seek jobs this run: {len(new_seek)}")
            print(f"→ Relevant new Seek jobs: {len(relevant_seek)}")
            
            if relevant_seek:
                print(f"→ Sample Seek roles:")
                for j in relevant_seek[:5]:
                    print(f"   • {j['title']} at {j['company']} ({j.get('location', 'N/A')})")
                if len(relevant_seek) > 5:
                    print(f"   ... and {len(relevant_seek) - 5} more")
            
            all_relevant_new.extend(relevant_seek)
        else:
            print("⚠ No jobs returned from Seek")
    
    except Exception as e:
        print(f"✗ Seek scraping failed: {e}")
        import traceback
        traceback.print_exc()

    # ─── STEP 4: Scrape eFinancialCareers ────────────────────────────────────
    print(f"\n{'='*80}")
    print("EFINANCIALCAREERS JOB SEARCH")
    print(f"{'='*80}")
    
    try:
        efc_jobs = scrape_efc_jobs()
        
        if efc_jobs:
            print(f"\n✓ Scraped {len(efc_jobs)} jobs from eFinancialCareers")
            
            efc_jobs = deduplicate_jobs(efc_jobs)
            print(f"✓ Deduplicated to {len(efc_jobs)} unique eFC jobs")
            
            stats["total_jobs"] += len(efc_jobs)
            
            saved_efc = upsert_jobs(efc_jobs)
            print(f"✓ Upserted {len(saved_efc)} eFC jobs to database")
            
            new_efc = [j for j in saved_efc if j.get("created_at") == j.get("updated_at")]
            relevant_efc = [j for j in new_efc if is_relevant(j)]
            
            stats["new_jobs"] += len(new_efc)
            stats["relevant_new"] += len(relevant_efc)
            
            print(f"→ New eFC jobs this run: {len(new_efc)}")
            print(f"→ Relevant new eFC jobs: {len(relevant_efc)}")
            
            if relevant_efc:
                # Sub-group by track for readable console output
                tech_roles = [j for j in relevant_efc if "technology" in (j.get("description") or "")]
                apac_roles = [j for j in relevant_efc if "apac_china" in (j.get("description") or "")]
                other_roles = [j for j in relevant_efc if j not in tech_roles and j not in apac_roles]

                if tech_roles:
                    print(f"\n  📱 Technology leadership roles ({len(tech_roles)}):")
                    for j in tech_roles[:5]:
                        print(f"     • {j['title']} at {j['company']} ({j.get('location', 'N/A')})")
                    if len(tech_roles) > 5:
                        print(f"     ... and {len(tech_roles) - 5} more")

                if apac_roles:
                    print(f"\n  🌏 APAC & China strategy roles ({len(apac_roles)}):")
                    for j in apac_roles[:5]:
                        print(f"     • {j['title']} at {j['company']} ({j.get('location', 'N/A')})")
                    if len(apac_roles) > 5:
                        print(f"     ... and {len(apac_roles) - 5} more")

                if other_roles:
                    print(f"\n  📋 Other relevant roles ({len(other_roles)}):")
                    for j in other_roles[:3]:
                        print(f"     • {j['title']} at {j['company']} ({j.get('location', 'N/A')})")
            
            all_relevant_new.extend(relevant_efc)
        else:
            print("⚠ No jobs returned from eFinancialCareers")
    
    except Exception as e:
        print(f"✗ eFinancialCareers scraping failed: {e}")
        import traceback
        traceback.print_exc()

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
        
        def make_section(title: str, jobs: list[dict], color: str) -> str:
            if not jobs:
                return ""
            items = "\n".join(
                f"<li style='margin-bottom:8px'>"
                f"<b>{j['company']}</b> — "
                f"<a href='{j['url']}' style='color:#0066cc'>{j['title']}</a> "
                f"<span style='color:#666'>({j.get('location','')}) "
                f"[{j.get('source','')}]</span>"
                f"</li>"
                for j in jobs
            )
            return f"""
            <h3 style='color:{color};margin-top:24px;margin-bottom:8px'>{title}</h3>
            <ul style='line-height:1.8;padding-left:20px'>{items}</ul>
            """

        # Split into three groups for the email
        apac_china_jobs = [
            j for j in all_relevant_new
            if j.get("source") == "efinancialcareers"
            and "apac_china" in (j.get("description") or "")
        ]
        tech_efc_jobs = [
            j for j in all_relevant_new
            if j.get("source") == "efinancialcareers"
            and "apac_china" not in (j.get("description") or "")
        ]
        other_jobs = [
            j for j in all_relevant_new
            if j.get("source") != "efinancialcareers"
        ]

        html = f"""
        <html><body style='font-family:Arial,sans-serif;max-width:700px;margin:0 auto'>
        <h2 style='color:#1a1a2e;border-bottom:2px solid #1a1a2e;padding-bottom:8px'>
            JobRadar — {len(all_relevant_new)} New Relevant Roles
        </h2>
        <p style='color:#444'>
            Found <b>{len(all_relevant_new)}</b> new roles matching your criteria
            across {len(set(j.get('source') for j in all_relevant_new))} sources.
        </p>

        {make_section('🌏 APAC & China Strategy Roles', apac_china_jobs, '#c0392b')}
        {make_section('💻 Technology Leadership (eFinancialCareers)', tech_efc_jobs, '#2980b9')}
        {make_section('📋 Technology Leadership (LinkedIn / Seek / ATS)', other_jobs, '#27ae60')}

        <hr style='margin-top:32px'>
        <p style='color:#999;font-size:12px'>
            JobRadar • ASX200 + LinkedIn + Seek + eFinancialCareers monitor
        </p>
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
