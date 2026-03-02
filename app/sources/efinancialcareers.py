# app/sources/efinancialcareers.py
"""
eFinancialCareers AU scraper — Playwright-based, matching seek.py pattern.

Covers two distinct search tracks:
  1. Technology leadership roles (CIO/CTO/Head of/Director)
  2. APAC & China business strategy/growth roles (new)

Uses Playwright for JS-rendered pages, with the same stealth settings
and delay patterns as seek.py.
"""
from __future__ import annotations
import asyncio
import hashlib
import random
import re
from datetime import datetime, timedelta

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

BASE_URL = "https://www.efinancialcareers.com.au"

# ─── Search tracks ────────────────────────────────────────────────────────────

# Track 1: Technology leadership — mirrors existing ROLE_KEYWORDS in ingest.py
TECH_LEADERSHIP_SEARCHES = [
    "Chief Information Officer",
    "Chief Technology Officer",
    "Chief Digital Officer",
    "Chief Data Officer",
    "Technology Director",
    "IT Director",
    "Head of Technology",
    "Head of IT",
    "Head of Digital",
    "Head of Engineering",
    "Head of Transformation",
    "General Manager Technology",
    "VP Technology",
    "VP Engineering",
    "Director of Technology",
    "Director of IT",
]

# Track 2: APAC & China business strategy/growth roles
APAC_CHINA_SEARCHES = [
    # APAC leadership
    "Head of APAC",
    "APAC Director",
    "APAC General Manager",
    "Regional Director APAC",
    "Managing Director APAC",

    # Business development & strategy
    "Business Development Director APAC",
    "Head of Business Development APAC",
    "APAC Business Strategy",
    "Head of Strategy APAC",
    "Strategic Partnerships APAC",
    "Market Development APAC",

    # China / Greater China specific
    "Greater China Director",
    "Head of China",
    "China Business Development",
    "China Strategy",
    "Greater China Strategy",
]

# ─── Keyword signals used in APAC relevance check ────────────────────────────
# These flag roles as APAC/China relevant during post-scrape filtering.
# Also fed into ingest.py ROLE_KEYWORDS for the is_relevant() check.
APAC_CHINA_SIGNALS = [
    # Geographic scope
    "apac", "asia pacific", "asia-pacific",
    "greater china", "china", "chinese",
    "hong kong", "singapore", "taiwan",
    "cross-border", "cross border",

    # Language signals
    "mandarin",

    # Business motion signals
    "market entry", "market expansion", "market development",
    "business development", "business strategy",
    "strategic partnerships", "growth strategy",
    "regional expansion", "international expansion",
    "china desk", "china coverage",

    # Role title signals (catch titles that weren't in search terms)
    "head of apac", "apac head", "apac director", "apac lead",
    "regional director", "regional head", "regional manager",
    "country manager", "country director",
    "managing director apac", "gm apac",
]


# ─── Date parsing ─────────────────────────────────────────────────────────────

def parse_efc_date(date_text: str) -> str | None:
    """
    Convert eFinancialCareers relative date strings to ISO 8601.
    Examples: '2 hours ago', '1 day ago', '3 days ago', '28 Feb 2026'
    Returns ISO string or None.
    """
    if not date_text:
        return None

    text = date_text.lower().strip()
    now = datetime.now()

    if any(x in text for x in ["just now", "minute", "hour"]):
        return now.isoformat()

    m = re.search(r"(\d+)\s+day", text)
    if m:
        return (now - timedelta(days=int(m.group(1)))).isoformat()

    m = re.search(r"(\d+)\s+week", text)
    if m:
        return (now - timedelta(weeks=int(m.group(1)))).isoformat()

    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).isoformat()
        except ValueError:
            pass

    return None


# ─── Normaliser ───────────────────────────────────────────────────────────────

def normalize_efc(job: dict) -> dict:
    """Normalise an eFinancialCareers job dict to the shared JobRadar schema."""
    job_id   = str(job.get("id", ""))
    title    = job.get("title", "Unknown title")
    company  = job.get("company", "Unknown Company")
    location = job.get("location", "")
    job_url  = job.get("url", "")
    snippet  = job.get("snippet", "")
    track    = job.get("track", "technology")

    description = f"eFinancialCareers job posting [{track}]."
    if snippet:
        description += f"\n\n{snippet}"
    description += f"\n\nView full details at the link."

    content_hash = hashlib.sha256(
        f"efinancialcareers|{company}|{title}|{location}|{job_url}".encode()
    ).hexdigest()

    return {
        "company":       company,
        "title":         title,
        "location":      location,
        "url":           job_url,
        "description":   description,
        "source":        "efinancialcareers",
        "source_job_id": job_id or content_hash[:16],
        "posted_at":     parse_efc_date(job.get("posted_text", "")),
        "content_hash":  content_hash,
        "is_active":     True,
    }


# ─── Page scraper ─────────────────────────────────────────────────────────────

async def scrape_efc_search(
    page,
    search_term: str,
    track: str,
    max_pages: int = 2,
) -> list[dict]:
    """
    Scrape one eFinancialCareers keyword search, up to max_pages pages.
    Returns raw (un-normalised) job dicts.
    """
    jobs = []
    encoded = search_term.replace(" ", "%20")

    for page_num in range(1, max_pages + 1):
        url = (
            f"{BASE_URL}/jobs"
            f"?q={encoded}"
            f"&location=Australia"
            f"&enableVectorSearch=true"
            f"&page={page_num}"
        )

        print(f"    [eFC] Loading page {page_num}: {search_term[:50]}")

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)

            # Wait for job results — try multiple selector patterns
            for selector in [
                '[data-testid="job-result"]',
                'article.job-result',
                '.job-item',
                '[class*="JobCard"]',
                '[class*="job-card"]',
            ]:
                try:
                    await page.wait_for_selector(selector, timeout=8000)
                    break
                except PlaywrightTimeout:
                    continue

            await asyncio.sleep(random.uniform(1.5, 3.0))

            # ── Extract job cards via JS evaluation ──────────────────────────
            raw_jobs = await page.evaluate("""
                () => {
                    const results = [];

                    // Try multiple card selectors (eFC updates their HTML periodically)
                    const cardSelectors = [
                        '[data-testid="job-result"]',
                        'article.job-result',
                        '.job-item',
                        '[class*="JobCard"]',
                        '[class*="job-card"]',
                        'li[class*="job"]',
                    ];

                    let cards = [];
                    for (const sel of cardSelectors) {
                        cards = document.querySelectorAll(sel);
                        if (cards.length > 0) break;
                    }

                    cards.forEach(card => {
                        // Title — try multiple patterns
                        const titleEl = (
                            card.querySelector('h2 a, h3 a') ||
                            card.querySelector('[class*="title"] a') ||
                            card.querySelector('[class*="JobTitle"] a') ||
                            card.querySelector('a[class*="job"]')
                        );
                        const title = titleEl ? titleEl.innerText.trim() : null;
                        if (!title) return;

                        const href = titleEl.getAttribute('href') || '';
                        const url  = href.startsWith('http')
                            ? href
                            : 'https://www.efinancialcareers.com.au' + href;

                        // Job ID from URL slug or data attribute
                        const idMatch = url.match(/\\/([a-z0-9-]+-\\d+)(?:\\?|$)/i)
                            || url.match(/jobId=([^&]+)/);
                        const id = idMatch ? idMatch[1] : url.split('/').pop().split('?')[0];

                        // Company
                        const companyEl = (
                            card.querySelector('[class*="company"]') ||
                            card.querySelector('[class*="employer"]') ||
                            card.querySelector('[class*="Employer"]') ||
                            card.querySelector('[class*="Organisation"]')
                        );
                        const company = companyEl ? companyEl.innerText.trim() : 'Unknown';

                        // Location
                        const locationEl = (
                            card.querySelector('[class*="location"]') ||
                            card.querySelector('[class*="Location"]') ||
                            card.querySelector('[class*="city"]')
                        );
                        const location = locationEl ? locationEl.innerText.trim() : '';

                        // Posted date
                        const dateEl = (
                            card.querySelector('time') ||
                            card.querySelector('[class*="date"]') ||
                            card.querySelector('[class*="Date"]') ||
                            card.querySelector('[class*="posted"]') ||
                            card.querySelector('[class*="ago"]')
                        );
                        const postedText = dateEl ? (dateEl.getAttribute('datetime') || dateEl.innerText.trim()) : '';

                        // Snippet / description excerpt
                        const snippetEl = (
                            card.querySelector('[class*="snippet"]') ||
                            card.querySelector('[class*="summary"]') ||
                            card.querySelector('[class*="description"]') ||
                            card.querySelector('[class*="excerpt"]') ||
                            card.querySelector('p')
                        );
                        const snippet = snippetEl ? snippetEl.innerText.trim().slice(0, 300) : '';

                        results.push({ id, title, company, location, url, postedText, snippet });
                    });

                    return results;
                }
            """)

            new_on_page = 0
            for rj in raw_jobs:
                if rj.get("title"):
                    jobs.append({
                        "id":          rj.get("id", ""),
                        "title":       rj.get("title", ""),
                        "company":     rj.get("company", "Unknown"),
                        "location":    rj.get("location", ""),
                        "url":         rj.get("url", ""),
                        "posted_text": rj.get("postedText", ""),
                        "snippet":     rj.get("snippet", ""),
                        "track":       track,
                        "search_term": search_term,
                    })
                    new_on_page += 1

            print(f"    [eFC] Extracted {new_on_page} jobs from page {page_num}")

            if new_on_page == 0:
                break   # no results on this page — stop paginating

            if page_num < max_pages:
                await asyncio.sleep(random.uniform(3.0, 6.0))

        except PlaywrightTimeout:
            print(f"    [eFC] Timeout on page {page_num} for '{search_term}'")
            break
        except Exception as e:
            print(f"    [eFC] Error on page {page_num} for '{search_term}': {e}")
            break

    return jobs


# ─── Main async scraper ───────────────────────────────────────────────────────

async def scrape_efc_async() -> list[dict]:
    """
    Full async scrape of both tech leadership and APAC/China tracks.
    Returns normalised job dicts ready for upsert_jobs().
    """
    all_raw: list[dict] = []
    seen_ids: set[str]  = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-AU",
            timezone_id="Australia/Melbourne",
        )

        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)

        page = await context.new_page()

        # ── Track 1: Technology leadership ───────────────────────────────────
        print(f"\n[eFC] TECHNOLOGY LEADERSHIP track ({len(TECH_LEADERSHIP_SEARCHES)} searches)...")
        for term in TECH_LEADERSHIP_SEARCHES:
            print(f"\n  [eFC] Tech search: {term}")
            jobs = await scrape_efc_search(page, term, track="technology", max_pages=2)

            new = 0
            for job in jobs:
                key = job.get("id") or job.get("url", "")
                if key and key not in seen_ids:
                    seen_ids.add(key)
                    all_raw.append(job)
                    new += 1
            print(f"  [eFC] +{new} new (running total: {len(all_raw)})")

            await asyncio.sleep(random.uniform(6.0, 12.0))

        # ── Track 2: APAC & China ─────────────────────────────────────────────
        print(f"\n[eFC] APAC & CHINA STRATEGY track ({len(APAC_CHINA_SEARCHES)} searches)...")
        for term in APAC_CHINA_SEARCHES:
            print(f"\n  [eFC] APAC/China search: {term}")
            jobs = await scrape_efc_search(page, term, track="apac_china", max_pages=2)

            new = 0
            for job in jobs:
                key = job.get("id") or job.get("url", "")
                if key and key not in seen_ids:
                    seen_ids.add(key)
                    all_raw.append(job)
                    new += 1
            print(f"  [eFC] +{new} new (running total: {len(all_raw)})")

            await asyncio.sleep(random.uniform(6.0, 12.0))

        await browser.close()

    print(f"\n[eFC] Total unique jobs scraped: {len(all_raw)}")
    return [normalize_efc(j) for j in all_raw]


# ─── Sync wrapper (matches seek.py pattern) ───────────────────────────────────

def scrape_efc_jobs() -> list[dict]:
    """
    Synchronous entry point — matches scrape_seek_senior_tech_roles() pattern.
    Called from ingest.py.
    """
    return asyncio.run(scrape_efc_async())


# ─── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    jobs = scrape_efc_jobs()
    print(f"\nFound {len(jobs)} total jobs from eFinancialCareers")

    tech_jobs  = [j for j in jobs if "technology" in j.get("description", "")]
    apac_jobs  = [j for j in jobs if "apac_china" in j.get("description", "")]

    print(f"  Technology leadership: {len(tech_jobs)}")
    print(f"  APAC & China:          {len(apac_jobs)}")

    if jobs:
        print("\nSample jobs:")
        for job in jobs[:10]:
            print(f"  [{job['source']}] {job['title']} at {job['company']} ({job['location']})")
