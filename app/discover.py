import re
import time
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from app.db import sb  # reuse the Supabase client

HEADERS = {
    "User-Agent": "JobRadar/1.0 (contact: victor@oryndraconsulting.com)"
}

ATS_PATTERNS = [
    ("greenhouse", re.compile(r"https?://boards\.greenhouse\.io/[a-z0-9_-]+", re.I)),
    ("lever", re.compile(r"https?://jobs\.lever\.co/[a-z0-9_-]+", re.I)),
    ("workday", re.compile(r"https?://[^\"'\s]+myworkdayjobs\.com[^\"'\s]*", re.I)),
    ("smartrecruiters", re.compile(r"https?://(?:www\.)?smartrecruiters\.com/[^\"'\s]+", re.I)),
    ("successfactors", re.compile(r"https?://[^\"'\s]*successfactors\.com[^\"'\s]*", re.I)),
    ("icims", re.compile(r"https?://[^\"'\s]*icims\.com[^\"'\s]*", re.I)),
    ("jobvite", re.compile(r"https?://[^\"'\s]*jobvite\.com[^\"'\s]*", re.I)),
]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
    r.raise_for_status()
    return r.text

def find_careers_link(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    # 1️⃣ Look for links in the page
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"].strip()

        if any(k in text for k in ["careers", "career", "jobs", "join us", "work with us"]):
            if href.startswith("http"):
                return href
            if href.startswith("/"):
                return base_url.rstrip("/") + href

    # 2️⃣ Fallback: try common careers URLs automatically
    fallbacks = [
        "/careers",
        "/career",
        "/jobs",
        "/about/careers",
        "/about-us/careers",
    ]

    for f in fallbacks:
        test_url = base_url.rstrip("/") + f
        try:
            r = requests.get(test_url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                return test_url
        except:
            pass

    return None

def detect_ats(html: str) -> tuple[str, str] | None:
    for ats_type, pattern in ATS_PATTERNS:
        m = pattern.search(html)
        if m:
            return ats_type, m.group(0)
    return None

def update_company(company_id: str, fields: dict):
    sb.table("companies").update(fields).eq("id", company_id).execute()

def discover_for_company(row: dict) -> dict:
    name = row["name"]
    website = row.get("website_url")
    if not website:
        return {"notes": "No website_url set."}

    result = {"notes": None, "careers_url": None, "ats_type": "unknown", "ats_board_url": None}

    try:
        home = fetch_html(website)
    except Exception as e:
        return {"notes": f"Failed to fetch homepage: {e}"}

    # ATS might already be on homepage
    detected = detect_ats(home)
    if detected:
        result["ats_type"], result["ats_board_url"] = detected
        return result

    careers_url = find_careers_link(home, website)
    if not careers_url:
        result["notes"] = "No careers link found on homepage."
        return result

    result["careers_url"] = careers_url

    try:
        careers_html = fetch_html(careers_url)
    except Exception as e:
        result["notes"] = f"Careers page found but failed to fetch: {e}"
        return result

    detected = detect_ats(careers_html)

    if not detected:
        # some pages hide links in hrefs; check visible href attributes
        soup = BeautifulSoup(careers_html, "html.parser")
        hrefs = " ".join(a["href"] for a in soup.find_all("a", href=True))
        detected = detect_ats(hrefs)

    if detected:
        result["ats_type"], result["ats_board_url"] = detected
    else:
        result["notes"] = "Careers page found but ATS not detected (possibly JS-rendered)."

    return result

def run_discovery(limit: int = 50):
    # CRITICAL FIX: Only discover companies that HAVE website_url
    rows = (
        sb.table("companies")
        .select("*")
        .not_.is_("website_url", "null")  # Only companies with websites
        .or_("ats_type.is.null,ats_type.eq.unknown,ats_board_url.is.null")
        .limit(limit)
        .execute()
        .data
    )

    print(f"Discovering ATS for {len(rows)} companies (with websites)...\n")

    for row in rows:
        print(f"→ {row.get('ticker','')} {row['name']}")
        try:
            discovered = discover_for_company(row)
            update_company(row["id"], discovered)
            if discovered.get('careers_url'):
                print(f"   careers_url: {discovered.get('careers_url')}")
            print(f"   ats_type: {discovered.get('ats_type')}")
            if discovered.get('ats_board_url'):
                print(f"   ats_board_url: {discovered.get('ats_board_url')}")
            if discovered.get("notes"):
                print(f"   notes: {discovered['notes']}")
        except Exception as e:
            update_company(row["id"], {"notes": f"Discovery error: {e}"})
            print(f"   ERROR: {e}")
        print()

        time.sleep(1.0)  # be polite

if __name__ == "__main__":
    run_discovery(limit=50)
