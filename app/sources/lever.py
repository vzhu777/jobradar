# app/sources/lever.py
"""
Lever ATS scraper.
Lever has a public JSON API: https://api.lever.co/v0/postings/{company_slug}
No authentication required — completely open.

The company slug is the part after jobs.lever.co:
  https://jobs.lever.co/atlassian  →  slug = "atlassian"
"""
from __future__ import annotations
import hashlib
import re
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

HEADERS = {
    "User-Agent": "JobRadar/1.0 (contact: your@email.com)",
    "Accept": "application/json",
}

API_BASE = "https://api.lever.co/v0/postings"


def extract_company_slug(board_url: str) -> str:
    """
    Extract the Lever company slug from a careers URL.
    e.g. https://jobs.lever.co/atlassian  →  atlassian
    """
    m = re.search(r"lever\.co/([a-z0-9_-]+)", board_url, re.I)
    if not m:
        raise ValueError(f"Cannot extract Lever slug from: {board_url}")
    return m.group(1)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch_lever_jobs(board_url: str) -> list[dict]:
    """
    Fetch all jobs from a Lever board using the public JSON API.
    Lever paginates via `offset` cursor returned in each response.
    """
    slug = extract_company_slug(board_url)
    api_url = f"{API_BASE}/{slug}?mode=json&limit=250"

    all_jobs = []
    next_url = api_url

    while next_url:
        resp = requests.get(next_url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        # Lever returns a list directly (not wrapped in a key)
        if isinstance(data, list):
            all_jobs.extend(data)
            break  # no pagination needed when all returned at once

        # Some responses are {"data": [...], "next": "cursor"}
        if isinstance(data, dict):
            all_jobs.extend(data.get("data", []))
            next_cursor = data.get("next")
            if next_cursor:
                next_url = f"{API_BASE}/{slug}?mode=json&limit=250&offset={next_cursor}"
            else:
                break

    return all_jobs


def normalize_lever(company_name: str, board_url: str, job: dict) -> dict:
    """Normalize a raw Lever job dict into our standard schema."""
    title = job.get("text", "Unknown title")
    job_url = job.get("hostedUrl", "") or job.get("applyUrl", "")
    job_id = job.get("id", "")

    # Location: Lever returns a workplaceType and categories.location
    categories = job.get("categories", {})
    location = categories.get("location", "") or categories.get("allLocations", "")
    if isinstance(location, list):
        location = ", ".join(location)

    department = categories.get("department", "") or categories.get("team", "")

    # Description: Lever returns lists of content blocks
    description_parts = []
    for section in job.get("lists", []):
        description_parts.append(section.get("text", ""))
        for item in section.get("content", "").split("<li>"):
            clean = re.sub(r"<[^>]+>", "", item).strip()
            if clean:
                description_parts.append(f"- {clean}")

    description = "\n".join(description_parts)

    # Posted at (Lever uses millisecond timestamps)
    created_at_ms = job.get("createdAt")
    posted_at = None
    if created_at_ms:
        from datetime import datetime, timezone
        posted_at = datetime.fromtimestamp(
            created_at_ms / 1000, tz=timezone.utc
        ).isoformat()

    content_hash = hashlib.sha256(
        f"{company_name}|{title}|{location}|{job_url}".encode()
    ).hexdigest()

    return {
        "company": company_name,
        "title": title,
        "location": location,
        "department": department,
        "url": job_url,
        "description": description,
        "source": "lever",
        "source_job_id": str(job_id),
        "posted_at": posted_at,
        "content_hash": content_hash,
        "is_active": True,
    }


def scrape_lever(company_name: str, board_url: str) -> list[dict]:
    """
    Main entry point. Returns list of normalised job dicts ready for upsert.
    """
    raw_jobs = fetch_lever_jobs(board_url)
    print(f"  [Lever] {company_name}: {len(raw_jobs)} jobs found")
    return [normalize_lever(company_name, board_url, j) for j in raw_jobs]
