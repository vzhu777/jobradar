# app/sources/greenhouse.py
"""
Greenhouse ATS scraper.
Greenhouse has a public JSON API: https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs
No authentication required — completely open.

Many ASX200 companies use Greenhouse e.g. Atlassian, Canva (private), REA Group, etc.
The board_token is the slug in the careers URL:
  https://boards.greenhouse.io/atlassian  →  board_token = "atlassian"
"""
from __future__ import annotations
import hashlib
import re
import time
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

HEADERS = {
    "User-Agent": "JobRadar/1.0 (contact: your@email.com)",
    "Accept": "application/json",
}

API_BASE = "https://boards-api.greenhouse.io/v1/boards"


def extract_board_token(board_url: str) -> str:
    """
    Extract the Greenhouse board token from a careers URL.
    e.g. https://boards.greenhouse.io/atlassian  →  atlassian
         https://boards.greenhouse.io/realestate  →  realestate
    """
    m = re.search(r"greenhouse\.io/([a-z0-9_-]+)", board_url, re.I)
    if not m:
        raise ValueError(f"Cannot extract Greenhouse board token from: {board_url}")
    return m.group(1)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch_greenhouse_jobs(board_url: str) -> list[dict]:
    """Fetch all jobs from a Greenhouse board using the public JSON API."""
    token = extract_board_token(board_url)
    api_url = f"{API_BASE}/{token}/jobs?content=true"

    resp = requests.get(api_url, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    data = resp.json()
    return data.get("jobs", [])


def normalize_greenhouse(company_name: str, board_url: str, job: dict) -> dict:
    """Normalize a raw Greenhouse job dict into our standard schema."""
    title = job.get("title", "Unknown title")
    job_url = job.get("absolute_url", "")
    job_id = str(job.get("id", ""))

    # Location: Greenhouse returns a location object
    location_obj = job.get("location", {})
    location = location_obj.get("name", "") if isinstance(location_obj, dict) else ""

    # Departments
    departments = job.get("departments", [])
    department = departments[0].get("name", "") if departments else ""

    # Content / description
    content = job.get("content", "") or ""

    # Posted at
    posted_at = job.get("updated_at") or job.get("created_at")

    content_hash = hashlib.sha256(
        f"{company_name}|{title}|{location}|{job_url}".encode()
    ).hexdigest()

    return {
        "company": company_name,
        "title": title,
        "location": location,
        "department": department,
        "url": job_url,
        "description": content,
        "source": "greenhouse",
        "source_job_id": job_id,
        "posted_at": posted_at,
        "content_hash": content_hash,
        "is_active": True,
    }


def scrape_greenhouse(company_name: str, board_url: str) -> list[dict]:
    """
    Main entry point. Returns list of normalised job dicts ready for upsert.
    """
    raw_jobs = fetch_greenhouse_jobs(board_url)
    print(f"  [Greenhouse] {company_name}: {len(raw_jobs)} jobs found")
    return [normalize_greenhouse(company_name, board_url, j) for j in raw_jobs]
