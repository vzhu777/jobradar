# app/sources/seek.py
"""
Seek.com.au scraper using Playwright for browser automation.
Uses headless browser to avoid API blocking.
"""
from __future__ import annotations
import asyncio
import hashlib
import time
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup


async def scrape_seek_search(keywords: str, location: str = "All Australia", max_pages: int = 2) -> list[dict]:
    """
    Scrape Seek search results using Playwright.
    
    Args:
        keywords: Search terms
        location: Location filter
        max_pages: Max pages to scrape
    """
    jobs = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Build search URL
        base_url = f"https://www.seek.com.au/{keywords.lower().replace(' ', '-')}-jobs"
        
        for page_num in range(1, max_pages + 1):
            url = f"{base_url}?page={page_num}" if page_num > 1 else base_url
            
            print(f"  [Seek] Fetching page {page_num} for '{keywords}'...")
            
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)  # Let JS render
                
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                
                # Find job cards (Seek uses article tags with data-job-id)
                job_cards = soup.find_all("article", {"data-job-id": True})
                
                if not job_cards:
                    print(f"  [Seek] No job cards found on page {page_num}")
                    break
                
                for card in job_cards:
                    try:
                        job_id = card.get("data-job-id")
                        
                        # Title
                        title_elem = card.find("a", {"data-automation": "jobTitle"})
                        title = title_elem.get_text(strip=True) if title_elem else "Unknown"
                        
                        # Company
                        company_elem = card.find("a", {"data-automation": "jobCompany"})
                        company = company_elem.get_text(strip=True) if company_elem else "Unknown Company"
                        
                        # Location
                        location_elem = card.find("a", {"data-automation": "jobLocation"})
                        job_location = location_elem.get_text(strip=True) if location_elem else ""
                        
                        # Short description
                        desc_elem = card.find("span", {"data-automation": "jobShortDescription"})
                        description = desc_elem.get_text(strip=True) if desc_elem else ""
                        
                        # Salary
                        salary_elem = card.find("span", {"data-automation": "jobSalary"})
                        salary = salary_elem.get_text(strip=True) if salary_elem else ""
                        
                        # Work type
                        work_type_elem = card.find("span", {"data-automation": "jobWorkType"})
                        work_type = work_type_elem.get_text(strip=True) if work_type_elem else ""
                        
                        # Listing date
                        date_elem = card.find("span", {"data-automation": "jobListingDate"})
                        listing_date = date_elem.get_text(strip=True) if date_elem else ""
                        
                        # Build full description
                        full_desc = description
                        if work_type:
                            full_desc += f"\n\nWork Type: {work_type}"
                        if salary:
                            full_desc += f"\nSalary: {salary}"
                        
                        jobs.append({
                            "id": job_id,
                            "title": title,
                            "company": company,
                            "location": job_location,
                            "description": full_desc,
                            "salary": salary,
                            "work_type": work_type,
                            "listing_date": listing_date,
                        })
                    
                    except Exception as e:
                        print(f"  [Seek] Error parsing job card: {e}")
                        continue
                
                print(f"  [Seek] Found {len(job_cards)} jobs on page {page_num}")
                
            except Exception as e:
                print(f"  [Seek] Error fetching page {page_num}: {e}")
                break
        
        await browser.close()
    
    return jobs


def normalize_seek(job: dict) -> dict:
    """Normalize Seek job to our schema."""
    job_id = str(job.get("id", ""))
    title = job.get("title", "Unknown title")
    company = job.get("company", "Unknown Company")
    location = job.get("location", "")
    description = job.get("description", "")
    
    # URL
    job_url = f"https://www.seek.com.au/job/{job_id}"
    
    # Posted date - convert relative dates like "2d ago" to None for now
    listing_date = None  # Seek uses relative dates, hard to parse accurately
    
    content_hash = hashlib.sha256(
        f"seek|{company}|{title}|{location}|{job_url}".encode()
    ).hexdigest()
    
    return {
        "company": company,
        "title": title,
        "location": location,
        "url": job_url,
        "description": description,
        "source": "seek",
        "source_job_id": job_id,
        "posted_at": listing_date,
        "content_hash": content_hash,
        "is_active": True,
    }


def scrape_seek_senior_tech_roles(location: str = "All Australia") -> list[dict]:
    """
    Main entry point for Seek scraping.
    Searches for multiple senior tech role keywords.
    """
    # Simplified search terms that match Seek's URL structure
    search_terms = [
        "chief information officer",
        "chief technology officer",
        "chief digital officer",
        "technology director",
        "it director",
        "head of technology",
        "head of it",
        "general manager technology",
        "program director",
        "transformation director",
    ]
    
    all_jobs = []
    seen_ids = set()
    
    print(f"\n[Seek] Searching for {len(search_terms)} role types in {location}...")
    
    for term in search_terms:
        jobs = asyncio.run(scrape_seek_search(term, location, max_pages=2))
        
        # Deduplicate
        for job in jobs:
            job_id = job.get("id")
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                all_jobs.append(job)
        
        time.sleep(2.0)  # Be polite between searches
    
    print(f"[Seek] Total unique jobs found: {len(all_jobs)}")
    
    # Normalize all jobs
    return [normalize_seek(j) for j in all_jobs]


if __name__ == "__main__":
    # Test the scraper
    jobs = scrape_seek_senior_tech_roles()
    print(f"\nFound {len(jobs)} unique senior tech roles on Seek")
    
    if jobs:
        print("\nSample jobs:")
        for job in jobs[:5]:
            print(f"  - {job['title']} at {job['company']} ({job['location']})")
