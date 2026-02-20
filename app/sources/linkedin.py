# app/sources/linkedin.py
"""
LinkedIn RSS feed scraper - EXPANDED coverage
"""
from __future__ import annotations
import hashlib
import time
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch_linkedin_jobs(keywords: str, location: str = "Australia", start: int = 0) -> list[dict]:
    """Fetch jobs from LinkedIn's public job search API."""
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    params = {
        "keywords": keywords,
        "location": location,
        "start": start,
        "geoId": "101452733",  # Australia geo ID
    }
    
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    job_cards = soup.find_all("div", class_="base-card")
    
    jobs = []
    for card in job_cards:
        try:
            job_id = card.get("data-entity-urn", "").split(":")[-1]
            if not job_id:
                continue
            
            title_elem = card.find("h3", class_="base-search-card__title")
            title = title_elem.get_text(strip=True) if title_elem else "Unknown"
            
            company_elem = card.find("h4", class_="base-search-card__subtitle")
            company = company_elem.get_text(strip=True) if company_elem else "Unknown Company"
            
            location_elem = card.find("span", class_="job-search-card__location")
            job_location = location_elem.get_text(strip=True) if location_elem else ""
            
            # Filter out non-Australian jobs
            if job_location:
                loc_lower = job_location.lower()
                au_indicators = ["australia", "sydney", "melbourne", "brisbane", "perth", "adelaide", "canberra", "nsw", "vic", "qld", "wa", "sa", "act", "tas"]
                if not any(indicator in loc_lower for indicator in au_indicators):
                    continue
            
            time_elem = card.find("time")
            posted_date = time_elem.get("datetime") if time_elem else None
            
            link_elem = card.find("a", class_="base-card__full-link")
            job_url = link_elem.get("href") if link_elem else f"https://www.linkedin.com/jobs/view/{job_id}"
            
            jobs.append({
                "id": job_id,
                "title": title,
                "company": company,
                "location": job_location,
                "posted_date": posted_date,
                "url": job_url.split("?")[0],
            })
        
        except Exception as e:
            print(f"  [LinkedIn] Error parsing job card: {e}")
            continue
    
    return jobs


def normalize_linkedin(job: dict) -> dict:
    """Normalize LinkedIn job to our schema."""
    job_id = str(job.get("id", ""))
    title = job.get("title", "Unknown title")
    company = job.get("company", "Unknown Company")
    location = job.get("location", "")
    job_url = job.get("url", "")
    posted_date = job.get("posted_date")
    
    description = f"LinkedIn job posting. View full details at the link.\n\nPosted: {posted_date or 'Recently'}"
    
    content_hash = hashlib.sha256(
        f"linkedin|{company}|{title}|{location}|{job_url}".encode()
    ).hexdigest()
    
    return {
        "company": company,
        "title": title,
        "location": location,
        "url": job_url,
        "description": description,
        "source": "linkedin",
        "source_job_id": job_id,
        "posted_at": posted_date,
        "content_hash": content_hash,
        "is_active": True,
    }


def scrape_linkedin_senior_tech_roles(location: str = "Australia") -> list[dict]:
    """
    EXPANDED LinkedIn scraping - more search terms and pages.
    """
    # EXPANDED search terms (20 terms instead of 11)
    search_terms = [
        # C-Level
        "Chief Information Officer",
        "Chief Technology Officer",
        "Chief Digital Officer",
        "Chief Innovation Officer",
        "CIO",
        "CTO",
        "CDO",
        
        # Director Level
        "Technology Director",
        "IT Director",
        "Digital Director",
        "Engineering Director",
        
        # VP & Head Level
        "VP Technology",
        "VP Engineering",
        "Head of Technology",
        "Head of IT",
        "Head of Engineering",
        "Head of Digital",
        
        # GM & Senior Manager (NEW)
        "General Manager Technology",
        "Senior Manager Technology",
        "Principal Engineer",
        "Principal Architect",
    ]
    
    all_jobs = []
    seen_ids = set()
    
    print(f"\n[LinkedIn] EXPANDED search: {len(search_terms)} role types, 4 pages each...")
    
    for term in search_terms:
        print(f"  [LinkedIn] Searching: {term}")
        
        # Fetch 4 pages instead of 2 (100 jobs per term instead of 50)
        for start in [0, 25, 50, 75]:
            try:
                jobs = fetch_linkedin_jobs(term, location="Australia", start=start)
                
                if not jobs:
                    if start == 0:
                        print(f"    No jobs found")
                    break
                
                new_count = 0
                for job in jobs:
                    job_id = job.get("id")
                    if job_id and job_id not in seen_ids:
                        seen_ids.add(job_id)
                        all_jobs.append(job)
                        new_count += 1
                
                print(f"    Found {new_count} new AU jobs (page {start//25 + 1})")
                
                if len(jobs) < 25:
                    break
                
                time.sleep(2.0)
            
            except Exception as e:
                print(f"  [LinkedIn] Error fetching {term}: {e}")
                break
        
        time.sleep(2.0)
    
    print(f"[LinkedIn] Total unique Australia jobs found: {len(all_jobs)}")
    
    return [normalize_linkedin(j) for j in all_jobs]


if __name__ == "__main__":
    jobs = scrape_linkedin_senior_tech_roles()
    print(f"\nFound {len(jobs)} unique senior tech roles on LinkedIn (Australia)")
    
    if jobs:
        print("\nSample jobs:")
        for job in jobs[:10]:
            print(f"  - {job['title']} at {job['company']} ({job['location']})")
