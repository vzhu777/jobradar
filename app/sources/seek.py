# app/sources/seek_stealthy.py
"""
Stealthy Seek scraper using Playwright with anti-detection measures.
Focus on quality senior tech roles only.
"""
import asyncio
import random
import hashlib
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# Senior tech role search terms (including APAC and Global)
SENIOR_TECH_SEARCHES = [
    "Chief Information Officer",
    "Chief Technology Officer", 
    "Chief Digital Officer",
    "Technology Director",
    "IT Director",
    "Head of Technology",
    "Head of IT",
    "General Manager Technology",
    "APAC Technology Director",
    "Global Technology Director",

    # Mandarin / China market roles
    "Mandarin speaking",
    "Mandarin required",
    "China market",
    "Greater China",
    "China business development",
    "APAC business development",
    "APAC growth",
    "APAC expansion",
    "China strategy",
    "Head of China",
]

AUSTRALIAN_LOCATIONS = [
    "All Australia",
    "Melbourne VIC",
    "Sydney NSW",
    "Brisbane QLD",
]


async def scrape_seek_search(page, search_term: str, location: str = "All Australia", max_pages: int = 2):
    """
    Scrape a single Seek search with stealth.
    """
    jobs = []
    
    # Build Seek URL
    base_url = "https://www.seek.com.au"
    keywords = search_term.replace(" ", "-").lower()
    location_param = location.replace(" ", "-").lower()
    
    for page_num in range(1, max_pages + 1):
        try:
            url = f"{base_url}/{keywords}-jobs/in-{location_param}?page={page_num}"
            print(f"    [Seek] Loading: {url}")
            
            # Navigate with realistic timing
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # Wait for job cards to load
            await page.wait_for_selector('[data-automation="normalJob"]', timeout=10000)
            
            # Random human-like delay
            await asyncio.sleep(random.uniform(2.0, 4.0))
            
            # Extract job cards
            job_cards = await page.query_selector_all('[data-automation="normalJob"]')
            print(f"    [Seek] Found {len(job_cards)} job cards on page {page_num}")
            
            for card in job_cards:
                try:
                    # Extract title
                    title_elem = await card.query_selector('a[data-automation="jobTitle"]')
                    title = await title_elem.inner_text() if title_elem else "Unknown"
                    
                    # Extract company
                    company_elem = await card.query_selector('a[data-automation="jobCompany"]')
                    company = await company_elem.inner_text() if company_elem else "Unknown Company"
                    
                    # Extract location
                    location_elem = await card.query_selector('a[data-automation="jobLocation"]')
                    job_location = await location_elem.inner_text() if location_elem else ""
                    
                    # Extract salary (if present)
                    salary_elem = await card.query_selector('[data-automation="jobSalary"]')
                    salary = await salary_elem.inner_text() if salary_elem else None
                    
                    # Extract job URL
                    job_url = await title_elem.get_attribute('href') if title_elem else None
                    if job_url and not job_url.startswith('http'):
                        job_url = base_url + job_url
                    
                    # Extract job ID from URL
                    job_id = job_url.split('/')[-1].split('?')[0] if job_url else None
                    
                    jobs.append({
                        'id': job_id,
                        'title': title.strip(),
                        'company': company.strip(),
                        'location': job_location.strip(),
                        'salary': salary.strip() if salary else None,
                        'url': job_url,
                        'search_term': search_term,
                    })
                    
                except Exception as e:
                    print(f"      [Seek] Error parsing job card: {e}")
                    continue
            
            # Human-like delay between pages
            if page_num < max_pages:
                await asyncio.sleep(random.uniform(3.0, 6.0))
        
        except PlaywrightTimeout:
            print(f"    [Seek] Timeout loading page {page_num}")
            break
        except Exception as e:
            print(f"    [Seek] Error on page {page_num}: {e}")
            break
    
    return jobs


def normalize_seek(job: dict) -> dict:
    """Normalize Seek job to our schema."""
    job_id = str(job.get('id', ''))
    title = job.get('title', 'Unknown title')
    company = job.get('company', 'Unknown Company')
    location = job.get('location', '')
    salary = job.get('salary', '')
    job_url = job.get('url', '')
    
    description = f"Seek job posting for {title}.\n"
    if salary:
        description += f"Salary: {salary}\n"
    description += f"\nView full details at the link."
    
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
        "posted_at": None,
        "content_hash": content_hash,
        "is_active": True,
    }


async def scrape_seek_senior_tech_async():
    """
    Main async function to scrape Seek for senior tech roles.
    """
    all_jobs = []
    seen_ids = set()
    
    async with async_playwright() as p:
        # Launch browser with stealth settings
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )
        
        # Create context with realistic settings
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-AU',
            timezone_id='Australia/Melbourne',
        )
        
        # Add stealth scripts
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        page = await context.new_page()
        
        print(f"\n[Seek] Searching for {len(SENIOR_TECH_SEARCHES)} senior tech roles...")
        
        for search_term in SENIOR_TECH_SEARCHES:
            print(f"\n  [Seek] Search: {search_term}")
            
            # Try first with "All Australia"
            jobs = await scrape_seek_search(page, search_term, "All Australia", max_pages=2)
            
            # Deduplicate
            new_count = 0
            for job in jobs:
                job_id = job.get('id')
                if job_id and job_id not in seen_ids:
                    seen_ids.add(job_id)
                    all_jobs.append(job)
                    new_count += 1
            
            print(f"  [Seek] Found {new_count} new jobs for '{search_term}'")
            
            # Long delay between searches to avoid detection
            await asyncio.sleep(random.uniform(8.0, 15.0))
        
        await browser.close()
    
    print(f"\n[Seek] Total unique jobs found: {len(all_jobs)}")
    return [normalize_seek(j) for j in all_jobs]


def scrape_seek_senior_tech_roles():
    """
    Synchronous wrapper for the async scraper.
    """
    return asyncio.run(scrape_seek_senior_tech_async())


if __name__ == "__main__":
    # Test the scraper
    jobs = scrape_seek_senior_tech_roles()
    print(f"\nFound {len(jobs)} unique senior tech roles on Seek")
    
    if jobs:
        print("\nSample jobs:")
        for job in jobs[:10]:
            salary_info = f" - {job.get('salary', 'No salary')}" if job.get('salary') else ""
            print(f"  - {job['title']} at {job['company']} ({job['location']}){salary_info}")
