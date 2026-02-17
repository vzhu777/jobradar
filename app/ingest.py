from app.db import fetch_companies, upsert_jobs
from app.sources.workday_playwright import fetch_workday_jobs_sync
from app.sources.workday import normalize_workday
from app.emailer import send_email



def run():
    companies = fetch_companies()

    for c in companies:
        ats_type = (c.get("ats_type") or "").lower()
        board_url = c.get("ats_board_url")
        name = c["name"]

        if ats_type != "workday" or not board_url:
            continue

        print(f"\nIngesting Workday jobs for {name} ...")

        raw = []
        offset = 0
        page_size = 20
        seen_ids = set()

        while True:
            batch = fetch_workday_jobs_sync(
                board_url,
                search_text="",   # empty = all jobs
                limit=page_size,
                offset=offset,
            )

            if not batch:
                print("No more jobs returned → stopping pagination")
                break

            # De-dupe to avoid infinite loops if the same page repeats
            new_items = []
            for j in batch:
                jid = j.get("externalPath") or j.get("id") or str(j)
                if jid not in seen_ids:
                    seen_ids.add(jid)
                    new_items.append(j)

            if not new_items:
                print("Pagination repeating same jobs → stopping")
                break

            raw.extend(new_items)
            print(f"Collected so far: {len(raw)} jobs")

            # If we got less than a full page, we're done
            if len(batch) < page_size:
                print("Last page reached")
                break

            offset += page_size

            # Hard safety stop (prevents runaway loops)
            if offset > 2000:
                print("Safety stop triggered (offset > 2000)")
                break

        print(f"Fetched {len(raw)} postings (paginated)")

        normalized = [normalize_workday(name, board_url, j) for j in raw]

        # attach company_id if available
        for n in normalized:
            n["company_id"] = c["id"]


        saved = upsert_jobs(normalized)
        print(f"Upserted {len(saved)} rows into jobs table")

        # 1) detect new jobs
        new_jobs = [j for j in saved if j.get("created_at") == j.get("updated_at")]
        print(f"New jobs this run: {len(new_jobs)}")

        # 2) filter to relevant AU exec/tech roles
        ROLE_KEYWORDS = [
            "chief", "cio", "cto", "cdo",
            "head", "director", "general manager", "gm",
            "transformation", "technology", "digital", "data", "information"
        ]

        AU_LOCATIONS = [
            "australia", "melbourne", "sydney", "brisbane",
            "perth", "adelaide", "canberra", "remote"
        ]

        def is_relevant(j):
            title = (j.get("title") or "").lower()
            loc = (j.get("location") or "").lower()
            return any(k in title for k in ROLE_KEYWORDS) and any(a in loc for a in AU_LOCATIONS)

        new_jobs_relevant = [j for j in new_jobs if is_relevant(j)]
        print(f"Relevant new jobs this run: {len(new_jobs_relevant)}")

        # 3) email only if relevant new jobs exist
        if new_jobs_relevant:
            items = "\n".join(
                f"<li><a href='{j['url']}'>{j['company']} — {j['title']}</a> ({j.get('location','')})</li>"
                for j in new_jobs_relevant
            )
            html = f"""
            <h3>New relevant roles found: {len(new_jobs_relevant)}</h3>
            <ul>{items}</ul>
            """
            send_email(
                subject=f"JobRadar — {len(new_jobs_relevant)} new relevant roles",
                html_body=html
            )
            print("Email sent.")
        else:
            print("No relevant new jobs; no email sent.")

