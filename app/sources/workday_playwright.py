import asyncio
import json
import re
from playwright.async_api import async_playwright


def _extract_base_site(board_url: str) -> tuple[str, str]:
    m = re.match(r"^(https?://[^/]+)/([^/?#]+)", board_url.strip())
    if not m:
        raise ValueError(f"Invalid Workday board_url: {board_url}")
    return m.group(1), m.group(2)


async def fetch_workday_jobs_pw(
    board_url: str,
    search_text: str = "",
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    base, site = _extract_base_site(board_url)
    tenant = base.split("://", 1)[1].split(".", 1)[0]
    api_url = f"{base}/wday/cxs/{tenant}/{site}/jobs"

    warm_url = f"{board_url}?q={search_text}" if search_text else board_url

    payload = {
        "appliedFacets": {},
        "limit": limit,
        "offset": offset,
        "searchText": search_text,
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(warm_url, wait_until="networkidle")

        # Make sure timezone cookie is set (Melbourne = -660)
        await page.evaluate("document.cookie = 'timezoneOffset=-660; path=/'")

        cookies = await page.context.cookies()
        csrf = None
        for c in cookies:
            if c.get("name") == "CALYPSO_CSRF_TOKEN":
                csrf = c.get("value")
                break

        if not csrf:
            await browser.close()
            raise RuntimeError("Missing CALYPSO_CSRF_TOKEN cookie.")

        # Run the POST inside the page so Cloudflare/Workday cookies are included.
        result = await page.evaluate(
            """async ({ apiUrl, payload, csrf }) => {
              const res = await fetch(apiUrl, {
                method: "POST",
                headers: {
                  "accept": "application/json",
                  "content-type": "application/json",
                  "x-requested-with": "XMLHttpRequest",
                  "x-calypso-csrf-token": csrf
                },
                body: JSON.stringify(payload),
                credentials: "include",
                mode: "cors"
              });
              const text = await res.text();
              return { status: res.status, text };
            }""",
            {"apiUrl": api_url, "payload": payload, "csrf": csrf},
        )

        await browser.close()

    status = result["status"]
    text = result["text"]

    print(f"WORKDAY page offset={offset} limit={limit} → STATUS {status}")
    print("TEXT HEAD:", text[:120])

    if status != 200:
        raise RuntimeError(f"Workday request failed: {status} {text[:300]}")

    data = json.loads(text)
    return data.get("jobPostings") or data.get("items") or []

def fetch_workday_jobs_sync(board_url: str, search_text: str = "", limit: int = 20, offset: int = 0) -> list[dict]:
    return asyncio.run(fetch_workday_jobs_pw(board_url, search_text, limit, offset))
