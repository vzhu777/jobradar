import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_KEY")

print("Connecting to Supabase...")

if not url or not key:
    print("Missing env variables")
else:
    sb = create_client(url, key)
    print("Connected OK")

from app.db import fetch_companies

print("Fetching companies from Supabase...")

companies = fetch_companies()

print(f"Found {len(companies)} companies:\n")

for c in companies:
    print(f"- {c.get('ticker','')} {c['name']} | website={c.get('website_url')}")
