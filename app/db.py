import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def fetch_companies():
    resp = sb.table("companies").select("*").execute()
    return resp.data

def upsert_jobs(rows):
    resp = sb.table("jobs").upsert(rows, on_conflict="source,source_job_id").execute()
    return resp.data

def upsert_companies(rows: list[dict]):
    """
    Upserts companies into Supabase.
    Expects fields like:
      ticker, name, homepage_url, source
    """

    if not rows:
        return []

    # We use ticker as unique identifier
    resp = (
        sb.table("companies")
        .upsert(rows, on_conflict="ticker")
        .execute()
    )

    return resp.data or []
