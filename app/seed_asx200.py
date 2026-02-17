# app/seed_asx200.py
from __future__ import annotations
from pathlib import Path
import requests

from app.db import upsert_companies
from app.ioz_holdings import parse_ioz_pcf_csv

IOZ_PCF_URL = "https://www.blackrock.com/au/literature/pcf/pcf-ioz-en_au.csv"
DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "ioz_holdings.csv"

def download_ioz_csv():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    r = requests.get(IOZ_PCF_URL, timeout=30)
    r.raise_for_status()
    CSV_PATH.write_bytes(r.content)

def run():
    download_ioz_csv()
    print("Saved CSV to:", CSV_PATH)
    print("File size:", CSV_PATH.stat().st_size, "bytes")
    print("First 20 lines:\n", "\n".join(CSV_PATH.read_text(encoding="utf-8", errors="replace").splitlines()[:20]))

        
    companies = parse_ioz_pcf_csv(CSV_PATH)

    print(f"Parsed {len(companies)} companies from IOZ PCF.")

    if not companies:
        print("No companies parsed — check data/ioz_holdings.csv content.")
        return

    saved = upsert_companies(companies)
    print(f"Upserted {len(saved)} rows into companies table.")

if __name__ == "__main__":
    run()
