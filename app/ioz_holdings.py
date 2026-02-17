# app/ioz_holdings.py
from __future__ import annotations
import csv
from pathlib import Path
from typing import List, Dict


def isin_to_asx_ticker(isin: str) -> str:
    """
    Extract ASX ticker from an Australian ISIN.
    Format: AU000000XXX# where XXX is the ticker (3-5 chars) and # is check digit.
    Examples:
        AU000000ANZ3  → ANZ
        AU000000BHP4  → BHP
        AU000000CSL8  → CSL
        AU000000WBC1  → WBC
    """
    isin = isin.strip().upper()
    if not isin.startswith("AU") or len(isin) != 12:
        return isin  # not a standard AU ISIN, return as-is

    # Strip "AU", then strip leading zeros, then strip trailing check digit
    middle = isin[2:]               # e.g. "000000ANZ3"
    core = middle.rstrip("0123456789")  # strip trailing digits → "000000ANZ"
    ticker = core.lstrip("0")           # strip leading zeros → "ANZ"

    return ticker if ticker else isin  # fallback to full ISIN if extraction fails


def parse_ioz_pcf_csv(csv_path: str | Path) -> List[Dict]:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    raw = csv_path.read_text(encoding="utf-8", errors="replace")
    lines = [ln.rstrip("\n") for ln in raw.splitlines() if ln.strip()]

    # Find the holdings table header (contains both "Security Name" and "ISIN")
    header_idx = None
    for i, line in enumerate(lines[:500]):
        low = line.lower()
        if "security name" in low and "isin" in low:
            header_idx = i
            break

    if header_idx is None:
        preview = "\n".join(lines[:30])
        raise ValueError(
            "Could not locate holdings table header (needs 'Security Name' and 'ISIN').\n"
            "First 30 non-empty lines were:\n\n" + preview
        )

    reader = csv.DictReader(lines[header_idx:])

    out: List[Dict] = []
    seen_tickers = set()

    for row in reader:
        def get_col(col: str) -> str:
            if col in row and row[col] is not None:
                return str(row[col]).strip()
            for k in row.keys():
                if k and k.strip().lower() == col.lower():
                    return str(row[k]).strip()
            return ""

        name = get_col("Security Name")
        isin = get_col("ISIN")

        if not name or not isin:
            continue

        # Extract real ASX ticker from ISIN (was incorrectly using full ISIN before)
        ticker = isin_to_asx_ticker(isin)

        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)

        # Title-case the name (CSV has it in ALL CAPS e.g. "ANZ GROUP HOLDINGS LTD")
        name_clean = name.title()

        out.append({
            "ticker": ticker,
            "name": name_clean,
            "isin": isin,
        })

    return out
