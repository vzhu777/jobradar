# app/ioz_holdings.py
from __future__ import annotations
import csv
from pathlib import Path
from typing import List, Dict

def parse_ioz_pcf_csv(csv_path: str | Path) -> List[Dict]:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    raw = csv_path.read_text(encoding="utf-8", errors="replace")
    lines = [ln.rstrip("\n") for ln in raw.splitlines() if ln.strip()]

    # Find the first line that contains BOTH "Security Name" and "ISIN"
    # (this is the holdings table header we care about)
    header_idx = None
    for i, line in enumerate(lines[:500]):
        low = line.lower()
        if "security name" in low and "isin" in low:
            header_idx = i
            break

    if header_idx is None:
        # Debug preview so you can see what the file really contains
        preview = "\n".join(lines[:30])
        raise ValueError(
            "Could not locate holdings table header (needs 'Security Name' and 'ISIN').\n"
            "First 30 non-empty lines were:\n\n" + preview
        )

    # Parse from header onward
    reader = csv.DictReader(lines[header_idx:])

    out: List[Dict] = []
    seen = set()

    for row in reader:
        # Column names can vary slightly, so we normalize:
        # try exact first, then fall back to case-insensitive match.
        def get_col(col: str) -> str:
            if col in row and row[col] is not None:
                return str(row[col]).strip()
            # case-insensitive fallback
            for k in row.keys():
                if k and k.strip().lower() == col.lower():
                    return str(row[k]).strip()
            return ""

        name = get_col("Security Name")
        isin = get_col("ISIN")

        if not name or not isin:
            continue

        # Use ISIN as unique identifier (PCF usually doesn't contain ASX ticker)
        ticker = isin.upper()

        if ticker in seen:
            continue
        seen.add(ticker)

        out.append({
            "ticker": ticker,
            "name": name,
        })

    return out
