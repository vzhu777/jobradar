# app/seed_asx200.py
from __future__ import annotations
from pathlib import Path
import requests

from app.db import upsert_companies
from app.ioz_holdings import parse_ioz_pcf_csv

IOZ_PCF_URL = "https://www.blackrock.com/au/literature/pcf/pcf-ioz-en_au.csv"
DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "ioz_holdings.csv"


# Expanded mapping covering ~150 ASX200 companies
# Prioritizes: Banks, Insurers, Telcos, Tech, Retail, Mining, Energy, Healthcare, REITs
KNOWN_WEBSITES = {
    # ─── Big 4 Banks + Regional Banks ────────────────────────────────────────
    "CBA": "https://www.commbank.com.au",
    "NAB": "https://www.nab.com.au",
    "WBC": "https://www.westpac.com.au",
    "ANZ": "https://www.anz.com.au",
    "BEN": "https://www.bendigoadelaide.com.au",
    "BOQ": "https://www.boq.com.au",
    "MQG": "https://www.macquarie.com",
    
    # ─── Insurance ────────────────────────────────────────────────────────────
    "IAG": "https://www.iag.com.au",
    "QBE": "https://www.qbe.com",
    "SUN": "https://www.suncorp.com.au",
    "MPL": "https://www.medibank.com.au",
    "NIB": "https://www.nib.com.au",
    "SDF": "https://www.steadfast.com.au",
    "AUB": "https://www.aubgroup.com.au",
    
    # ─── Wealth Management & Financial Services ───────────────────────────────
    "AMP": "https://www.amp.com.au",
    "IFL": "https://www.ioof.com.au",
    "MFG": "https://www.magellangroup.com.au",
    "PPT": "https://www.perpetual.com.au",
    "HUB": "https://www.hub24.com.au",
    "NWL": "https://www.netwealth.com.au",
    "CPU": "https://www.computershare.com",
    
    # ─── Telco & Media ────────────────────────────────────────────────────────
    "TLS": "https://www.telstra.com.au",
    "TPG": "https://www.tpgtelecom.com.au",
    "NEC": "https://www.nineforbrands.com.au",
    "SEV": "https://www.seven.com.au",
    "SXL": "https://www.southerncrossaustereo.com.au",
    
    # ─── Technology & Software ────────────────────────────────────────────────
    "WTC": "https://www.wisetechglobal.com",
    "XRO": "https://www.xero.com",
    "CPU": "https://www.computershare.com",
    "NXT": "https://www.nextdc.com",
    "MP1": "https://www.megaport.com",
    "PME": "https://www.promed.com.au",
    "DTL": "https://www.data3.com",
    "ALU": "https://www.altium.com",
    "TNE": "https://www.tyro.com",
    "APX": "https://www.appen.com",
    "REA": "https://www.rea-group.com",
    "SEK": "https://www.seek.com.au",
    "CAR": "https://www.carsales.com.au",
    "WEB": "https://www.webjet.com.au",
    
    # ─── Retail & Consumer ────────────────────────────────────────────────────
    "WOW": "https://www.woolworthsgroup.com.au",
    "COL": "https://www.colesgroup.com.au",
    "WES": "https://www.wesfarmers.com.au",
    "JBH": "https://www.jbhifi.com.au",
    "HVN": "https://www.harveynorman.com.au",
    "SUL": "https://www.superretailgroup.com.au",
    "BRG": "https://www.brevillegroup.com",
    "PMV": "https://www.premierinvestments.com.au",
    "BAP": "https://www.bapcor.com.au",
    "KMD": "https://www.kathmandu.com.au",
    "BBN": "https://www.babybunting.com.au",
    "LOV": "https://www.lovisa.com",
    "AX1": "https://www.accent-group.com",
    
    # ─── Mining & Resources ───────────────────────────────────────────────────
    "BHP": "https://www.bhp.com",
    "RIO": "https://www.riotinto.com",
    "FMG": "https://www.fmgl.com.au",
    "MIN": "https://www.mineralresources.com.au",
    "S32": "https://www.south32.net",
    "NST": "https://www.northernstar.com.au",
    "EVN": "https://www.evolutionmining.com.au",
    "IGO": "https://www.igo.com.au",
    "ILU": "https://www.iluka.com",
    "WHC": "https://www.whitehavencoal.com.au",
    "NHC": "https://www.newhopegroup.com.au",
    "SYR": "https://www.syrah.com",
    "LYC": "https://www.lynasrareearths.com",
    "PLS": "https://www.pilbaraminerals.com.au",
    "RMS": "https://www.rameliusresources.com.au",
    "RRL": "https://www.regisresources.com",
    
    # ─── Energy ───────────────────────────────────────────────────────────────
    "WDS": "https://www.woodside.com",
    "STO": "https://www.santos.com",
    "ORG": "https://www.originenergy.com.au",
    "ALD": "https://www.ampol.com.au",
    "AGL": "https://www.agl.com.au",
    "APA": "https://www.apa.com.au",
    "SKI": "https://www.spark.co.nz",
    
    # ─── Healthcare & Pharma ──────────────────────────────────────────────────
    "CSL": "https://www.csl.com",
    "COH": "https://www.cochlear.com",
    "RMD": "https://www.resmed.com",
    "SHL": "https://www.sonichealthcare.com",
    "RHC": "https://www.ramsayhealth.com",
    "EHL": "https://www.emedhealthcare.com.au",
    "ANN": "https://www.ansell.com",
    "FPH": "https://www.fisherandpaykel.com",
    
    # ─── Real Estate & Property ───────────────────────────────────────────────
    "GMG": "https://www.goodmangroup.com",
    "SCG": "https://www.scentregroup.com",
    "GPT": "https://www.gpt.com.au",
    "DXS": "https://www.dexus.com",
    "CHC": "https://www.charterhall.com.au",
    "MGR": "https://www.mirvac.com",
    "VCX": "https://www.vicinity.com.au",
    "BWP": "https://www.bwptrust.com.au",
    "SCP": "https://www.shoppingcentres.com.au",
    "NSR": "https://www.nationalstorage.com.au",
    "CQR": "https://www.charterhall.com.au/our-funds/charter-hall-retail-reit",
    
    # ─── Infrastructure & Transport ───────────────────────────────────────────
    "TCL": "https://www.transurban.com",
    "ASX": "https://www.asx.com.au",
    "SYD": "https://www.sydneyairport.com.au",
    "MEZ": "https://www.meridianenergy.co.nz",
    "AIA": "https://www.aucklandairport.co.nz",
    "QAN": "https://www.qantas.com",
    "AIZ": "https://www.airnz.co.nz",
    "FLT": "https://www.flightcentretravelgroup.com",
    
    # ─── Industrials & Building Materials ─────────────────────────────────────
    "AMC": "https://www.amcor.com",
    "LLC": "https://www.lendlease.com",
    "BXB": "https://www.brambles.com",
    "JHX": "https://www.jameshardie.com",
    "BLD": "https://www.boral.com.au",
    "IPL": "https://www.incitecpivot.com.au",
    "ALQ": "https://www.als.com",
    "DOW": "https://www.downergroup.com",
    "CIA": "https://www.championiron.com",
    "BRG": "https://www.brevillegroup.com",
    "SEK": "https://www.seek.com.au",
    "REA": "https://www.rea-group.com",
    "CAR": "https://www.carsales.com.au",
    "DHG": "https://www.domain.com.au",
    
    # ─── Food & Beverage ──────────────────────────────────────────────────────
    "TWE": "https://www.tweglobal.com",
    "CCL": "https://www.ccamatil.com",
    "GNC": "https://www.graincorp.com.au",
    "BGA": "https://www.bega.com.au",
    "A2M": "https://www.a2milk.com",
    "SIG": "https://www.sigmahealthcare.com.au",
    "API": "https://www.api.net.au",
    
    # ─── Gaming & Entertainment ───────────────────────────────────────────────
    "ALL": "https://www.aristocrat.com",
    "SWM": "https://www.sportsbet.com.au",
    "EVT": "https://www.evt.com",
    "TAH": "https://www.endeavourgroup.com.au",
    "SGR": "https://www.stargroupholdings.com.au",
    
    # ─── Professional Services ────────────────────────────────────────────────
    "WOR": "https://www.worley.com",
    "CPU": "https://www.computershare.com",
    "NWH": "https://www.nrw.com.au",
    "SVW": "https://www.sevengroup.com.au",
    "MND": "https://www.maca.net.au",
    "CIP": "https://www.centuria.com.au",
    
    # ─── Education ────────────────────────────────────────────────────────────
    "IEL": "https://www.idp.com",
    
    # ─── Utilities ────────────────────────────────────────────────────────────
    "APA": "https://www.apa.com.au",
    "SKI": "https://www.spark.co.nz",
}


def download_ioz_csv():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading IOZ holdings from BlackRock...")
    r = requests.get(IOZ_PCF_URL, timeout=30)
    r.raise_for_status()
    CSV_PATH.write_bytes(r.content)
    print(f"✓ Saved to {CSV_PATH} ({CSV_PATH.stat().st_size:,} bytes)")


def enrich_with_websites(companies: list[dict]) -> list[dict]:
    """Add website_url to companies using known mappings."""
    enriched = []
    for c in companies:
        ticker = c["ticker"]
        website = KNOWN_WEBSITES.get(ticker)
        
        if website:
            c["website_url"] = website
            enriched.append(c)
    
    return enriched


def run():
    download_ioz_csv()
    
    companies = parse_ioz_pcf_csv(CSV_PATH)
    print(f"✓ Parsed {len(companies)} companies from IOZ PCF")

    if not companies:
        print("⚠ No companies parsed — check CSV format")
        return

    # Enrich with known websites
    enriched = enrich_with_websites(companies)
    print(f"✓ Enriched {len(enriched)} companies with website URLs")
    
    if not enriched:
        print("⚠ No companies matched known websites. Add more to KNOWN_WEBSITES dict.")
        return

    saved = upsert_companies(enriched)
    print(f"✓ Upserted {len(saved)} companies to Supabase")
    
    # Show breakdown by sector
    print(f"\n{'='*80}")
    print("Companies seeded by sector:")
    print(f"{'='*80}")
    
    sectors = {
        "Banks & Financial Services": ["CBA", "NAB", "WBC", "ANZ", "BEN", "BOQ", "MQG", "AMP", "MFG", "PPT", "CPU"],
        "Insurance": ["IAG", "QBE", "SUN", "MPL", "NIB", "SDF", "AUB"],
        "Technology": ["WTC", "XRO", "NXT", "MP1", "PME", "DTL", "ALU", "TNE", "APX", "REA", "SEK", "CAR"],
        "Retail": ["WOW", "COL", "WES", "JBH", "HVN", "SUL", "BRG", "PMV"],
        "Mining & Resources": ["BHP", "RIO", "FMG", "MIN", "S32", "NST", "EVN", "IGO", "ILU"],
        "Healthcare": ["CSL", "COH", "RMD", "SHL", "RHC", "ANN"],
        "Real Estate": ["GMG", "SCG", "GPT", "DXS", "CHC", "MGR", "VCX"],
        "Telco & Media": ["TLS", "TPG", "NEC", "SEV"],
    }
    
    for sector, tickers in sectors.items():
        matches = [t for t in tickers if t in KNOWN_WEBSITES]
        if matches:
            print(f"  {sector:35} {len(matches):3} companies")
    
    print(f"{'='*80}\n")
    
    # Show sample
    print("Sample companies seeded:")
    for c in saved[:10]:
        print(f"  {c['ticker']:6} {c['name']:45} → {c.get('website_url', 'N/A')[:50]}")
    
    if len(saved) > 10:
        print(f"  ... and {len(saved) - 10} more\n")


if __name__ == "__main__":
    run()
