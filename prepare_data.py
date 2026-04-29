#!/Library/Developer/CommandLineTools/usr/bin/python3.9
"""
prepare_data.py — One-time data prep for First Country Records tool.

Generates:
  reviewers.json  — gitignored, local only (contains reviewer emails)
  taxonomy.json   — committed, public eBird taxonomy for autocomplete

Usage:
    python3 prepare_data.py
"""

import csv
import json
import re
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    sys.exit("pandas required: pip install pandas openpyxl")

HERE = Path(__file__).parent


# ── Taxonomy ──────────────────────────────────────────────────────────────────

def build_taxonomy():
    csv_candidates = sorted(HERE.glob("eBird_taxonomy_*.csv"))
    if not csv_candidates:
        sys.exit("No eBird_taxonomy_*.csv found.")
    csv_path = csv_candidates[-1]
    print(f"Loading taxonomy from {csv_path.name} …")
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append([
                row["SPECIES_CODE"],
                row["PRIMARY_COM_NAME"],
                row["SCI_NAME"],
                row["CATEGORY"],
            ])
    out = HERE / "taxonomy.json"
    out.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"taxonomy.json written: {len(rows):,} entries ({out.stat().st_size // 1024} KB)")


# ── Reviewers ─────────────────────────────────────────────────────────────────

def extract_country_from_name(name):
    """Try to derive a 2-letter country code from a reviewer assignment name."""
    if not isinstance(name, str):
        return None
    # Pattern: "eBird-XX-..." or "eBird-XX " at start
    m = re.match(r"eBird-([A-Z]{2})[-\s]", name)
    if m:
        return m.group(1)
    # Pattern: "XX-..." at start (e.g., "US-CA-Farallon Islands")
    m = re.match(r"^([A-Z]{2})-", name)
    if m:
        return m.group(1)
    return None


def extract_subnational_from_named_checklist(region_code):
    """
    For named CHECKLIST region codes like 'EC-W--Galápagos-Santiago',
    extract the subnational1 code (e.g., 'EC-W').
    Returns None for CL#### codes.
    """
    if not isinstance(region_code, str):
        return None
    if re.match(r"^CL\d+$", region_code, re.IGNORECASE):
        return None
    # Try to extract XX-YY prefix
    m = re.match(r"^([A-Z]{2}-[A-Z0-9]+)--", region_code)
    if m:
        return m.group(1)
    return None


def build_reviewers():
    xlsx_candidates = [f for f in HERE.glob("eBird Reviewers*.xlsx")
                       if not f.name.startswith("~")]
    if not xlsx_candidates:
        sys.exit("No 'eBird Reviewers*.xlsx' found.")
    xlsx_path = xlsx_candidates[0]
    print(f"Loading reviewers from {xlsx_path.name} …")

    df = pd.read_excel(xlsx_path, skiprows=2, header=0)
    df = df.dropna(subset=["email"])

    by_country     = {}  # country_code → list of {first_name, name, email}
    by_subnational = {}  # subnational_code → list

    def add_to(d, key, reviewer):
        if not key or not isinstance(key, str):
            return
        key = key.strip()
        if not key:
            return
        if key not in d:
            d[key] = []
        # Deduplicate by email within this key
        if not any(r["email"] == reviewer["email"] for r in d[key]):
            d[key].append(reviewer)

    skipped_cl = 0
    for _, row in df.iterrows():
        rv = {
            "first_name": str(row["first_name"]).strip() if pd.notna(row["first_name"]) else "",
            "name":       str(row["reviewer"]).strip()   if pd.notna(row["reviewer"])   else "",
            "email":      str(row["email"]).strip(),
        }
        rc    = str(row["region_code"]).strip() if pd.notna(row["region_code"]) else ""
        rname = str(row["name"]).strip()        if pd.notna(row["name"])        else ""
        cls   = str(row["restriction_class"]).strip()

        if cls == "COUNTRY":
            add_to(by_country, rc, rv)

        elif cls == "STATE":
            add_to(by_subnational, rc, rv)

        elif cls == "COUNTY":
            add_to(by_subnational, rc, rv)

        elif cls == "COUNTY_LIST":
            for code in rc.split(","):
                add_to(by_subnational, code.strip(), rv)

        elif cls == "CHECKLIST":
            # Named polygon (e.g., EC-W--Galápagos-Santiago)
            sub = extract_subnational_from_named_checklist(rc)
            if sub:
                add_to(by_subnational, sub, rv)
            else:
                # CL#### — fall back to country level
                country = extract_country_from_name(rname)
                if country:
                    add_to(by_country, country, rv)
                else:
                    skipped_cl += 1

    total_country = sum(len(v) for v in by_country.values())
    total_sub     = sum(len(v) for v in by_subnational.values())
    print(f"  by_country:     {len(by_country)} keys, {total_country} reviewer slots")
    print(f"  by_subnational: {len(by_subnational)} keys, {total_sub} reviewer slots")
    if skipped_cl:
        print(f"  Skipped {skipped_cl} CL#### entries (no country derivable)")

    out = HERE / "reviewers.json"
    out.write_text(
        json.dumps({"by_country": by_country, "by_subnational": by_subnational},
                   ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"reviewers.json written ({out.stat().st_size // 1024} KB) — gitignored, local only")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    build_taxonomy()
    print()
    build_reviewers()
    print("\nDone.")
