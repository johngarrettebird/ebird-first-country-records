#!/usr/bin/env python3.9
"""
process_ebd.py — Build first_records.json from the eBird Basic Dataset.

Usage:
    python3.9 process_ebd.py /path/to/ebd_relMMM-YYYY.txt
    python3.9 process_ebd.py /path/to/ebd_relMMM-YYYY.txt.gz   # gzip also works

Output:
    first_records.json  (same directory as this script)

Runtime: ~10-20 min on a laptop for the full EBD. Re-run when you get a new EBD snapshot.
"""

import sys
import json
from pathlib import Path
from datetime import date

try:
    import polars as pl
except ImportError:
    sys.exit("Install polars first:  python3.9 -m pip install polars")


def main():
    if len(sys.argv) < 2:
        sys.exit("Usage: python3.9 process_ebd.py /path/to/ebd_relMMM-YYYY.txt[.gz]")

    ebd_path = Path(sys.argv[1])
    if not ebd_path.exists():
        sys.exit(f"File not found: {ebd_path}")

    out_path = Path(__file__).parent / "first_records.json"

    print(f"Scanning {ebd_path.name} ...")
    print("(This may take 10-20 minutes for the full EBD — progress is not streamed)")

    # Lazy scan — polars reads only what's needed, handles gzip transparently
    lf = pl.scan_csv(
        str(ebd_path),
        separator="\t",
        has_header=True,
        infer_schema_length=0,          # read all columns as strings
        truncate_ragged_lines=True,
        ignore_errors=True,
    )

    needed = [
        "CATEGORY",
        "COMMON NAME",
        "SCIENTIFIC NAME",
        "TAXON CONCEPT ID",
        "COUNTRY",
        "COUNTRY CODE",
        "OBSERVATION DATE",
        "SAMPLING EVENT IDENTIFIER",
        "APPROVED",
    ]

    print("Finding first approved full-species record per country …")

    first = (
        lf
        .select(needed)
        .filter(pl.col("CATEGORY") == "species")
        .filter(pl.col("APPROVED") == "1")
        .group_by(["COUNTRY CODE", "TAXON CONCEPT ID"])
        .agg([
            pl.col("COUNTRY").first().alias("country"),
            pl.col("COMMON NAME").first().alias("common_name"),
            pl.col("SCIENTIFIC NAME").first().alias("scientific_name"),
            pl.col("OBSERVATION DATE").min().alias("first_date"),
            # Get the checklist ID from whichever row has the earliest date
            pl.col("SAMPLING EVENT IDENTIFIER")
              .sort_by("OBSERVATION DATE")
              .first()
              .alias("checklist_id"),
        ])
        .sort("first_date", descending=True)
        .collect()
    )

    print(f"Found {len(first):,} first country records across "
          f"{first['COUNTRY CODE'].n_unique():,} countries.")

    records = [
        {
            "cc":  row["COUNTRY CODE"],
            "cn":  row["country"],
            "sn":  row["common_name"],
            "sc":  row["scientific_name"],
            "tc":  row["TAXON CONCEPT ID"],
            "fd":  row["first_date"],
            "cl":  row["checklist_id"],
        }
        for row in first.iter_rows(named=True)
    ]

    output = {
        "generated": str(date.today()),
        "source": ebd_path.name,
        "total": len(records),
        "records": records,
    }

    out_path.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")
    print(f"Written to {out_path}")
    print("Next: run update_monitor.py to track new first records going forward.")


if __name__ == "__main__":
    main()
