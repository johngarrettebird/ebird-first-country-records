#!/Library/Developer/CommandLineTools/usr/bin/python3.9
"""
update_monitor.py — Detect new first country records via the eBird API.

Compares each country's current eBird species list against a stored snapshot.
Any species that appears in a country for the first time is logged as a
potential new first country record.

Usage:
    python3 update_monitor.py              # run update, save results
    python3 update_monitor.py --status     # show counts from last run

Output files (in same directory as this script):
    species_snapshot.json   — current species-per-country baseline
    new_firsts.json         — accumulated log of new detections
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import date, datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

HERE        = Path(__file__).parent
_config     = json.loads((HERE / "config.json").read_text()) if (HERE / "config.json").exists() else {}

API_KEY       = _config.get("ebird_api_key")     or os.environ.get("EBIRD_API_KEY", "YOUR_API_KEY_HERE")
SLACK_WEBHOOK = _config.get("slack_webhook_url") or os.environ.get("SLACK_WEBHOOK_URL", "")
BASE_URL      = "https://api.ebird.org/v2"
DELAY         = 0.4   # seconds between API calls — be a good citizen

# ── Paths ─────────────────────────────────────────────────────────────────────

SNAPSHOT_PATH = HERE / "species_snapshot.json"
NEW_FIRSTS    = HERE / "new_firsts.json"
FIRST_RECORDS = HERE / "first_records.json"


# ── API helpers ───────────────────────────────────────────────────────────────

def api_get(path, params=""):
    url = f"{BASE_URL}/{path}{'?' + params if params else ''}"
    req = urllib.request.Request(url, headers={"X-eBirdApiToken": API_KEY})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []      # country has no records
        raise


def fetch_taxonomy():
    """Return dict of speciesCode → {commonName, sciName}."""
    print("Fetching eBird taxonomy …")
    taxa = api_get("ref/taxonomy/ebird", "fmt=json&cat=species")
    time.sleep(DELAY)
    return {
        t["speciesCode"]: {"sn": t["comName"], "sc": t["sciName"]}
        for t in taxa
    }


def fetch_countries():
    """Return list of {code, name} for every country in eBird."""
    countries = api_get("ref/region/list/country/world")
    time.sleep(DELAY)
    return countries


def fetch_spplist(country_code):
    """Return list of species codes recorded in this country."""
    result = api_get(f"product/spplist/{country_code}")
    time.sleep(DELAY)
    return result if isinstance(result, list) else []


def fetch_checklist_id(country_code, species_code):
    """Return the most recent checklist subId for this species in this country, or None."""
    result = api_get(f"data/obs/{country_code}/recent/{species_code}", "back=30")
    time.sleep(DELAY)
    if result and isinstance(result, list):
        return result[0].get("subId")
    return None


# ── Slack ─────────────────────────────────────────────────────────────────────

def post_slack(new_this_run):
    if not SLACK_WEBHOOK or not new_this_run:
        return
    lines = [f"*eBird First Country Records — {len(new_this_run)} new detection{'s' if len(new_this_run) > 1 else ''}*"]
    for r in new_this_run:
        cl_part = f"  <{r['cl_url']}|checklist>" if r.get("cl_url") else ""
        lines.append(f"• <{r['ebird_url']}|{r['common_name']}> — {r['country']}{cl_part}")
    payload = json.dumps({"text": "\n".join(lines)}).encode()
    req = urllib.request.Request(SLACK_WEBHOOK, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
        print("Slack notification sent.")
    except Exception as e:
        print(f"Slack notification failed: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def load_json(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None



def write_first_records(detections, snapshot, today):
    """Write first_records.json from accumulated monitor detections."""
    monitoring_since = None
    firsts_data = load_json(NEW_FIRSTS)
    if firsts_data:
        # Infer monitoring_since from oldest detection, or snapshot date
        dates = [d["detected"] for d in detections if d.get("detected")]
        monitoring_since = min(dates) if dates else (
            load_json(SNAPSHOT_PATH) or {}).get("updated", today)

    records = [
        {
            "cc":  d["country_code"],
            "cn":  d["country"],
            "sn":  d["common_name"],
            "sc":  d["scientific_name"],
            "tc":  d["species_code"],
            "fd":  d["detected"],
            "cl":  None,
            "url": d.get("ebird_url"),
        }
        for d in detections
    ]

    output = {
        "generated":        today,
        "source":           "eBird API monitor",
        "monitoring_since": monitoring_since or today,
        "countries_tracked": len(snapshot),
        "total":            len(records),
        "records":          records,
    }
    FIRST_RECORDS.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")
    print(f"first_records.json written: {len(records)} detection(s).")


def run_update():
    today = str(date.today())

    # Load existing snapshot (species codes per country from prior API runs)
    snapshot_data = load_json(SNAPSHOT_PATH) or {"updated": today, "countries": {}}
    snapshot = snapshot_data.get("countries", {})   # {countryCode: [speciesCodes]}

    # Load accumulated new firsts log
    firsts_data = load_json(NEW_FIRSTS) or {"detections": []}
    detections  = firsts_data["detections"]

    # No EBD needed — snapshot is the sole baseline
    print("Using API snapshot as baseline.")

    taxonomy  = fetch_taxonomy()
    countries = fetch_countries()
    print(f"Checking {len(countries)} countries …\n")

    new_this_run = []

    for i, country in enumerate(countries, 1):
        code = country["code"]
        name = country["name"]
        print(f"  [{i:3}/{len(countries)}] {name} ({code})", end="", flush=True)

        current_codes = set(fetch_spplist(code))

        prior_codes = set(snapshot.get(code, []))
        newly_added = current_codes - prior_codes

        if newly_added:
            print(f"  → {len(newly_added)} new species detected", end="")
            for sp_code in sorted(newly_added):
                tax = taxonomy.get(sp_code, {"sn": sp_code, "sc": "unknown"})
                cl_id = fetch_checklist_id(code, sp_code)
                entry = {
                    "detected": today,
                    "country_code": code,
                    "country": name,
                    "species_code": sp_code,
                    "common_name": tax["sn"],
                    "scientific_name": tax["sc"],
                    "ebird_url": f"https://ebird.org/species/{sp_code}/{code}",
                    "cl": cl_id,
                    "cl_url": f"https://ebird.org/checklist/{cl_id}" if cl_id else None,
                }
                detections.append(entry)
                new_this_run.append(entry)

        print()

        # Update snapshot
        snapshot[code] = list(current_codes)

    # Save updated files
    snapshot_data["updated"]   = today
    snapshot_data["countries"] = snapshot
    SNAPSHOT_PATH.write_text(
        json.dumps(snapshot_data, ensure_ascii=False, indent=None),
        encoding="utf-8",
    )

    firsts_data["last_updated"] = today
    firsts_data["detections"]   = detections
    NEW_FIRSTS.write_text(
        json.dumps(firsts_data, ensure_ascii=False),
        encoding="utf-8",
    )

    post_slack(new_this_run)

    print(f"\n{'─'*50}")
    print(f"New first country records detected this run: {len(new_this_run)}")
    if new_this_run:
        print()
        for r in new_this_run:
            print(f"  {r['country']:25s}  {r['common_name']}")
        print()
        print("Open first_records.html to review.")
    print(f"Total accumulated detections: {len(detections)}")
    print(f"Snapshot saved to {SNAPSHOT_PATH.name}")


def show_status():
    firsts_data = load_json(NEW_FIRSTS)
    if not firsts_data or not firsts_data.get("detections"):
        print("No detections logged yet. Run without --status to do a first check.")
        return

    dets = firsts_data["detections"]
    print(f"Accumulated detections: {len(dets)}")
    print(f"Last updated: {firsts_data.get('last_updated', 'unknown')}")
    print()
    # Show most recent 20
    for r in sorted(dets, key=lambda x: x["detected"], reverse=True)[:20]:
        print(f"  {r['detected']}  {r['country']:25s}  {r['common_name']}")


def bootstrap_first_records():
    """Build first_records.json from existing snapshot + taxonomy (no country API calls)."""
    snapshot_data = load_json(SNAPSHOT_PATH)
    if not snapshot_data:
        sys.exit("No species_snapshot.json found. Run without flags first to build a snapshot.")
    snapshot   = snapshot_data.get("countries", {})
    since      = snapshot_data.get("updated", str(date.today()))
    taxonomy   = fetch_taxonomy()

    # Fetch country names once
    print("Fetching country names …")
    countries  = fetch_countries()
    name_map   = {c["code"]: c["name"] for c in countries}

    firsts_data = load_json(NEW_FIRSTS) or {"detections": []}
    detections  = firsts_data.get("detections", [])

    # Build one record per (country, species) pair already in the snapshot
    records = []
    for cc, sp_codes in snapshot.items():
        cn = name_map.get(cc, cc)
        for sp_code in sp_codes:
            tax = taxonomy.get(sp_code, {"sn": sp_code, "sc": "unknown"})
            records.append({
                "cc":  cc,
                "cn":  cn,
                "sn":  tax["sn"],
                "sc":  tax["sc"],
                "tc":  sp_code,
                "fd":  None,
                "cl":  None,
            })

    # Overlay dates for anything already flagged as a new detection
    detected_lookup = {(d["country_code"], d["species_code"]): d["detected"] for d in detections}
    for r in records:
        r["fd"] = detected_lookup.get((r["cc"], r["tc"]))

    output = {
        "generated":         str(date.today()),
        "source":            "eBird API snapshot",
        "monitoring_since":  since,
        "countries_tracked": len(snapshot),
        "total":             len(records),
        "records":           records,
    }
    FIRST_RECORDS.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")
    print(f"first_records.json written: {len(records):,} records across {len(snapshot)} countries.")


def git_push():
    """Commit and push changed data files to GitHub."""
    import subprocess
    files = ["new_firsts.json", "species_snapshot.json", "first_records.json"]
    result = subprocess.run(
        ["git", "add"] + files,
        cwd=HERE, capture_output=True, text=True
    )
    # Check if there's anything staged
    diff = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=HERE
    )
    if diff.returncode == 0:
        print("No changes to push.")
        return
    today = str(date.today())
    subprocess.run(
        ["git", "commit", "-m", f"Monitor run {today}"],
        cwd=HERE, check=True
    )
    subprocess.run(["git", "push"], cwd=HERE, check=True)
    print("Pushed to GitHub.")


def backfill_checklists():
    """Fetch checklist IDs for existing detections that are missing them (within 30-day window)."""
    firsts_data = load_json(NEW_FIRSTS)
    if not firsts_data or not firsts_data.get("detections"):
        sys.exit("No detections to backfill.")
    detections = firsts_data["detections"]
    missing = [d for d in detections if not d.get("cl")]
    print(f"Backfilling checklist IDs for {len(missing)} detection(s) …")
    updated = 0
    for d in missing:
        cl_id = fetch_checklist_id(d["country_code"], d["species_code"])
        if cl_id:
            d["cl"]     = cl_id
            d["cl_url"] = f"https://ebird.org/checklist/{cl_id}"
            print(f"  ✓  {d['country']:25s}  {d['common_name']}  → {cl_id}")
            updated += 1
        else:
            print(f"  –  {d['country']:25s}  {d['common_name']}  (not found)")
    firsts_data["detections"] = detections
    NEW_FIRSTS.write_text(json.dumps(firsts_data, ensure_ascii=False), encoding="utf-8")
    print(f"\nDone. {updated}/{len(missing)} checklist IDs filled in.")


if __name__ == "__main__":
    if "--status" in sys.argv:
        show_status()
    elif "--bootstrap" in sys.argv:
        bootstrap_first_records()
    elif "--backfill-checklists" in sys.argv:
        backfill_checklists()
    elif "--push" in sys.argv:
        git_push()
    else:
        run_update()
