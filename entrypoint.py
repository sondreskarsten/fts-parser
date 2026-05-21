"""FTS parser entrypoint. Reads XLSX snapshots, resolves Norwegian VAT→orgnr, emits 12-col CDC."""

import os, sys
from datetime import date
from reader import GCSReader
from parser import parse_xlsx
from cdc import FTSCDC

GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "fts")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
SNAPSHOT_DATE = os.environ.get("SNAPSHOT_DATE", "")
YEAR_FILTER = os.environ.get("YEARS", "")


def main():
    print(f"{'='*60}\n  fts-parser — mode: {RUN_MODE}\n  {date.today().isoformat()}\n  GCS: gs://{GCS_BUCKET}/{GCS_PREFIX}/\n{'='*60}", flush=True)

    reader = GCSReader(GCS_BUCKET, GCS_PREFIX)
    snapshot_dates = reader.list_snapshot_dates()
    if not snapshot_dates:
        print("  No snapshots. Run fts-collector first.", flush=True)
        sys.exit(1)

    snapshot = SNAPSHOT_DATE if SNAPSHOT_DATE else snapshot_dates[-1]
    print(f"  Using snapshot: {snapshot}", flush=True)

    manifest = reader.load_manifest(snapshot)
    if not manifest:
        print("  No manifest found.", flush=True)
        sys.exit(1)

    years = [int(y) for y in YEAR_FILTER.split(",") if y.strip()] if YEAR_FILTER else None
    all_resolved = []
    all_unresolved = []

    for yr_info in manifest.get("years", []):
        if yr_info.get("status") != "ok":
            continue
        year = yr_info["year"]
        if years and year not in years:
            continue

        xlsx_bytes = reader.read_xlsx_bytes(snapshot, year)
        if not xlsx_bytes:
            print(f"  {year}: no XLSX", flush=True)
            continue

        resolved, unresolved = parse_xlsx(xlsx_bytes, year)
        print(f"  {year}: {len(resolved)} resolved, {len(unresolved)} unresolved NO", flush=True)
        all_resolved.extend(resolved)
        all_unresolved.extend(unresolved)

    print(f"\n  Total: {len(all_resolved):,} resolved, {len(all_unresolved):,} unresolved", flush=True)

    cdc = FTSCDC(GCS_BUCKET, GCS_PREFIX)
    run_mode = "bootstrap" if RUN_MODE == "bootstrap" else "daily"
    stats = cdc.run(all_resolved, all_unresolved, date.today().isoformat(), run_mode=run_mode)
    print(f"  CDC: {stats}", flush=True)


if __name__ == "__main__":
    main()
