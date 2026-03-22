"""
Blue-Green daily update for the UK Electricity Generation Mix dataset.

Strategy:
  1. FETCH   → download new data into  data/staging/  (green)
  2. BUILD   → create Parquet in       data/staging/
  3. VALIDATE→ DuckDB checks on        data/staging/
  4. PROMOTE → copy staging → data/    (blue)  ONLY if validation passes
  5. CLEANUP → remove staging/

If anything fails, data/ (blue) is never touched.

Reuses fetch_year() from download_generation_mix.py.
"""

import glob
import os
import shutil
import sys
from datetime import datetime, timezone

import duckdb

from download_generation_mix import fetch_year

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")              # blue  (safe)
STAGING_DIR = os.path.join(SCRIPT_DIR, "data", "staging") # green (working)
PARQUET_NAME = "neso-uk-electricity-generation-mix.parquet"
ROLLOVER_GRACE_DAYS = 7


# ---------------------------------------------------------------------------
# 1. Determine which years to fetch
# ---------------------------------------------------------------------------

def years_to_fetch() -> list[int]:
    """Return the list of years that need fetching today."""
    now = datetime.now(timezone.utc)
    current_year = now.year
    day_of_year = now.timetuple().tm_yday

    if day_of_year <= ROLLOVER_GRACE_DAYS:
        return [current_year - 1, current_year]
    return [current_year]


# ---------------------------------------------------------------------------
# 2. Fetch new data → staging  (green)
# ---------------------------------------------------------------------------

def fetch_to_staging(years: list[int]) -> None:
    """Fetch data for each year and write CSVs into data/staging/."""
    os.makedirs(STAGING_DIR, exist_ok=True)

    # Seed staging with existing blue CSVs so the combined parquet covers
    # all years, not just the ones we're fetching today.
    for csv in sorted(glob.glob(os.path.join(DATA_DIR, "generation_mix_*.csv"))):
        dest = os.path.join(STAGING_DIR, os.path.basename(csv))
        if not os.path.exists(dest):
            shutil.copy2(csv, dest)
            print(f"  Copied {os.path.basename(csv)} → staging/")

    for year in years:
        print(f"\n[{year}] Fetching data from NESO API...")
        df = fetch_year(year)
        if df.empty:
            print(f"[{year}] WARNING: No data returned — skipping CSV write")
            continue
        filename = os.path.join(STAGING_DIR, f"generation_mix_{year}.csv")
        df.to_csv(filename, index=False)
        print(f"[{year}] Saved {len(df):,} records → staging/")


# ---------------------------------------------------------------------------
# 3. Build Parquet in staging  (green)
# ---------------------------------------------------------------------------

def build_staging_parquet() -> str:
    """Combine all staging CSVs into a single Parquet file. Returns path."""
    csv_pattern = os.path.join(STAGING_DIR, "generation_mix_*.csv")
    csv_files = sorted(glob.glob(csv_pattern))
    if not csv_files:
        print(f"ERROR: No CSVs found in {STAGING_DIR}")
        sys.exit(1)

    parquet_path = os.path.join(STAGING_DIR, PARQUET_NAME)
    print(f"\nBuilding Parquet from {len(csv_files)} CSV(s) in staging/...")

    con = duckdb.connect()
    csv_list_sql = ", ".join(f"'{f}'" for f in csv_files)
    con.execute(
        f"""
        COPY (
            SELECT * FROM read_csv_auto([{csv_list_sql}])
            ORDER BY CAST(DATETIME AS TIMESTAMP)
        ) TO '{parquet_path}' (FORMAT PARQUET)
        """
    )
    con.close()
    print(f"  Written → staging/{PARQUET_NAME}")
    return parquet_path


# ---------------------------------------------------------------------------
# 4. Validate staging Parquet with DuckDB
# ---------------------------------------------------------------------------

def validate_parquet(parquet_path: str, previous_count: int | None) -> bool:
    """Run validation queries on the staging parquet. Returns True if valid."""
    con = duckdb.connect()
    ok = True

    print("\n" + "=" * 60)
    print("VALIDATION REPORT  (staging)")
    print("=" * 60)

    # Total record count
    total = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]
    print(f"\nTotal records : {total:,}")
    if total == 0:
        print("  FAIL — zero records")
        ok = False

    # Date range
    row = con.execute(
        f"""
        SELECT MIN(CAST(DATETIME AS TIMESTAMP)),
               MAX(CAST(DATETIME AS TIMESTAMP))
        FROM read_parquet('{parquet_path}')
        """
    ).fetchone()
    print(f"Date range    : {row[0]}  →  {row[1]}")

    # Per-year counts
    print("\nRecords per year:")
    year_rows = con.execute(
        f"""
        SELECT YEAR(CAST(DATETIME AS TIMESTAMP)) AS yr, COUNT(*) AS cnt
        FROM read_parquet('{parquet_path}')
        GROUP BY yr ORDER BY yr
        """
    ).fetchall()
    for yr, cnt in year_rows:
        print(f"  {yr}: {cnt:,}")

    # NULL checks on critical columns
    null_dt = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') WHERE DATETIME IS NULL"
    ).fetchone()[0]
    null_gen = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') WHERE GENERATION IS NULL"
    ).fetchone()[0]
    print(f"\nNULL DATETIME  : {null_dt}")
    print(f"NULL GENERATION: {null_gen}")
    if null_dt > 0 or null_gen > 0:
        print("  FAIL — critical columns contain NULLs")
        ok = False

    # Duplicate timestamps
    dupes = con.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT DATETIME, COUNT(*) AS n
            FROM read_parquet('{parquet_path}')
            GROUP BY DATETIME HAVING n > 1
        )
        """
    ).fetchone()[0]
    print(f"Duplicate timestamps: {dupes}")
    if dupes > 0:
        print("  WARNING — duplicate timestamps found")

    # Regression check — new data should not lose records
    if previous_count is not None:
        print(f"\nPrevious count: {previous_count:,}")
        if total < previous_count:
            print(f"  FAIL — record count decreased by {previous_count - total:,}")
            ok = False
        else:
            print(f"  OK — gained {total - previous_count:,} records")

    print("\n" + "=" * 60)
    status = "PASSED ✅" if ok else "FAILED ❌"
    print(f"Validation {status}")
    print("=" * 60)

    con.close()
    return ok


# ---------------------------------------------------------------------------
# 5. Promote staging → data  (green → blue)
# ---------------------------------------------------------------------------

def promote_staging() -> None:
    """Copy validated staging files into the safe data/ directory."""
    print("\nPromoting staging → data/  (green → blue)...")

    # Copy CSVs
    for csv in sorted(glob.glob(os.path.join(STAGING_DIR, "generation_mix_*.csv"))):
        dest = os.path.join(DATA_DIR, os.path.basename(csv))
        shutil.copy2(csv, dest)
        print(f"  ✓ {os.path.basename(csv)}")

    # Copy Parquet
    src_parquet = os.path.join(STAGING_DIR, PARQUET_NAME)
    dst_parquet = os.path.join(DATA_DIR, PARQUET_NAME)
    if os.path.exists(src_parquet):
        shutil.copy2(src_parquet, dst_parquet)
        print(f"  ✓ {PARQUET_NAME}")

    print("  Promotion complete.")


def cleanup_staging() -> None:
    """Remove the staging directory."""
    if os.path.isdir(STAGING_DIR):
        shutil.rmtree(STAGING_DIR)
        print("  Staging cleaned up.")


# ---------------------------------------------------------------------------
# 6. Helpers
# ---------------------------------------------------------------------------

def get_blue_record_count() -> int | None:
    """Read record count from the current blue Parquet file, if any."""
    blue_parquet = os.path.join(DATA_DIR, PARQUET_NAME)
    if not os.path.exists(blue_parquet):
        return None
    try:
        con = duckdb.connect()
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{blue_parquet}')"
        ).fetchone()[0]
        con.close()
        return count
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("NESO UK Electricity Generation Mix — Blue-Green Update")
    print("=" * 60)

    now = datetime.now(timezone.utc)
    print(f"Run time (UTC): {now.isoformat()}")

    # Snapshot the blue record count before any changes
    previous_count = get_blue_record_count()
    if previous_count is not None:
        print(f"Blue record count: {previous_count:,}")
    else:
        print("Blue record count: (no existing data)")

    # Determine which years to update
    years = years_to_fetch()
    print(f"Years to fetch: {years}")

    # --- GREEN PHASE ---
    print("\n" + "-" * 60)
    print("GREEN PHASE — fetch & build in staging/")
    print("-" * 60)

    cleanup_staging()  # start clean
    fetch_to_staging(years)
    staging_parquet = build_staging_parquet()

    # --- VALIDATE ---
    valid = validate_parquet(staging_parquet, previous_count)

    if not valid:
        print("\n❌ Validation failed — blue data/ is UNTOUCHED.")
        cleanup_staging()
        sys.exit(1)

    # --- PROMOTE green → blue ---
    print("\n" + "-" * 60)
    print("BLUE PHASE — promoting validated data")
    print("-" * 60)

    promote_staging()
    cleanup_staging()

    print("\n✅ Done — blue data/ updated successfully.")


if __name__ == "__main__":
    main()
