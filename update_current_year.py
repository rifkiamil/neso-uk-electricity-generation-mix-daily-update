"""
Daily update script for the UK Electricity Generation Mix dataset.

- Fetches only the current year's data (and previous year during rollover grace period)
- Regenerates the combined Parquet file from all per-year CSVs
- Validates the output with DuckDB and prints a summary report
- Exits non-zero if validation fails

Reuses fetch_year() from download_generation_mix.py.
"""

import glob
import os
import sys
from datetime import datetime, timezone

import duckdb

from download_generation_mix import fetch_year

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PARQUET_FILE = os.path.join(DATA_DIR, "neso-uk-electricity-generation-mix.parquet")
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
# 2. Fetch and save CSV(s)
# ---------------------------------------------------------------------------

def update_csvs(years: list[int]) -> None:
    """Fetch data for each year and overwrite its CSV file in data/."""
    os.makedirs(DATA_DIR, exist_ok=True)
    for year in years:
        print(f"\n[{year}] Fetching data from NESO API...")
        df = fetch_year(year)
        if df.empty:
            print(f"[{year}] WARNING: No data returned — skipping CSV write")
            continue
        filename = os.path.join(DATA_DIR, f"generation_mix_{year}.csv")
        df.to_csv(filename, index=False)
        print(f"[{year}] Saved {len(df):,} records to {filename}")


# ---------------------------------------------------------------------------
# 3. Regenerate combined Parquet
# ---------------------------------------------------------------------------

def rebuild_parquet() -> str:
    """Combine all per-year CSVs into a single Parquet file. Returns path."""
    csv_pattern = os.path.join(DATA_DIR, "generation_mix_*.csv")
    csv_files = sorted(glob.glob(csv_pattern))
    if not csv_files:
        print(f"ERROR: No generation_mix_*.csv files found in {DATA_DIR}")
        sys.exit(1)

    print(f"\nRebuilding Parquet from {len(csv_files)} CSV file(s)...")

    # Remove old dynamically-named parquet files to avoid stale copies
    parquet_pattern = os.path.join(DATA_DIR, "neso-uk-electricity-generation-mix*.parquet")
    for old in glob.glob(parquet_pattern):
        if old != PARQUET_FILE:
            print(f"  Removing old Parquet file: {old}")
            os.remove(old)

    con = duckdb.connect()
    con.execute(
        """
        COPY (
            SELECT * FROM read_csv_auto(?)
            ORDER BY CAST(DATETIME AS TIMESTAMP)
        ) TO ? (FORMAT PARQUET)
        """,
        [csv_files, PARQUET_FILE],
    )
    con.close()
    print(f"  Written to {PARQUET_FILE}")
    return PARQUET_FILE


# ---------------------------------------------------------------------------
# 4. Validate with DuckDB
# ---------------------------------------------------------------------------

def validate_parquet(parquet_path: str, previous_count: int | None) -> bool:
    """Run validation queries and print a report. Returns True if valid."""
    con = duckdb.connect()
    ok = True

    print("\n" + "=" * 60)
    print("VALIDATION REPORT")
    print("=" * 60)

    # Total record count
    total = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [parquet_path]
    ).fetchone()[0]
    print(f"\nTotal records : {total:,}")
    if total == 0:
        print("  FAIL — zero records")
        ok = False

    # Date range
    row = con.execute(
        """
        SELECT MIN(CAST(DATETIME AS TIMESTAMP)),
               MAX(CAST(DATETIME AS TIMESTAMP))
        FROM read_parquet(?)
        """,
        [parquet_path],
    ).fetchone()
    print(f"Date range    : {row[0]}  →  {row[1]}")

    # Per-year counts
    print("\nRecords per year:")
    year_rows = con.execute(
        """
        SELECT YEAR(CAST(DATETIME AS TIMESTAMP)) AS yr, COUNT(*) AS cnt
        FROM read_parquet(?)
        GROUP BY yr ORDER BY yr
        """,
        [parquet_path],
    ).fetchall()
    for yr, cnt in year_rows:
        print(f"  {yr}: {cnt:,}")

    # NULL checks
    null_dt = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?) WHERE DATETIME IS NULL",
        [parquet_path],
    ).fetchone()[0]
    null_gen = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?) WHERE GENERATION IS NULL",
        [parquet_path],
    ).fetchone()[0]
    print(f"\nNULL DATETIME : {null_dt}")
    print(f"NULL GENERATION: {null_gen}")
    if null_dt > 0 or null_gen > 0:
        print("  FAIL — critical columns contain NULLs")
        ok = False

    # Duplicate timestamps
    dupes = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT DATETIME, COUNT(*) AS n
            FROM read_parquet(?)
            GROUP BY DATETIME HAVING n > 1
        )
        """,
        [parquet_path],
    ).fetchone()[0]
    print(f"Duplicate timestamps: {dupes}")
    if dupes > 0:
        print("  WARNING — duplicate timestamps found")

    # Record count regression check
    if previous_count is not None:
        print(f"\nPrevious count: {previous_count:,}")
        if total < previous_count:
            print(f"  FAIL — record count decreased by {previous_count - total:,}")
            ok = False
        else:
            print(f"  OK — gained {total - previous_count:,} records")

    print("\n" + "=" * 60)
    status = "PASSED" if ok else "FAILED"
    print(f"Validation {status}")
    print("=" * 60)

    con.close()
    return ok


# ---------------------------------------------------------------------------
# 5. Helpers
# ---------------------------------------------------------------------------

def get_previous_record_count() -> int | None:
    """Read record count from the existing Parquet file, if any."""
    parquet_pattern = os.path.join(DATA_DIR, "neso-uk-electricity-generation-mix*.parquet")
    parquet_files = glob.glob(parquet_pattern)
    if not parquet_files:
        return None
    # Use the first match (there should be exactly one)
    path = parquet_files[0]
    try:
        con = duckdb.connect()
        count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [path]
        ).fetchone()[0]
        con.close()
        return count
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("NESO UK Electricity Generation Mix — Daily Update")
    print("=" * 60)

    now = datetime.now(timezone.utc)
    print(f"Run time (UTC): {now.isoformat()}")

    # Snapshot the previous record count before any changes
    previous_count = get_previous_record_count()

    # Determine which years to update
    years = years_to_fetch()
    print(f"Years to fetch: {years}")

    # Fetch and write CSVs
    update_csvs(years)

    # Rebuild combined Parquet
    parquet_path = rebuild_parquet()

    # Validate
    valid = validate_parquet(parquet_path, previous_count)

    if not valid:
        print("\nAborting — validation failed. Changes should NOT be committed.")
        sys.exit(1)

    print("\nDone — all checks passed.")


if __name__ == "__main__":
    main()

