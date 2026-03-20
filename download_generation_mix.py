"""
Download historical power generation mix data from NESO (National Energy System Operator)
data portal and save as separate CSV files by year.

Data source: https://www.neso.energy/data-portal/historic-generation-mix/historic_gb_generation_mix
API: CKAN DataStore API at https://api.neso.energy
Resource ID: f93d1835-75bc-43e5-84ad-12472b180a98
"""

import os
import sys
import time
import requests
import pandas as pd

BASE_URL = "https://api.neso.energy/api/3/action/datastore_search_sql"
RESOURCE_ID = "f93d1835-75bc-43e5-84ad-12472b180a98"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
YEARS = range(2019, 2027)


def fetch_year(year: int, max_retries: int = 3) -> pd.DataFrame:
    """Fetch all records for a given year using the CKAN SQL API with pagination."""
    all_records = []
    page_size = 10000
    offset = 0

    while True:
        sql = (
            f'SELECT * FROM "{RESOURCE_ID}" '
            f"WHERE \"DATETIME\" >= '{year}-01-01T00:00:00' "
            f"AND \"DATETIME\" < '{year + 1}-01-01T00:00:00' "
            f"ORDER BY \"DATETIME\" "
            f"LIMIT {page_size} OFFSET {offset}"
        )
        print(f"  Fetching offset {offset}...", end=" ", flush=True)

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(BASE_URL, params={"sql": sql}, timeout=120)
                resp.raise_for_status()
                data = resp.json()
                break
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.Timeout) as e:
                if attempt == max_retries:
                    raise
                wait = 2 ** attempt
                print(f"\n    Retry {attempt}/{max_retries} after error: {e}. "
                      f"Waiting {wait}s...", end=" ", flush=True)
                time.sleep(wait)

        if not data.get("success"):
            raise RuntimeError(f"API error: {data.get('error', 'unknown')}")

        records = data["result"]["records"]
        print(f"got {len(records)} records")
        if not records:
            break

        all_records.extend(records)
        if len(records) < page_size:
            break
        offset += page_size
        time.sleep(0.5)  # be polite to the API

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    # Drop the internal CKAN row ID if present
    if "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)
    if "_full_text" in df.columns:
        df.drop(columns=["_full_text"], inplace=True)
    return df


def main():
    print("NESO Historic GB Generation Mix Downloader")
    print("=" * 50)

    os.makedirs(DATA_DIR, exist_ok=True)

    failures = []

    for year in YEARS:
        print(f"\n[{year}] Downloading data...")
        try:
            df = fetch_year(year)
            if df.empty:
                print(f"[{year}] WARNING: No data found for this year")
                failures.append((year, "No data returned"))
                continue

            filename = os.path.join(DATA_DIR, f"generation_mix_{year}.csv")
            df.to_csv(filename, index=False)
            print(f"[{year}] Saved {len(df)} records to {filename}")

        except requests.exceptions.Timeout:
            msg = "Request timed out"
            print(f"[{year}] ERROR: {msg}")
            failures.append((year, msg))
        except requests.exceptions.ConnectionError as e:
            msg = f"Connection error: {e}"
            print(f"[{year}] ERROR: {msg}")
            failures.append((year, msg))
        except requests.exceptions.HTTPError as e:
            msg = f"HTTP error {e.response.status_code}: {e}"
            print(f"[{year}] ERROR: {msg}")
            failures.append((year, msg))
        except Exception as e:
            msg = f"Unexpected error: {e}"
            print(f"[{year}] ERROR: {msg}")
            failures.append((year, msg))

    print("\n" + "=" * 50)
    if failures:
        print("The following years had issues:")
        for year, reason in failures:
            print(f"  {year}: {reason}")
        sys.exit(1)
    else:
        print("All years downloaded successfully.")


if __name__ == "__main__":
    main()

