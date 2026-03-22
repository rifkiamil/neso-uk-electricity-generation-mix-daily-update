![Update Generation Mix Data](https://github.com/rifkiamil/neso-uk-electricity-generation-mix-daily-update/actions/workflows/update-data.yml/badge.svg)
![License](https://img.shields.io/github/license/rifkiamil/neso-uk-electricity-generation-mix-daily-update)
![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/validated_with-DuckDB-FFF000?logo=duckdb&logoColor=black)

# 🇬🇧 UK Electricity Generation Mix

Half-hourly electricity generation data by fuel type for Great Britain, updated daily from the [NESO Data Portal](https://www.neso.energy/data-portal/historic-generation-mix/historic_gb_generation_mix).

| | |
|---|---|
| **Granularity** | 30-minute intervals |
| **Coverage** | 2009 – present |
| **Formats** | CSV (per year) and Parquet (combined) |
| **Source** | National Energy System Operator (NESO) |
| **License** | [ODC-By](https://opendatacommons.org/licenses/by/) |
| **Updated** | Daily at 06:00 UTC via GitHub Actions |

---

## 📊 Data Discovery & Usage

### Get the data

The validated data files live in the [`data/`](data/) directory of this repository:

| File | Description |
|---|---|
| `data/generation_mix_{year}.csv` | Per-year CSV with all half-hourly records |
| `data/neso-uk-electricity-generation-mix.parquet` | Combined Parquet file across all years |

```bash
# Clone and start querying
git clone https://github.com/rifkiamil/neso-uk-electricity-generation-mix-daily-update.git
cd neso-uk-electricity-generation-mix-daily-update
```

### Query with DuckDB

```sql
-- Load and explore
SELECT COUNT(*) AS total_records,
       MIN(DATETIME) AS earliest,
       MAX(DATETIME) AS latest
FROM read_parquet('data/neso-uk-electricity-generation-mix.parquet');

-- Daily average generation by fuel type
SELECT CAST(DATETIME AS DATE) AS day,
       ROUND(AVG(GAS), 0)     AS avg_gas_mw,
       ROUND(AVG(WIND), 0)    AS avg_wind_mw,
       ROUND(AVG(SOLAR), 0)   AS avg_solar_mw,
       ROUND(AVG(NUCLEAR), 0) AS avg_nuclear_mw
FROM read_parquet('data/neso-uk-electricity-generation-mix.parquet')
GROUP BY day
ORDER BY day DESC
LIMIT 10;

-- Carbon intensity trend by month
SELECT DATE_TRUNC('month', CAST(DATETIME AS TIMESTAMP)) AS month,
       ROUND(AVG(CARBON_INTENSITY), 1) AS avg_gco2_kwh
FROM read_parquet('data/neso-uk-electricity-generation-mix.parquet')
GROUP BY month
ORDER BY month;
```

### Query with Python / pandas

```python
import pandas as pd

# From Parquet (fastest)
df = pd.read_parquet("data/neso-uk-electricity-generation-mix.parquet")

# From a single year's CSV
df_2025 = pd.read_csv("data/generation_mix_2025.csv", parse_dates=["DATETIME"])

# Renewable share over time
df["DATETIME"] = pd.to_datetime(df["DATETIME"])
monthly = df.set_index("DATETIME").resample("ME")["RENEWABLE_perc"].mean()
print(monthly.tail(12))
```

### Data dictionary

#### Generation by fuel type (MW)

| Column | Description |
|---|---|
| `DATETIME` | Timestamp (ISO 8601) |
| `GAS` | Gas-fired generation |
| `COAL` | Coal-fired generation |
| `NUCLEAR` | Nuclear generation |
| `WIND` | Metered wind generation |
| `WIND_EMB` | Embedded (non-metered) wind |
| `HYDRO` | Hydro generation |
| `SOLAR` | Solar generation |
| `BIOMASS` | Biomass generation |
| `STORAGE` | Storage output |
| `IMPORTS` | Interconnector imports |
| `OTHER` | Other fuel types |

#### Aggregated totals (MW)

| Column | Description |
|---|---|
| `GENERATION` | Total generation — sum of all fuel types |
| `FOSSIL` | Gas + coal |
| `RENEWABLE` | Wind + hydro + solar |
| `LOW_CARBON` | Renewables + nuclear + biomass |
| `ZERO_CARBON` | Renewables + nuclear |
| `CARBON_INTENSITY` | gCO₂/kWh |

#### Percentage shares (%)

Every column above also has a `_perc` variant — e.g. `GAS_perc`, `WIND_perc`, `RENEWABLE_perc`, `CARBON_INTENSITY` (already a rate).

### Attribution

Data published by the **National Energy System Operator (NESO)** under the [Open Data Commons Attribution License (ODC-By)](https://opendatacommons.org/licenses/by/).

> Portal: [Historic GB Generation Mix](https://www.neso.energy/data-portal/historic-generation-mix/historic_gb_generation_mix)
> API: `https://api.neso.energy/api/3/action/datastore_search_sql`
> Resource ID: `f93d1835-75bc-43e5-84ad-12472b180a98`

---

## 🔧 Build, Pipeline & Testing

### Blue-green deployment

This pipeline uses a **blue-green deployment** pattern to ensure the data in `data/` is always valid. New data is never written directly to `data/` — it goes through a staging area first.

```
┌─────────────────────────────────────────────────────────┐
│  🟢 GREEN PHASE                                        │
│                                                         │
│  1. Fetch current year from NESO API                    │
│  2. Write CSVs to  data/staging/                        │
│  3. Build combined Parquet in  data/staging/             │
│                                                         │
│  🔍 VALIDATE                                            │
│                                                         │
│  4. DuckDB checks on staging Parquet:                   │
│     • Record count > 0                                  │
│     • No NULLs in DATETIME or GENERATION                │
│     • No record count regression vs current blue data   │
│     • Duplicate timestamp detection                     │
│                                                         │
│  🔵 BLUE PHASE  (only if validation passes)             │
│                                                         │
│  5. Promote  staging/ → data/                           │
│  6. Clean up staging/                                   │
│  7. Commit & push updated data/                         │
│                                                         │
│  ❌ ON FAILURE                                           │
│                                                         │
│  • data/ is NEVER touched                               │
│  • staging/ is discarded                                │
│  • Workflow exits with error                            │
└─────────────────────────────────────────────────────────┘
```

### GitHub Actions workflow

The [`update-data.yml`](.github/workflows/update-data.yml) workflow runs:

| Trigger | Schedule |
|---|---|
| ⏰ Cron | Daily at **06:00 UTC** |
| 🖱️ Manual | **Run workflow** button on the [Actions page](../../actions) |

**Steps:** checkout → setup Python 3.12 → install deps → run blue-green update → commit & push validated data back to the repo.

### Run locally

```bash
# Install dependencies
pip install requests pandas duckdb

# Run the daily blue-green update (current year)
python update_current_year.py

# Or do a full historical download (all years 2009–2026)
python download_generation_mix.py
```

### Validation checks

The pipeline runs these DuckDB checks on the staging Parquet before promoting:

| Check | Behaviour |
|---|---|
| **Record count** | Must be > 0 or pipeline fails |
| **NULL DATETIME** | Any NULLs → fail |
| **NULL GENERATION** | Any NULLs → fail |
| **Regression** | If new count < previous blue count → fail |
| **Duplicates** | Duplicate timestamps → warning (non-blocking) |

### Repository structure

```
├── .github/workflows/
│   └── update-data.yml              # Daily scheduled workflow
├── data/                            # 🔵 Blue — validated, committed data
│   ├── generation_mix_2026.csv
│   ├── ...
│   └── neso-uk-electricity-generation-mix.parquet
├── download_generation_mix.py       # NESO API client (fetch_year function)
├── update_current_year.py           # Blue-green update orchestrator
├── .gitignore
├── LICENSE
└── README.md
```

> `data/staging/` is the 🟢 green working directory — created at runtime, gitignored, and cleaned up after each run.

