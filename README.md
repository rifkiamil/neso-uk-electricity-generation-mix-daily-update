# UK Electricity Generation Mix — Daily Update Pipeline

Automated daily pipeline that fetches half-hourly electricity generation data by fuel type and carbon intensity for Great Britain from the [NESO (National Energy System Operator) Data Portal](https://www.neso.energy/data-portal/historic-generation-mix/historic_gb_generation_mix).

## How It Works

A [GitHub Actions workflow](.github/workflows/update-data.yml) runs daily at 06:00 UTC:

1. Fetches the current year's data from the NESO API (and the previous year during the first 7 days of January for year-rollover coverage)
2. Saves per-year CSV files into `data/`
3. Regenerates a combined Parquet file (`data/neso-uk-electricity-generation-mix.parquet`)
4. Validates the output with DuckDB (record counts, NULL checks, duplicate detection, regression checks)
5. Uploads the data files as **GitHub Actions artifacts** (retained for 90 days)

**Data files are not stored in this repository.** They are generated fresh on each workflow run and made available as downloadable artifacts from the [Actions tab](../../actions).

You can also trigger the workflow manually via the **"Run workflow"** button on the Actions page.

## Repository Structure

```
├── .github/workflows/
│   └── update-data.yml          # Daily scheduled workflow
├── download_generation_mix.py   # NESO API client (fetch_year function)
├── update_current_year.py       # Daily update orchestrator
├── README.md
├── .gitignore
└── data/                        # Generated at runtime (gitignored)
    ├── generation_mix_2019.csv
    ├── generation_mix_2020.csv
    ├── ...
    ├── generation_mix_{current_year}.csv
    └── neso-uk-electricity-generation-mix.parquet
```

## Running Locally

```bash
# Install dependencies
pip install requests pandas duckdb

# Run the daily update (fetches current year, rebuilds Parquet, validates)
python update_current_year.py

# Or do a full historical download (all years)
python download_generation_mix.py
```

Generated files will be written to the `data/` directory.

## Data Details

### Source

| | |
|---|---|
| **Granularity** | 30-minute intervals |
| **Source** | NESO Historic GB Generation Mix |
| **License** | Open Data Commons Attribution License (ODC-By) |
| **API** | `https://api.neso.energy/api/3/action/datastore_search_sql` |
| **Resource ID** | `f93d1835-75bc-43e5-84ad-12472b180a98` |

### Generation by Fuel Type (MW)

| Column | Description |
|---|---|
| `DATETIME` | Timestamp in UTC (ISO 8601) |
| `GAS` | Gas-fired generation (MW) |
| `COAL` | Coal-fired generation (MW) |
| `NUCLEAR` | Nuclear generation (MW) |
| `WIND` | Metered wind generation (MW) |
| `WIND_EMB` | Embedded (non-metered) wind generation (MW) |
| `HYDRO` | Hydro generation (MW) |
| `SOLAR` | Solar generation (MW) |
| `BIOMASS` | Biomass generation (MW) |
| `STORAGE` | Storage output (MW) |
| `IMPORTS` | Interconnector imports (MW) |
| `OTHER` | Other fuel types (MW) |

### Aggregated Totals (MW)

| Column | Description |
|---|---|
| `GENERATION` | Total generation — sum of all fuel types |
| `FOSSIL` | Fossil generation (gas + coal) |
| `RENEWABLE` | Renewable generation (wind + hydro + solar) |
| `LOW_CARBON` | Low-carbon generation (renewables + nuclear + biomass) |
| `ZERO_CARBON` | Zero-carbon generation (renewables + nuclear) |
| `CARBON_INTENSITY` | Carbon intensity of electricity (gCO₂/kWh) |

### Percentage Shares (%)

Each fuel type and aggregate has a corresponding `_perc` column:
`GAS_perc`, `COAL_perc`, `NUCLEAR_perc`, `WIND_perc`, `WIND_EMB_perc`, `HYDRO_perc`, `SOLAR_perc`, `BIOMASS_perc`, `STORAGE_perc`, `IMPORTS_perc`, `OTHER_perc`, `GENERATION_perc`, `FOSSIL_perc`, `RENEWABLE_perc`, `LOW_CARBON_perc`, `ZERO_CARBON_perc`

## Data Source & Attribution

Data published by the **National Energy System Operator (NESO)** under the Open Data Commons Attribution License.

- Portal: [Historic GB Generation Mix](https://www.neso.energy/data-portal/historic-generation-mix/historic_gb_generation_mix)
- API: `https://api.neso.energy/api/3/action/datastore_search_sql`
- Resource ID: `f93d1835-75bc-43e5-84ad-12472b180a98`

