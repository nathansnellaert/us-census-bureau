# us-census-bureau

Connector for the US Census Bureau's public JSON API (`https://api.census.gov/data.json`). Publishes a curated, exhaustive (within declared scope) slice of core economic and demographic programs as Subsets datasets. See `catalog.json` for the full machine-readable coverage manifest.

## Published datasets

| Subset id | Source program | Geography | Coverage |
|---|---|---|---|
| `us_census_catalog` | Census data.json registry | n/a | One row per (program, vintage) this connector ingests |
| `us_census_acs` | ACS 1-Year Detailed Tables (`acs/acs1`) | state, county | 2010 → latest; curated set of 20 Summary File tables (B01001, B02001, B03001, B03002, B14001, B15003, B17001, B19001, B19013, B22001, B23025, B24010, B08301, B08303, B25001, B25002, B25003, B25061, B25077, B01002), estimate variables only |
| `us_census_cbp` | County Business Patterns (`cbp`) | state | 2012 → latest; all NAICS sectors (2-6 digit) via the vintage's NAICS dimension; measures EMP, PAYANN, ESTAB, PAYQTR1. County-level CBP is intentionally excluded for now (see "Excluded on purpose" below). |
| `us_census_pep` | Population Estimates Program (`pep/*`) | state, county | Every `pep/*` endpoint the API exposes for vintages ≥ 2015 (excludes intercensal `pep/int_*` archives); long-form per-variable rows so schema evolution across vintages doesn't break the asset |
| `us_census_saipe` | SAIPE timeseries (`timeseries/poverty/saipe`) | state, county | 2005 → latest; measures covering poverty rates/counts and median household income |
| `us_census_sahie` | SAHIE timeseries (`timeseries/healthins/sahie`) | state, county | 2008 → latest; total-population bucket (AGECAT/RACECAT/SEXCAT/IPRCAT all filtered to "all") |
| `us_census_intltrade_annual` | International Trade timeseries (`timeseries/intltrade/{imports,exports}/enduse`) | national | 2013 → latest; annual year-to-date totals (`*_YR` columns) at the 1-digit End-Use code level, both imports and exports |
| `us_census_ecnbasic` | Economic Census (`ecnbasic`) | national | 2017 and 2022 vintages; all NAICS codes; measures ESTAB, EMP, PAYANN, RCPTOT |

## Coverage decisions

This connector is intentionally **not** a complete mirror of every Census API endpoint (~1,700 catalog entries). The choices:

- **Single geography granularity per program.** State + county for state-level programs, national for national-level programs. Sub-county (tract, block group, ZIP), Puerto Rico, and island areas are out of scope for this release.
- **Single path per program where feasible.** ACS uses `acs/acs1` only, not the 5-year (`acs5`) or 3-year (`acs3`) tables or the Puerto Rico profiles — 1-year is the most commonly referenced release.
- **Curated ACS variable set.** ACS has ~30 000 variables per vintage; we publish the 20 Summary File tables listed above (age, sex, race, Hispanic origin, educational attainment, household income, poverty, SNAP, employment, occupation, commute, housing, tenure, rent). New variables can be added by editing `src/census_utils/constants.py:ACS_TABLE_PREFIXES`.
- **Exhaustive within scope.** Within the declared scope, the connector pulls every vintage and every variable. Variable lists are automatically chunked into 49-per-request batches to stay under the Census API's 50-variable ceiling, so tables like B01001 (49 age/sex cells) and B25077 all ship complete.
- **No `POPGROUP` / `AGECAT` index codes as values.** PEP keeps only numeric measurement variables; dimension/index codes are dropped. SAHIE filters AGECAT/RACECAT/SEXCAT/IPRCAT to the "all" bucket.

## Excluded on purpose

- **County-level CBP.** State + NAICS detail across 12 vintages already produces a few hundred thousand rows; the per-county NAICS-detail responses are 10x larger and the Census API rate-limits the per-state county queries we'd need. Adding county CBP is a follow-up.
- **Building Permits Survey (BPS).** Not available on the JSON API.
- **Decennial microdata, PUMS, and Current Population Survey (CPS).** Out of scope for a tabular Subsets asset.
- **Decennial Summary File and 2020 Census special tables.** Future work.
- **Sub-county and tract-level tables.** Future work if there is demand.
- **Monthly granularity of International Trade.** We store annual totals; the monthly timeseries is available upstream if you need it.
- **Intercensal PEP (`pep/int_*`).** Historical-archive-only paths; not refreshed.

## Refresh strategy

On each run:

1. `nodes/catalog_asset.py` fetches `https://api.census.gov/data.json`, enumerates entries through every program selector (`src/census_utils/programs.py`), and writes the `us_census_catalog` registry plus a fingerprint to state (`src/census_utils/catalog.py:catalog_fingerprint`). When the fingerprint matches the prior run, subsequent nodes do not re-download anything they've already cached.
2. Each program node walks the catalog entries, fetches variables.json per (path, vintage), and calls the data endpoint with chunked requests. Raw wide parquet files are cached under `data/raw/` per (program, vintage, geo_level), so subsequent runs only fetch new vintages.
3. `transform()` reads every cached raw parquet for the program, normalizes it (melt to long or project to wide), and `merge()`s into the published subset using a composite key that always includes the vintage/year. Re-running is idempotent and vintage-additive.

## Local development

```bash
cd integrations/us-census-bureau
ENVIRONMENT=dev DATA_DIR=data/dev uv run python src/nodes/catalog_asset.py
ENVIRONMENT=dev DATA_DIR=data/dev uv run python src/nodes/saipe.py
# ... or run everything:
ENVIRONMENT=dev DATA_DIR=data/dev uv run python src/main.py
```

Raw caches live under `data/dev/raw/`, published Delta tables under `data/dev/subsets/`.

## License

All datasets published by this connector are **U.S. Government Work (public domain)**. No attribution required, but the Census Bureau requests that downstream users cite the source program and vintage in any derived publication.
