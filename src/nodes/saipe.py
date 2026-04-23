"""Small Area Income and Poverty Estimates (SAIPE) — timeseries, multi-year."""

from datetime import datetime
import pyarrow as pa
from subsets_utils import save_raw_parquet, load_raw_parquet, raw_asset_exists, merge, publish, validate

from census_utils import (
    fetch_rows,
    GEOGRAPHY_LEVELS,
    SAIPE_MEASURES,
    load_metadata,
    parse_numeric,
)

SUBSET_ID = "us_census_saipe"
METADATA = load_metadata(SUBSET_ID)
ENDPOINT = "https://api.census.gov/data/timeseries/poverty/saipe"
MIN_YEAR = 2005


def _raw_id(year: int, geo_level: str) -> str:
    return f"saipe_{year}_{geo_level}"


def download():
    current = datetime.now().year
    print(f"[saipe] fetching {MIN_YEAR}-{current - 1}")
    for year in range(MIN_YEAR, current):
        for geo_level, (geo, in_clause) in GEOGRAPHY_LEVELS.items():
            raw_id = _raw_id(year, geo_level)
            if raw_asset_exists(raw_id):
                continue
            try:
                raw = fetch_rows(ENDPOINT, SAIPE_MEASURES, geo, in_clause, time_param=str(year))
            except RuntimeError as e:
                print(f"[saipe]   {year}/{geo_level} {e}")
                continue
            if raw is None:
                print(f"[saipe]   {year}/{geo_level} no data")
                continue
            header = raw[0]
            cols = {h: [] for h in header}
            for row in raw[1:]:
                for i, h in enumerate(header):
                    cols[h].append(row[i])
            save_raw_parquet(pa.table(cols), raw_id)
            print(f"[saipe]   {year}/{geo_level} {len(raw) - 1:,} rows")


def _normalize(wide: pa.Table, year: int, geo_level: str) -> pa.Table:
    cols = wide.column_names
    name_col = wide.column("NAME").to_pylist()
    state_col = [s or "" for s in (wide.column("state").to_pylist() if "state" in cols else [""] * wide.num_rows)]
    county_col = [c or "" for c in (wide.column("county").to_pylist() if "county" in cols else [""] * wide.num_rows)]

    def measure(name):
        if name in cols:
            return [parse_numeric(v) for v in wide.column(name).to_pylist()]
        return [None] * wide.num_rows

    rows = []
    measures = {m.lower(): measure(m) for m in SAIPE_MEASURES}
    for i in range(wide.num_rows):
        rows.append({
            "year": year,
            "geo_level": geo_level,
            "state_fips": state_col[i],
            "county_fips": county_col[i],
            "geography_name": name_col[i],
            **{k: v[i] for k, v in measures.items()},
        })

    schema = pa.schema([
        ("year", pa.int32()),
        ("geo_level", pa.string()),
        ("state_fips", pa.string()),
        ("county_fips", pa.string()),
        ("geography_name", pa.string()),
    ] + [(m.lower(), pa.float64()) for m in SAIPE_MEASURES])
    return pa.Table.from_pylist(rows, schema=schema)


def transform():
    current = datetime.now().year
    frames: list[pa.Table] = []
    for year in range(MIN_YEAR, current):
        for geo_level in GEOGRAPHY_LEVELS:
            raw_id = _raw_id(year, geo_level)
            if not raw_asset_exists(raw_id):
                continue
            wide = load_raw_parquet(raw_id)
            frames.append(_normalize(wide, year, geo_level))

    if not frames:
        print(f"[{SUBSET_ID}] no frames")
        return

    out = pa.concat_tables(frames, promote_options="default")
    print(f"[{SUBSET_ID}] merging {out.num_rows:,} rows")

    validate(out, {
        "not_null": ["year", "geo_level", "state_fips", "geography_name"],
        "min_rows": 100,
    })

    merge(out, SUBSET_ID, key=["year", "geo_level", "state_fips", "county_fips"])
    publish(SUBSET_ID, METADATA)


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
