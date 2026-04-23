"""Small Area Health Insurance Estimates (SAHIE) — timeseries, multi-year.

Filters AGECAT/RACECAT/SEXCAT/IPRCAT to the 'all' bucket so each row is the
total population for that geography-year.
"""

from datetime import datetime
import pyarrow as pa
from subsets_utils import save_raw_parquet, load_raw_parquet, raw_asset_exists, merge, publish, validate, get

from census_utils import (
    fetch_rows,
    GEOGRAPHY_LEVELS,
    SAHIE_MEASURES,
    load_metadata,
    parse_numeric,
)

SUBSET_ID = "us_census_sahie"
METADATA = load_metadata(SUBSET_ID)
ENDPOINT = "https://api.census.gov/data/timeseries/healthins/sahie"
MIN_YEAR = 2008

# Filter dimensions to "all" buckets so each row is the population total.
FILTERS = {
    "AGECAT": "0",
    "RACECAT": "0",
    "SEXCAT": "0",
    "IPRCAT": "0",
}


def _raw_id(year: int, geo_level: str) -> str:
    return f"sahie_{year}_{geo_level}"


def _fetch(year: int, geo: str, in_clause: str | None):
    params = {
        "get": "NAME," + ",".join(SAHIE_MEASURES),
        "for": geo,
        "time": str(year),
        **FILTERS,
    }
    if in_clause:
        params["in"] = in_clause
    r = get(ENDPOINT, params=params, timeout=120)
    if r.status_code == 204:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"SAHIE {year} {geo}: {r.status_code} {r.text[:200]}")
    data = r.json()
    if not data or len(data) < 2:
        return None
    return data


def download():
    current = datetime.now().year
    print(f"[sahie] fetching {MIN_YEAR}-{current - 1}")
    for year in range(MIN_YEAR, current):
        for geo_level, (geo, in_clause) in GEOGRAPHY_LEVELS.items():
            raw_id = _raw_id(year, geo_level)
            if raw_asset_exists(raw_id):
                continue
            try:
                raw = _fetch(year, geo, in_clause)
            except RuntimeError as e:
                print(f"[sahie]   {year}/{geo_level} {e}")
                continue
            if raw is None:
                print(f"[sahie]   {year}/{geo_level} no data")
                continue
            header = raw[0]
            cols = {h: [] for h in header}
            for row in raw[1:]:
                for i, h in enumerate(header):
                    cols[h].append(row[i])
            save_raw_parquet(pa.table(cols), raw_id)
            print(f"[sahie]   {year}/{geo_level} {len(raw) - 1:,} rows")


def _normalize(wide: pa.Table, year: int, geo_level: str) -> pa.Table:
    cols = wide.column_names
    name_col = wide.column("NAME").to_pylist()
    state_col = [s or "" for s in (wide.column("state").to_pylist() if "state" in cols else [""] * wide.num_rows)]
    county_col = [c or "" for c in (wide.column("county").to_pylist() if "county" in cols else [""] * wide.num_rows)]

    def measure(name):
        if name in cols:
            return [parse_numeric(v) for v in wide.column(name).to_pylist()]
        return [None] * wide.num_rows

    measures = {m.lower(): measure(m) for m in SAHIE_MEASURES}
    rows = []
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
    ] + [(m.lower(), pa.float64()) for m in SAHIE_MEASURES])
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
