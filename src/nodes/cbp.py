"""County Business Patterns — all in-scope vintages, state-level only.

CBP also exposes a county-level cut, but it's an order of magnitude larger
(~3M rows per vintage with NAICS detail) and the Census API rate-limits
single-state-list county queries; we restrict this asset to state level so it
runs in minutes instead of hours. County-level CBP is a follow-up.
"""

import pyarrow as pa
from subsets_utils import save_raw_parquet, load_raw_parquet, raw_asset_exists, merge, publish, validate

from census_utils import (
    load_catalog,
    fetch_variable_metadata,
    fetch_rows,
    CBP_MEASURES,
    PROGRAMS,
    load_metadata,
    parse_numeric,
)

# All state FIPS codes (50 + DC). PR is excluded because CBP coverage is uneven.
STATE_FIPS = [
    "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19",
    "20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35",
    "36","37","38","39","40","41","42","44","45","46","47","48","49","50","51","53",
    "54","55","56",
]

SUBSET_ID = "us_census_cbp"
METADATA = load_metadata(SUBSET_ID)
SELECT = PROGRAMS[SUBSET_ID]["selector"]


def _naics_dim(var_meta: dict) -> str:
    for candidate in ("NAICS2022", "NAICS2017", "NAICS2012", "NAICS2007", "NAICS2002"):
        if candidate in var_meta:
            return candidate
    raise RuntimeError(f"No NAICS dimension found in CBP variables: {sorted(var_meta)[:20]}")


def _raw_id(vintage: int, geo_level: str) -> str:
    return f"cbp_{vintage}_{geo_level}"


def _rows_to_table(raw: list[list]) -> pa.Table:
    header = raw[0]
    # Census API can echo a `get=` variable twice when the same name is also
    # used as a predicate; keep only the first occurrence of each column name.
    seen: dict[str, int] = {}
    for i, h in enumerate(header):
        seen.setdefault(h, i)
    keep_idx = list(seen.values())
    keep_names = [header[i] for i in keep_idx]
    cols = {h: [] for h in keep_names}
    for row in raw[1:]:
        for h, i in zip(keep_names, keep_idx):
            cols[h].append(row[i])
    return pa.table(cols)


def download():
    catalog = load_catalog()
    entries = SELECT(catalog)
    print(f"[cbp] {len(entries)} vintages selected")

    for entry in entries:
        vintage = entry["vintage"]
        endpoint = entry["api_endpoint"]
        var_meta = fetch_variable_metadata(endpoint)
        naics_dim = _naics_dim(var_meta)
        present_measures = [m for m in CBP_MEASURES if m in var_meta]
        if not present_measures:
            print(f"[cbp] {vintage}: no CBP measures present, skipping")
            continue
        # NAICS_LABEL is a Census "companion" variable — it works even when
        # not listed in variables.json, so always request it.
        get_vars = present_measures + [naics_dim, f"{naics_dim}_LABEL"]

        state_id = _raw_id(vintage, "state")
        if raw_asset_exists(state_id):
            print(f"[cbp]   {vintage}/state cached")
            continue
        raw = fetch_rows(endpoint, get_vars, "state:*")
        if raw is None:
            print(f"[cbp]   {vintage}/state no rows")
            continue
        save_raw_parquet(_rows_to_table(raw), state_id)
        print(f"[cbp]   {vintage}/state {len(raw) - 1:,} rows")


def _normalize(wide: pa.Table, vintage: int, geo_level: str) -> pa.Table:
    cols = wide.column_names
    naics_col = next((c for c in cols if c.startswith("NAICS") and not c.endswith("_LABEL")), None)
    naics_label_col = next((c for c in cols if c.startswith("NAICS") and c.endswith("_LABEL")), None)

    name_col = wide.column("NAME").to_pylist()
    state_col = [s or "" for s in (wide.column("state").to_pylist() if "state" in cols else [""] * wide.num_rows)]
    naics_vals = [n or "" for n in wide.column(naics_col).to_pylist()]
    naics_labels = wide.column(naics_label_col).to_pylist() if naics_label_col else [None] * wide.num_rows

    measures = {m: wide.column(m).to_pylist() if m in cols else [None] * wide.num_rows for m in CBP_MEASURES}

    rows = []
    for i in range(wide.num_rows):
        rows.append({
            "vintage": vintage,
            "state_fips": state_col[i],
            "geography_name": name_col[i],
            "naics": naics_vals[i],
            "naics_label": naics_labels[i],
            "emp": parse_numeric(measures["EMP"][i]),
            "payann": parse_numeric(measures["PAYANN"][i]),
            "estab": parse_numeric(measures["ESTAB"][i]),
            "payqtr1": parse_numeric(measures["PAYQTR1"][i]),
        })

    schema = pa.schema([
        ("vintage", pa.int32()),
        ("state_fips", pa.string()),
        ("geography_name", pa.string()),
        ("naics", pa.string()),
        ("naics_label", pa.string()),
        ("emp", pa.float64()),
        ("payann", pa.float64()),
        ("estab", pa.float64()),
        ("payqtr1", pa.float64()),
    ])
    return pa.Table.from_pylist(rows, schema=schema)


def transform():
    catalog = load_catalog()
    entries = SELECT(catalog)
    frames: list[pa.Table] = []
    for entry in entries:
        vintage = entry["vintage"]
        raw_id = _raw_id(vintage, "state")
        if not raw_asset_exists(raw_id):
            continue
        wide = load_raw_parquet(raw_id)
        frames.append(_normalize(wide, vintage, "state"))

    if not frames:
        print(f"[{SUBSET_ID}] no frames")
        return

    out = pa.concat_tables(frames, promote_options="default")
    print(f"[{SUBSET_ID}] merging {out.num_rows:,} rows")

    validate(out, {
        "not_null": ["vintage", "state_fips", "naics"],
        "min_rows": 100,
    })

    merge(out, SUBSET_ID, key=["vintage", "state_fips", "naics"])
    publish(SUBSET_ID, METADATA)


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
