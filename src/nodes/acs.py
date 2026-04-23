"""American Community Survey 1-Year Detailed Tables — all in-scope vintages."""

import pyarrow as pa
from subsets_utils import save_raw_parquet, load_raw_parquet, raw_asset_exists, merge, publish, validate

from census_utils import (
    load_catalog,
    fetch_variable_metadata,
    estimate_variables,
    chunked_fetch,
    ACS_TABLE_PREFIXES,
    GEOGRAPHY_LEVELS,
    PROGRAMS,
    load_metadata,
)

SUBSET_ID = "us_census_acs"
METADATA = load_metadata(SUBSET_ID)
SELECT = PROGRAMS[SUBSET_ID]["selector"]


def _raw_id(vintage: int, geo_level: str) -> str:
    return f"acs_acs1_{vintage}_{geo_level}"


def download():
    catalog = load_catalog()
    entries = SELECT(catalog)
    print(f"[acs] {len(entries)} vintage entries selected")

    for entry in entries:
        vintage = entry["vintage"]
        endpoint = entry["api_endpoint"]
        var_meta = fetch_variable_metadata(endpoint)
        variables = estimate_variables(var_meta, ACS_TABLE_PREFIXES)
        if not variables:
            print(f"[acs] {vintage}: no priority variables present, skipping")
            continue
        var_names = [v["name"] for v in variables]
        print(f"[acs] {vintage}: {len(var_names)} variables, {len(GEOGRAPHY_LEVELS)} geo levels")

        for geo_level, (geo, in_clause) in GEOGRAPHY_LEVELS.items():
            raw_id = _raw_id(vintage, geo_level)
            if raw_asset_exists(raw_id):
                print(f"[acs]   {vintage}/{geo_level} already cached")
                continue
            table = chunked_fetch(endpoint, var_names, geo, in_clause)
            if table is None:
                print(f"[acs]   {vintage}/{geo_level} returned no rows")
                continue
            save_raw_parquet(table, raw_id)


def _melt(wide: pa.Table, vintage: int, geo_level: str, label_map: dict[str, str], group_map: dict[str, str]) -> pa.Table:
    name_col = wide.column("NAME").to_pylist()
    state_col = [s or "" for s in (wide.column("state").to_pylist() if "state" in wide.column_names else [""] * wide.num_rows)]
    county_col = [c or "" for c in (wide.column("county").to_pylist() if "county" in wide.column_names else [""] * wide.num_rows)]

    skip = {"NAME", "state", "county", "us"}
    var_cols = [c for c in wide.column_names if c not in skip]

    rows = []
    for var in var_cols:
        col_data = wide.column(var).to_pylist()
        label = label_map.get(var, "")
        group = group_map.get(var, "")
        for i, value in enumerate(col_data):
            try:
                value_numeric = float(value) if value not in (None, "", "null", "N/A", "-", "*") else None
            except (TypeError, ValueError):
                value_numeric = None
            rows.append({
                "vintage": vintage,
                "geo_level": geo_level,
                "state_fips": state_col[i],
                "county_fips": county_col[i],
                "geography_name": name_col[i],
                "variable": var,
                "label": label,
                "table_id": group,
                "value": value if value is not None else "",
                "value_numeric": value_numeric,
            })

    schema = pa.schema([
        ("vintage", pa.int32()),
        ("geo_level", pa.string()),
        ("state_fips", pa.string()),
        ("county_fips", pa.string()),
        ("geography_name", pa.string()),
        ("variable", pa.string()),
        ("label", pa.string()),
        ("table_id", pa.string()),
        ("value", pa.string()),
        ("value_numeric", pa.float64()),
    ])
    return pa.Table.from_pylist(rows, schema=schema)


def transform():
    catalog = load_catalog()
    entries = SELECT(catalog)

    frames: list[pa.Table] = []
    for entry in entries:
        vintage = entry["vintage"]
        endpoint = entry["api_endpoint"]
        var_meta = fetch_variable_metadata(endpoint)
        variables = estimate_variables(var_meta, ACS_TABLE_PREFIXES)
        label_map = {v["name"]: v["label"] for v in variables}
        group_map = {v["name"]: v["group"] for v in variables}

        for geo_level in GEOGRAPHY_LEVELS:
            raw_id = _raw_id(vintage, geo_level)
            if not raw_asset_exists(raw_id):
                continue
            wide = load_raw_parquet(raw_id)
            frames.append(_melt(wide, vintage, geo_level, label_map, group_map))

    if not frames:
        print(f"[{SUBSET_ID}] no frames to merge")
        return

    out = pa.concat_tables(frames, promote_options="default")
    print(f"[{SUBSET_ID}] merging {out.num_rows:,} rows")

    validate(out, {
        "not_null": ["vintage", "geo_level", "state_fips", "variable"],
        "min_rows": 100,
    })

    merge(out, SUBSET_ID, key=["vintage", "geo_level", "state_fips", "county_fips", "variable"])
    publish(SUBSET_ID, METADATA)


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
