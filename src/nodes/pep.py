"""Population Estimates Program — long-form, multi-vintage, multi-path."""

import pyarrow as pa
from subsets_utils import save_raw_parquet, load_raw_parquet, raw_asset_exists, merge, publish, validate

from census_utils import (
    load_catalog,
    fetch_variable_metadata,
    chunked_fetch,
    GEOGRAPHY_LEVELS,
    NON_DATA_VARS,
    PROGRAMS,
    load_metadata,
)

SUBSET_ID = "us_census_pep"
METADATA = load_metadata(SUBSET_ID)
SELECT = PROGRAMS[SUBSET_ID]["selector"]

# Per-path measurement variable selectors. Each returns a list of variable names
# that are actual numeric measurements (not dimension codes / filters / metadata).
PEP_MEASURE_SELECTORS = {
    "pep/population": lambda var_meta: sorted(
        n for n in var_meta
        if (n.startswith("POP") or n.startswith("DENSITY"))
        and n != "POPGROUP" and not n.startswith("DESC_") and not n.startswith("RANK_")
        and var_meta[n].get("predicateType") in ("int", "float")
        and not var_meta[n].get("predicateOnly")
    ),
    "pep/components": lambda var_meta: [
        n for n in (
            "BIRTHS", "DEATHS", "NATURALINC", "DOMESTICMIG", "INTERNATIONALMIG", "NETMIG",
            "RBIRTH", "RDEATH", "RNATURALINC", "RDOMESTICMIG", "RINTERNATIONALMIG", "RNETMIG",
        ) if n in var_meta
    ],
}


def _measurement_vars(path: str, var_meta: dict) -> list[dict]:
    selector = PEP_MEASURE_SELECTORS.get(path)
    if selector is None:
        return []
    names = selector(var_meta)
    return [{"name": n, "label": var_meta.get(n, {}).get("label", "")} for n in names]


def _raw_id(vintage: int, path: str, geo_level: str) -> str:
    return f"pep_{vintage}_{path.replace('/', '_')}_{geo_level}"


def download():
    catalog = load_catalog()
    entries = SELECT(catalog)
    print(f"[pep] {len(entries)} (path, vintage) entries selected")

    for entry in entries:
        vintage = entry["vintage"]
        path = entry["path"]
        endpoint = entry["api_endpoint"]
        var_meta = fetch_variable_metadata(endpoint)
        variables = _measurement_vars(path, var_meta)
        if not variables:
            print(f"[pep]   {path}/{vintage}: no measurement variables, skipping")
            continue
        var_names = [v["name"] for v in variables]
        # PEP endpoints use either NAME (newer) or GEONAME (older). Detect from var_meta.
        name_var = "NAME" if "NAME" in var_meta else ("GEONAME" if "GEONAME" in var_meta else None)
        print(f"[pep]   {path}/{vintage}: {len(var_names)} variables ({', '.join(var_names[:6])}{'...' if len(var_names) > 6 else ''}) name_var={name_var}")

        for geo_level, (geo, in_clause) in GEOGRAPHY_LEVELS.items():
            raw_id = _raw_id(vintage, path, geo_level)
            if raw_asset_exists(raw_id):
                continue
            try:
                table = chunked_fetch(endpoint, var_names, geo, in_clause, name_var=name_var)
            except RuntimeError as e:
                print(f"[pep]     {geo_level} unavailable: {e}")
                continue
            if table is None:
                continue
            save_raw_parquet(table, raw_id)


def _melt(wide: pa.Table, vintage: int, path: str, geo_level: str, label_map: dict[str, str], measure_names: list[str]) -> pa.Table:
    cols = wide.column_names
    name_source = "NAME" if "NAME" in cols else ("GEONAME" if "GEONAME" in cols else None)
    name_col = wide.column(name_source).to_pylist() if name_source else [""] * wide.num_rows
    state_col = [s or "" for s in (wide.column("state").to_pylist() if "state" in cols else [""] * wide.num_rows)]
    county_col = [c or "" for c in (wide.column("county").to_pylist() if "county" in cols else [""] * wide.num_rows)]

    measure_cols = [m for m in measure_names if m in cols]

    rows = []
    for var in measure_cols:
        col_data = wide.column(var).to_pylist()
        label = label_map.get(var, "")
        for i, value in enumerate(col_data):
            try:
                value_numeric = float(value) if value not in (None, "", "null", "N/A", "-", "*") else None
            except (TypeError, ValueError):
                value_numeric = None
            rows.append({
                "vintage": vintage,
                "path": path,
                "geo_level": geo_level,
                "state_fips": state_col[i],
                "county_fips": county_col[i],
                "geography_name": name_col[i],
                "variable": var,
                "label": label,
                "value": value if value is not None else "",
                "value_numeric": value_numeric,
            })

    schema = pa.schema([
        ("vintage", pa.int32()),
        ("path", pa.string()),
        ("geo_level", pa.string()),
        ("state_fips", pa.string()),
        ("county_fips", pa.string()),
        ("geography_name", pa.string()),
        ("variable", pa.string()),
        ("label", pa.string()),
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
        path = entry["path"]
        endpoint = entry["api_endpoint"]
        var_meta = fetch_variable_metadata(endpoint)
        label_map = {n: info.get("label", "") for n, info in var_meta.items()}
        measure_names = [v["name"] for v in _measurement_vars(path, var_meta)]

        for geo_level in GEOGRAPHY_LEVELS:
            raw_id = _raw_id(vintage, path, geo_level)
            if not raw_asset_exists(raw_id):
                continue
            wide = load_raw_parquet(raw_id)
            frames.append(_melt(wide, vintage, path, geo_level, label_map, measure_names))

    if not frames:
        print(f"[{SUBSET_ID}] no frames")
        return

    out = pa.concat_tables(frames, promote_options="default")
    print(f"[{SUBSET_ID}] merging {out.num_rows:,} rows")

    validate(out, {
        "not_null": ["vintage", "path", "geo_level", "state_fips", "variable"],
        "min_rows": 100,
    })

    merge(out, SUBSET_ID, key=["vintage", "path", "geo_level", "state_fips", "county_fips", "variable"])
    publish(SUBSET_ID, METADATA)


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
