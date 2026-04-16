"""Economic Census basic summary statistics — vintages 2017 and 2022, US national."""

import pyarrow as pa
from subsets_utils import save_raw_parquet, load_raw_parquet, raw_asset_exists, merge, publish, get

from census_utils import (
    load_catalog,
    fetch_variable_metadata,
    ECNBASIC_MEASURES,
    PROGRAMS,
    load_metadata,
)

SUBSET_ID = "us_census_ecnbasic"
METADATA = load_metadata(SUBSET_ID)
SELECT = PROGRAMS[SUBSET_ID]["selector"]


def _naics_dim(var_meta: dict, vintage: int) -> str:
    candidates = (f"NAICS{vintage}", "NAICS2022", "NAICS2017", "NAICS2012")
    for c in candidates:
        if c in var_meta:
            return c
    raise RuntimeError(f"No NAICS dim for ecnbasic {vintage}: {sorted(var_meta)[:20]}")


def _raw_id(vintage: int) -> str:
    return f"ecnbasic_{vintage}"


def _fetch_us(endpoint: str, get_vars: list[str]) -> list[list] | None:
    params = {
        "get": ",".join(get_vars),
        "for": "us:*",
    }
    r = get(endpoint, params=params, timeout=180)
    if r.status_code == 204:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"ecnbasic: {r.status_code} {r.text[:200]}")
    data = r.json()
    if not data or len(data) < 2:
        return None
    return data


def download():
    catalog = load_catalog()
    entries = SELECT(catalog)
    print(f"[ecnbasic] {len(entries)} vintages selected")

    for entry in entries:
        vintage = entry["vintage"]
        endpoint = entry["api_endpoint"]
        var_meta = fetch_variable_metadata(endpoint)
        naics_dim = _naics_dim(var_meta, vintage)
        present_measures = [m for m in ECNBASIC_MEASURES if m in var_meta]
        if not present_measures:
            print(f"[ecnbasic] {vintage}: no measures present")
            continue
        get_vars = present_measures + [naics_dim]

        raw_id = _raw_id(vintage)
        if raw_asset_exists(raw_id):
            print(f"[ecnbasic]   {vintage} cached")
            continue
        try:
            raw = _fetch_us(endpoint, get_vars)
        except RuntimeError as e:
            print(f"[ecnbasic]   {vintage} {e}")
            continue
        if raw is None:
            print(f"[ecnbasic]   {vintage} no data")
            continue
        header = raw[0]
        cols = {h: [] for h in header}
        for row in raw[1:]:
            for i, h in enumerate(header):
                cols[h].append(row[i])
        save_raw_parquet(pa.table(cols), raw_id)
        print(f"[ecnbasic]   {vintage} {len(raw) - 1:,} rows")


def _normalize(wide: pa.Table, vintage: int) -> pa.Table:
    cols = wide.column_names
    naics_col = next((c for c in cols if c.startswith("NAICS") and not c.endswith("_LABEL")), None)

    naics_vals = wide.column(naics_col).to_pylist()

    def _num(v):
        try:
            return float(v) if v not in (None, "", "null", "N/A", "-", "*") else None
        except (TypeError, ValueError):
            return None

    measures = {m: wide.column(m).to_pylist() if m in cols else [None] * wide.num_rows for m in ECNBASIC_MEASURES}

    rows = []
    for i in range(wide.num_rows):
        rows.append({
            "vintage": vintage,
            "naics": naics_vals[i],
            "estab": _num(measures["ESTAB"][i]),
            "emp": _num(measures["EMP"][i]),
            "payann": _num(measures["PAYANN"][i]),
            "rcptot": _num(measures["RCPTOT"][i]),
        })

    schema = pa.schema([
        ("vintage", pa.int32()),
        ("naics", pa.string()),
        ("estab", pa.float64()),
        ("emp", pa.float64()),
        ("payann", pa.float64()),
        ("rcptot", pa.float64()),
    ])
    return pa.Table.from_pylist(rows, schema=schema)


def transform():
    catalog = load_catalog()
    entries = SELECT(catalog)
    frames: list[pa.Table] = []
    for entry in entries:
        vintage = entry["vintage"]
        raw_id = _raw_id(vintage)
        if not raw_asset_exists(raw_id):
            continue
        wide = load_raw_parquet(raw_id)
        frames.append(_normalize(wide, vintage))

    if not frames:
        print(f"[{SUBSET_ID}] no frames")
        return

    out = pa.concat_tables(frames, promote_options="default")
    print(f"[{SUBSET_ID}] merging {out.num_rows:,} rows")
    merge(out, SUBSET_ID, key=["vintage", "naics"])
    publish(SUBSET_ID, METADATA)


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
