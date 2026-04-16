"""US International Trade — annual totals by 1-digit End-Use code, both flows."""

from datetime import datetime
import pyarrow as pa
from subsets_utils import save_raw_parquet, load_raw_parquet, raw_asset_exists, merge, publish, get

from census_utils import load_metadata

SUBSET_ID = "us_census_intltrade_annual"
METADATA = load_metadata(SUBSET_ID)

IMPORTS_ENDPOINT = "https://api.census.gov/data/timeseries/intltrade/imports/enduse"
EXPORTS_ENDPOINT = "https://api.census.gov/data/timeseries/intltrade/exports/enduse"
MIN_YEAR = 2013


def _fetch_year(endpoint: str, get_vars: list[str], year: int) -> list[list] | None:
    params = {
        "get": ",".join(get_vars),
        "time": f"{year}-12",
        "COMM_LVL": "EU1",
    }
    r = get(endpoint, params=params, timeout=120)
    if r.status_code == 204:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"intltrade {endpoint} {year}: {r.status_code} {r.text[:200]}")
    data = r.json()
    if not data or len(data) < 2:
        return None
    return data


def _raw_id(flow: str, year: int) -> str:
    return f"intltrade_{flow}_{year}"


def download():
    current = datetime.now().year
    print(f"[intltrade] fetching {MIN_YEAR}-{current - 1}")
    for year in range(MIN_YEAR, current):
        for flow, endpoint, get_vars in [
            ("imports", IMPORTS_ENDPOINT, ["I_ENDUSE", "I_ENDUSE_LDESC", "GEN_VAL_YR", "CON_VAL_YR"]),
            ("exports", EXPORTS_ENDPOINT, ["E_ENDUSE", "E_ENDUSE_LDESC", "ALL_VAL_YR"]),
        ]:
            raw_id = _raw_id(flow, year)
            if raw_asset_exists(raw_id):
                continue
            try:
                raw = _fetch_year(endpoint, get_vars, year)
            except RuntimeError as e:
                print(f"[intltrade]   {flow}/{year} {e}")
                continue
            if raw is None:
                print(f"[intltrade]   {flow}/{year} no data")
                continue
            header = raw[0]
            cols = {h: [] for h in header}
            for row in raw[1:]:
                for i, h in enumerate(header):
                    cols[h].append(row[i])
            save_raw_parquet(pa.table(cols), raw_id)
            print(f"[intltrade]   {flow}/{year} {len(raw) - 1} rows")


def _to_long(wide: pa.Table, flow: str, year: int) -> pa.Table:
    cols = wide.column_names
    if flow == "imports":
        enduse_col = wide.column("I_ENDUSE").to_pylist()
        label_col = wide.column("I_ENDUSE_LDESC").to_pylist() if "I_ENDUSE_LDESC" in cols else [""] * wide.num_rows
        measures = {
            "general_imports": wide.column("GEN_VAL_YR").to_pylist() if "GEN_VAL_YR" in cols else [None] * wide.num_rows,
            "consumption_imports": wide.column("CON_VAL_YR").to_pylist() if "CON_VAL_YR" in cols else [None] * wide.num_rows,
        }
    else:
        enduse_col = wide.column("E_ENDUSE").to_pylist()
        label_col = wide.column("E_ENDUSE_LDESC").to_pylist() if "E_ENDUSE_LDESC" in cols else [""] * wide.num_rows
        measures = {
            "total_exports": wide.column("ALL_VAL_YR").to_pylist() if "ALL_VAL_YR" in cols else [None] * wide.num_rows,
        }

    def _num(v):
        try:
            return float(v) if v not in (None, "", "null", "N/A", "-", "*") else None
        except (TypeError, ValueError):
            return None

    rows = []
    for measure_name, vals in measures.items():
        for i, raw_val in enumerate(vals):
            rows.append({
                "year": year,
                "flow": flow,
                "enduse": enduse_col[i],
                "enduse_label": label_col[i],
                "measure": measure_name,
                "value_usd": _num(raw_val),
            })

    schema = pa.schema([
        ("year", pa.int32()),
        ("flow", pa.string()),
        ("enduse", pa.string()),
        ("enduse_label", pa.string()),
        ("measure", pa.string()),
        ("value_usd", pa.float64()),
    ])
    return pa.Table.from_pylist(rows, schema=schema)


def transform():
    current = datetime.now().year
    frames: list[pa.Table] = []
    for year in range(MIN_YEAR, current):
        for flow in ("imports", "exports"):
            raw_id = _raw_id(flow, year)
            if not raw_asset_exists(raw_id):
                continue
            wide = load_raw_parquet(raw_id)
            frames.append(_to_long(wide, flow, year))

    if not frames:
        print(f"[{SUBSET_ID}] no frames")
        return

    out = pa.concat_tables(frames, promote_options="default")
    print(f"[{SUBSET_ID}] merging {out.num_rows:,} rows")
    merge(out, SUBSET_ID, key=["year", "flow", "enduse", "measure"])
    publish(SUBSET_ID, METADATA)


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
