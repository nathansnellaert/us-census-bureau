"""Census API HTTP layer: fetch, chunk, decode."""

from typing import Iterable
import pyarrow as pa
from subsets_utils import get

from .constants import NON_DATA_VARS

_VARS_PER_REQUEST = 49  # API hard-limit is 50, minus NAME


def _chunks(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def fetch_rows(
    api_endpoint: str,
    variables: list[str],
    geography: str,
    in_clause: str | None = None,
    time_param: str | None = None,
    name_var: str | None = "NAME",
) -> list[list] | None:
    """Single Census API call. Returns raw [[header], [row], ...] or None.

    None means legitimately-empty (HTTP 204 or empty/1-row response).
    Any other error is raised. Passes through `variables` exactly; if
    `name_var` is set it is prepended to the `get` clause (default 'NAME';
    pass None to omit, or 'GEONAME' for older PEP endpoints).
    """
    if not variables:
        raise ValueError("variables must be non-empty")
    get_list = ([name_var] if name_var else []) + list(variables)
    params = {
        "get": ",".join(get_list),
        "for": geography,
    }
    if in_clause:
        params["in"] = in_clause
    if time_param:
        params["time"] = time_param
    r = get(api_endpoint, params=params, timeout=120)
    if r.status_code == 204:
        return None
    if r.status_code != 200:
        raise RuntimeError(
            f"Census API {r.status_code} on {api_endpoint} "
            f"vars={variables[:3]}... geo={geography}: {r.text[:200]}"
        )
    data = r.json()
    if not data or len(data) < 2:
        return None
    return data


def chunked_fetch(
    api_endpoint: str,
    variables: list[str],
    geography: str,
    in_clause: str | None = None,
    time_param: str | None = None,
    name_var: str | None = "NAME",
) -> pa.Table | None:
    """Fetch variables in chunks of 49, concat into one wide PyArrow table.

    All chunks share geography/time columns. Variable columns merged across
    chunks (unioned), geography rows kept stable by position (each chunk
    returns the same set of geographies in the same order).
    """
    if not variables:
        return None

    all_chunks: list[pa.Table] = []
    geo_cols: list[str] | None = None

    for chunk_vars in _chunks(variables, _VARS_PER_REQUEST):
        raw = fetch_rows(api_endpoint, chunk_vars, geography, in_clause, time_param, name_var=name_var)
        if raw is None:
            return None
        header = raw[0]
        body = raw[1:]

        cols: dict[str, list] = {h: [] for h in header}
        for row in body:
            for i, h in enumerate(header):
                cols[h].append(row[i])

        table = pa.table({h: cols[h] for h in header})
        all_chunks.append(table)

        if geo_cols is None:
            geo_label_set = {"NAME", "GEONAME", "state", "county", "us", "time"}
            if name_var:
                geo_label_set.add(name_var)
            geo_cols = [h for h in header if h in geo_label_set or h == geography.split(":")[0]]

    if len(all_chunks) == 1:
        return all_chunks[0]

    base = all_chunks[0]
    for extra in all_chunks[1:]:
        data_only = [c for c in extra.column_names if c not in geo_cols]
        for col in data_only:
            base = base.append_column(col, extra.column(col))
    return base


def decode_values(code: str, values_map: dict[str, str]) -> str:
    """Map a string code like '001' to its labeled value using variables.json values dict."""
    if code is None:
        return None
    return values_map.get(code, code)
