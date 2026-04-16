"""Variable discovery + label lookup from Census variables.json endpoints."""

from subsets_utils import get

from .constants import NON_DATA_VARS

_variable_cache: dict[str, dict] = {}


def fetch_variable_metadata(api_endpoint: str) -> dict:
    """Fetch <endpoint>/variables.json for a dataset, cache per-process."""
    if api_endpoint in _variable_cache:
        return _variable_cache[api_endpoint]
    url = api_endpoint.rstrip("/") + "/variables.json"
    r = get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"variables.json fetch failed {r.status_code}: {url}")
    payload = r.json()
    meta = payload.get("variables", {})
    _variable_cache[api_endpoint] = meta
    return meta


def estimate_variables(meta: dict, table_groups: list[str]) -> list[dict]:
    """Return ACS estimate variables (name ends in 'E') belonging to given table groups.

    Skips margin-of-error (`M`), annotation (`EA`/`MA`), and predicate-only rows.
    Each entry: {name, label, group}.
    """
    wanted = set(table_groups)
    out: list[dict] = []
    for name, info in meta.items():
        if name in NON_DATA_VARS:
            continue
        if info.get("predicateOnly"):
            continue
        grp = info.get("group")
        if grp not in wanted:
            continue
        if not name.endswith("E"):
            continue
        if name.endswith("EA") or name.endswith("MA"):
            continue
        out.append({"name": name, "label": info.get("label", ""), "group": grp})
    out.sort(key=lambda v: v["name"])
    return out


def labels_for(meta: dict, names: list[str]) -> dict[str, str]:
    """Return {name: label} for the given variable names."""
    return {n: meta.get(n, {}).get("label", "") for n in names}
