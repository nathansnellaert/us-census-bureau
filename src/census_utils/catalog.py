"""Census data.json catalog: fetching, filtering, vintage enumeration."""

import hashlib
import json
from subsets_utils import get, load_state, save_state

from .constants import CATALOG_URL

_catalog_cache: dict | None = None


def load_catalog() -> dict:
    """Fetch https://api.census.gov/data.json once per process."""
    global _catalog_cache
    if _catalog_cache is None:
        r = get(CATALOG_URL, timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"Census catalog fetch failed: {r.status_code}")
        _catalog_cache = r.json()
    return _catalog_cache


def catalog_fingerprint(catalog: dict) -> str:
    """MD5 over the sorted list of (identifier, modified) pairs."""
    pairs = sorted(
        (ds.get("identifier", ""), ds.get("modified", ""))
        for ds in catalog.get("dataset", [])
    )
    return hashlib.md5(json.dumps(pairs).encode()).hexdigest()


def catalog_changed() -> tuple[dict, bool]:
    """Return (catalog, changed_since_last_run)."""
    catalog = load_catalog()
    fp = catalog_fingerprint(catalog)
    prev = load_state("census_catalog").get("fingerprint")
    changed = fp != prev
    return catalog, changed


def save_catalog_fingerprint(catalog: dict) -> None:
    save_state("census_catalog", {"fingerprint": catalog_fingerprint(catalog)})


def _path_of(ds: dict) -> str:
    return "/".join(ds.get("c_dataset", []))


def _api_endpoint(ds: dict) -> str | None:
    for dist in ds.get("distribution", []):
        if dist.get("format") == "API":
            url = dist.get("accessURL") or ""
            if url.startswith("http://"):
                url = "https://" + url[7:]
            return url
    return None


def matching_entries(
    catalog: dict,
    path_predicate,
    vintage_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Return simplified catalog entries whose c_dataset path matches.

    Each entry has: {path, vintage, api_endpoint, variables_link, title, modified, identifier}.
    Filtered by vintage range if provided; unavailable datasets dropped.
    """
    out: list[dict] = []
    for ds in catalog.get("dataset", []):
        if not ds.get("c_isAvailable", True):
            continue
        path = _path_of(ds)
        if not path_predicate(path):
            continue
        vintage = ds.get("c_vintage")
        if vintage_range is not None and vintage is not None:
            lo, hi = vintage_range
            if vintage < lo or vintage > hi:
                continue
        api = _api_endpoint(ds)
        if not api:
            continue
        out.append({
            "path": path,
            "vintage": vintage,
            "api_endpoint": api,
            "variables_link": ds.get("c_variablesLink"),
            "title": ds.get("title", ""),
            "modified": ds.get("modified", ""),
            "identifier": ds.get("identifier", ""),
        })
    out.sort(key=lambda e: (e["path"], e["vintage"] or 0))
    return out


def exact_path_entries(catalog: dict, path: str, vintage_range: tuple[int, int] | None = None) -> list[dict]:
    return matching_entries(catalog, lambda p: p == path, vintage_range)


def prefix_path_entries(catalog: dict, prefix: str, vintage_range: tuple[int, int] | None = None) -> list[dict]:
    return matching_entries(catalog, lambda p: p == prefix or p.startswith(prefix + "/"), vintage_range)
