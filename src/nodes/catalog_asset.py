"""Publishes us_census_catalog: the registry of (subset_id, path, vintage)
entries this connector actually ingests.

Built by re-running every program selector against the live Census data.json
catalog, so the registry can never drift from what the per-program nodes
publish.
"""

import pyarrow as pa
from subsets_utils import overwrite, publish

from census_utils import (
    load_catalog,
    catalog_fingerprint,
    save_catalog_fingerprint,
    PROGRAMS,
    load_metadata,
)

SUBSET_ID = "us_census_catalog"
METADATA = load_metadata(SUBSET_ID)


def download():
    """No raw artifact; the catalog is in-memory and shared across nodes."""
    catalog = load_catalog()
    print(f"Census data.json catalog: {len(catalog.get('dataset', []))} entries, "
          f"fingerprint={catalog_fingerprint(catalog)[:12]}")


def transform():
    catalog = load_catalog()

    rows: list[dict] = []
    for subset_id, spec in PROGRAMS.items():
        for entry in spec["selector"](catalog):
            rows.append({
                "subset_id": subset_id,
                "program": spec["program"],
                "path": entry["path"],
                "vintage": entry["vintage"],
                "title": entry["title"],
                "api_endpoint": entry["api_endpoint"],
                "variables_link": entry["variables_link"],
                "modified": entry["modified"],
                "identifier": entry["identifier"],
            })

    if not rows:
        print(f"[{SUBSET_ID}] no catalog entries selected — nothing to publish")
        return

    schema = pa.schema([
        ("subset_id", pa.string()),
        ("program", pa.string()),
        ("path", pa.string()),
        ("vintage", pa.int32()),
        ("title", pa.string()),
        ("api_endpoint", pa.string()),
        ("variables_link", pa.string()),
        ("modified", pa.string()),
        ("identifier", pa.string()),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    overwrite(table, SUBSET_ID)
    publish(SUBSET_ID, METADATA)
    save_catalog_fingerprint(catalog)
    print(f"[{SUBSET_ID}] published {len(rows)} registry rows")


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
