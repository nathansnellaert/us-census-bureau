"""Connector-specific utilities for US Census Bureau."""

from .constants import (
    CURRENT_YEAR,
    LICENSE,
    SOURCE_URL,
    CATALOG_URL,
    GEOGRAPHY_LEVELS,
    NON_DATA_VARS,
    ACS_TABLE_PREFIXES,
    PEP_POPULATION_MEASURES,
    PEP_COMPONENTS_MEASURES,
    CBP_MEASURES,
    SAIPE_MEASURES,
    SAHIE_MEASURES,
    INTLTRADE_MEASURES,
    ECNBASIC_MEASURES,
    ECNBASIC_VINTAGES,
)
from .catalog import (
    load_catalog,
    catalog_changed,
    save_catalog_fingerprint,
    catalog_fingerprint,
    exact_path_entries,
    prefix_path_entries,
    matching_entries,
)
from .api import fetch_rows, chunked_fetch, decode_values
from .variables import fetch_variable_metadata, estimate_variables, labels_for
from .programs import PROGRAMS

import json
from pathlib import Path

_CATALOG_JSON_PATH = Path(__file__).resolve().parents[2] / "catalog.json"


def load_metadata(subset_id: str) -> dict:
    """Load a published-asset metadata block from catalog.json."""
    data = json.loads(_CATALOG_JSON_PATH.read_text())
    return data["datasets"][subset_id]
