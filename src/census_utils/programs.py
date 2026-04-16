"""Per-program catalog selectors.

Each function takes the raw Census data.json catalog dict and returns the
in-scope entries (as produced by census_utils.catalog.matching_entries) for
that program. Used by both per-program node files and by the catalog asset
node so the registry stays in sync with what's actually published.
"""

from .constants import CURRENT_YEAR
from .catalog import exact_path_entries, prefix_path_entries

ACS_MIN_VINTAGE = 2010
CBP_MIN_VINTAGE = 2012
PEP_MIN_VINTAGE = 2015
ECNBASIC_VINTAGES = (2017, 2022)


def acs_entries(catalog: dict) -> list[dict]:
    return exact_path_entries(catalog, "acs/acs1", (ACS_MIN_VINTAGE, CURRENT_YEAR))


def cbp_entries(catalog: dict) -> list[dict]:
    return exact_path_entries(catalog, "cbp", (CBP_MIN_VINTAGE, CURRENT_YEAR))


PEP_INCLUDED_PATHS = {
    "pep/population",
    "pep/components",
}


def pep_entries(catalog: dict) -> list[dict]:
    """Whitelisted pep/* sub-paths that expose state/county measurement data.

    Character-breakdown paths (pep/charage, pep/charagegroups, pep/subcty, …)
    use different geography hierarchies or carry dimensional columns as rows;
    they're excluded so the published asset stays cleanly state/county-grained.
    """
    return [
        e for e in prefix_path_entries(catalog, "pep", (PEP_MIN_VINTAGE, CURRENT_YEAR))
        if e["path"] in PEP_INCLUDED_PATHS
    ]


def saipe_entries(catalog: dict) -> list[dict]:
    return exact_path_entries(catalog, "timeseries/poverty/saipe")


def sahie_entries(catalog: dict) -> list[dict]:
    return exact_path_entries(catalog, "timeseries/healthins/sahie")


def intltrade_imports_entries(catalog: dict) -> list[dict]:
    return exact_path_entries(catalog, "timeseries/intltrade/imports/enduse")


def intltrade_exports_entries(catalog: dict) -> list[dict]:
    return exact_path_entries(catalog, "timeseries/intltrade/exports/enduse")


def ecnbasic_entries(catalog: dict) -> list[dict]:
    return [e for e in exact_path_entries(catalog, "ecnbasic") if e["vintage"] in ECNBASIC_VINTAGES]


PROGRAMS: dict[str, dict] = {
    "us_census_acs": {"program": "acs", "selector": acs_entries},
    "us_census_cbp": {"program": "cbp", "selector": cbp_entries},
    "us_census_pep": {"program": "pep", "selector": pep_entries},
    "us_census_saipe": {"program": "saipe", "selector": saipe_entries},
    "us_census_sahie": {"program": "sahie", "selector": sahie_entries},
    "us_census_intltrade_annual": {
        "program": "intltrade",
        "selector": lambda c: intltrade_imports_entries(c) + intltrade_exports_entries(c),
    },
    "us_census_ecnbasic": {"program": "ecnbasic", "selector": ecnbasic_entries},
}
