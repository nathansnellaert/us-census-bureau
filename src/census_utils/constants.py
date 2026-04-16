"""Shared constants for US Census Bureau connector."""

from datetime import datetime

CURRENT_YEAR = datetime.now().year

LICENSE = "U.S. Government Work (public domain)"
SOURCE_URL = "https://www.census.gov/data/developers.html"
CATALOG_URL = "https://api.census.gov/data.json"

GEOGRAPHY_LEVELS = {
    "state": ("state:*", None),
    "county": ("county:*", "state:*"),
}

NON_DATA_VARS = {"for", "in", "ucgid", "NAME", "GEO_ID", "time"}

ACS_TABLE_PREFIXES = [
    "B01001", "B01002", "B02001", "B03001", "B03002",
    "B15003", "B14001",
    "B17001", "B19001", "B19013", "B22001",
    "B23025", "B24010", "B08301", "B08303",
    "B25001", "B25002", "B25003", "B25061", "B25077",
]

PEP_POPULATION_MEASURES = ["POP", "DENSITY"]
PEP_COMPONENTS_MEASURES = ["BIRTHS", "DEATHS", "NATURALCHG", "NETMIG", "RBIRTH", "RDEATH", "RNATURALCHG", "RNETMIG"]

CBP_MEASURES = ["EMP", "PAYANN", "ESTAB", "PAYQTR1"]

SAIPE_MEASURES = [
    "SAEPOVALL_PT", "SAEPOVRTALL_PT",
    "SAEPOV0_17_PT", "SAEPOVRT0_17_PT",
    "SAEPOV5_17R_PT", "SAEPOVRT5_17R_PT",
    "SAEMHI_PT",
]

SAHIE_MEASURES = ["NIC_PT", "NIPR_PT", "NUI_PT", "PCTIC_PT", "PCTUI_PT"]

INTLTRADE_MEASURES = ["GEN_VAL_MO", "CON_VAL_MO"]

ECNBASIC_MEASURES = ["ESTAB", "EMP", "PAYANN", "RCPTOT"]
ECNBASIC_VINTAGES = [2017, 2022]
