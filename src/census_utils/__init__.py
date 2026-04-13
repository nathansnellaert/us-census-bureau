"""Connector-specific utilities for US Census Bureau.

Named 'census_utils' to avoid conflict with legacy 'utils' directory at connector root.
"""

from .constants import (
    CURRENT_YEAR,
    PRIORITY_DATASETS,
    PRIORITY_VARIABLES,
    GEOGRAPHY_LEVELS,
    MAX_VARS_PER_REQUEST,
)
from .catalog import (
    extract_dataset_type,
    should_include_dataset,
    parse_dataset_metadata,
)
from .api import (
    fetch_census_data,
    find_latest_available_year,
)
