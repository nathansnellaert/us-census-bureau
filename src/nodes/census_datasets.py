"""Ingest and transform US Census Bureau datasets.

This node:
1. Fetches the Census API catalog
2. Filters to priority datasets
3. Fetches data for each dataset
4. Uploads to Delta tables
"""
import time
from datetime import datetime
import pyarrow as pa
from subsets_utils import get, save_raw_json, load_raw_json, merge, overwrite, publish, save_state

from census_utils.constants import (
    PRIORITY_DATASETS,
    PRIORITY_VARIABLES,
    GEOGRAPHY_LEVELS,
    MAX_VARS_PER_REQUEST,
)
from census_utils.catalog import should_include_dataset, parse_dataset_metadata
from census_utils.api import fetch_census_data, find_latest_available_year


CATALOG_URL = "https://api.census.gov/data.json"

CATALOG_METADATA = {
    "id": "us_census_catalog",
    "title": "US Census Bureau Dataset Catalog",
    "description": "Catalog of priority datasets available from the US Census Bureau API, including ACS, CBP, PEP, and other survey programs.",
    "column_descriptions": {
        "dataset_id": "Unique identifier URL for the dataset",
        "dataset_type": "Type of Census survey or program (e.g., acs, cbp, pep)",
        "dataset_path": "API path for the dataset",
        "vintage": "Reference year for the dataset",
        "title": "Human-readable title of the dataset",
        "api_endpoint": "Full API endpoint URL for data retrieval",
        "variables_link": "URL to the dataset's variable definitions",
        "priority": "Priority ranking for processing (1 = highest)",
    },
}

DATASET_METADATA_TEMPLATE = {
    "title": "US Census Bureau {dataset_type} ({vintage})",
    "description": "Census data from the {dataset_type} program for vintage {vintage}, with variables at state and county geography levels.",
    "column_descriptions": {
        "dataset_type": "Type of Census survey or program (e.g., acs, cbp, pep)",
        "vintage": "Reference year for the data",
        "geography_name": "Name of the geographic area (state or county)",
        "state_fips": "FIPS code for the state",
        "county_fips": "FIPS code for the county (null for state-level records)",
        "variable": "Census variable code",
        "value": "Raw string value from the Census API",
        "value_numeric": "Numeric value parsed from the raw value (null if non-numeric)",
        "label": "Human-readable label describing the variable",
    },
}


def is_priority_variable(dataset_type, var_name):
    """Check if variable is priority."""
    if dataset_type not in PRIORITY_VARIABLES:
        return dataset_type in ['cbp', 'pep', 'bps']

    for prefix in PRIORITY_VARIABLES[dataset_type]:
        if var_name.startswith(prefix):
            return True
    return False


def fetch_dataset_variables(dataset):
    """Fetch variables for a dataset."""
    variables_link = dataset.get('variables_link')
    if not variables_link:
        return []

    try:
        response = get(variables_link, timeout=30)
        data = response.json()
        variables = []

        for var_name, var_info in data.get('variables', {}).items():
            if var_name in ['for', 'in', 'ucgid', 'NAME']:
                continue
            if is_priority_variable(dataset['dataset_type'], var_name):
                variables.append({
                    'variable_name': var_name,
                    'label': var_info.get('label', ''),
                })

        return variables
    except Exception as e:
        print(f"    Error fetching variables: {e}")
        return []


def download():
    """Fetch Census API catalog and save raw JSON."""
    print("Fetching Census API catalog...")
    response = get(CATALOG_URL)
    catalog = response.json()

    total_datasets = len(catalog.get('dataset', []))
    print(f"  Found {total_datasets:,} datasets in catalog")

    save_raw_json(catalog, "census_catalog")


def transform():
    """Transform Census catalog, discover variables, fetch data."""
    raw_catalog = load_raw_json("census_catalog")
    all_datasets = raw_catalog.get('dataset', [])

    print(f"Processing {len(all_datasets):,} total datasets...")

    # Filter to priority datasets
    filtered = []
    for dataset in all_datasets:
        if should_include_dataset(dataset):
            filtered.append(parse_dataset_metadata(dataset))

    filtered.sort(key=lambda x: (x['priority'], -(x.get('vintage') or 0)))
    print(f"  Filtered to {len(filtered)} priority datasets")

    # Upload catalog
    if filtered:
        catalog_schema = pa.schema([
            ('dataset_id', pa.string()), ('dataset_type', pa.string()),
            ('dataset_path', pa.string()), ('vintage', pa.int32()),
            ('title', pa.string()), ('api_endpoint', pa.string()),
            ('variables_link', pa.string()), ('priority', pa.int32()),
        ])
        catalog_table = pa.Table.from_pylist(filtered, schema=catalog_schema)
        overwrite(catalog_table, "us_census_catalog")
        publish("us_census_catalog", CATALOG_METADATA)
        print(f"  Uploaded catalog: {len(filtered)} datasets")

    # Process top datasets (limit for initial run)
    processed_types = set()
    datasets_to_process = []
    for dataset in filtered:
        dtype = dataset['dataset_type']
        if dtype not in processed_types:
            datasets_to_process.append(dataset)
            processed_types.add(dtype)
            if len(datasets_to_process) >= 6:
                break

    print(f"Fetching data for {len(datasets_to_process)} datasets...")

    for dataset in datasets_to_process:
        print(f"  {dataset['dataset_path']} ({dataset['vintage']})")

        # Fetch variables
        variables = fetch_dataset_variables(dataset)
        if not variables:
            print(f"    No priority variables found")
            continue

        variable_names = [v['variable_name'] for v in variables][:MAX_VARS_PER_REQUEST]
        variable_map = {v['variable_name']: v for v in variables}

        # Determine if timeseries (no vintage) - find latest available year
        is_timeseries = dataset['vintage'] is None
        time_param = None
        effective_vintage = dataset['vintage']

        if is_timeseries:
            latest_year = find_latest_available_year(dataset['api_endpoint'], variable_names[0])
            if latest_year:
                time_param = str(latest_year)
                effective_vintage = latest_year
                print(f"    Using latest available year: {latest_year}")
            else:
                print(f"    Could not find available year for timeseries")
                continue

        records = []
        for geo_name, geo_spec in GEOGRAPHY_LEVELS.items():
            data = fetch_census_data(dataset['api_endpoint'], variable_names, geo_spec, time_param)
            if not data:
                continue

            headers = data[0]
            header_idx = {h: i for i, h in enumerate(headers)}

            for row in data[1:]:
                base = {
                    'dataset_type': dataset['dataset_type'],
                    'vintage': effective_vintage,
                    'geography_name': row[header_idx.get('NAME', 0)],
                    'state_fips': row[header_idx['state']] if 'state' in header_idx else None,
                    'county_fips': row[header_idx['county']] if 'county' in header_idx else None,
                }

                for var_name in variable_names:
                    if var_name in header_idx:
                        value = row[header_idx[var_name]]
                        record = base.copy()
                        record['variable'] = var_name
                        record['value'] = value
                        record['label'] = variable_map.get(var_name, {}).get('label', '')
                        try:
                            record['value_numeric'] = float(value) if value and value not in ['null', 'N/A', '-'] else None
                        except (ValueError, TypeError):
                            record['value_numeric'] = None
                        records.append(record)

            time.sleep(0.5)

        if records:
            schema = pa.schema([
                ('dataset_type', pa.string()), ('vintage', pa.int32()),
                ('geography_name', pa.string()), ('state_fips', pa.string()),
                ('county_fips', pa.string()), ('variable', pa.string()),
                ('value', pa.string()), ('value_numeric', pa.float64()),
                ('label', pa.string()),
            ])
            table = pa.Table.from_pylist(records, schema=schema)
            dataset_name = f"us_census_{dataset['dataset_type']}_{effective_vintage}"
            merge(table, dataset_name, key=["dataset_type", "vintage", "geography_name", "state_fips", "county_fips", "variable", "label"])
            dataset_metadata = {
                "id": dataset_name,
                "title": DATASET_METADATA_TEMPLATE["title"].format(dataset_type=dataset['dataset_type'].upper(), vintage=effective_vintage),
                "description": DATASET_METADATA_TEMPLATE["description"].format(dataset_type=dataset['dataset_type'].upper(), vintage=effective_vintage),
                "column_descriptions": DATASET_METADATA_TEMPLATE["column_descriptions"],
            }
            publish(dataset_name, dataset_metadata)
            print(f"    Uploaded {len(records):,} records as {dataset_name}")

    save_state("census_datasets", {
        "last_updated": datetime.now().isoformat(),
        "datasets_processed": len(datasets_to_process),
    })


NODES = {
    download: [],
    transform: [download],
}


if __name__ == "__main__":
    download()
    transform()
