"""Generic data fetcher for Census datasets using discovered metadata"""
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state, upload_data
import time
from datetime import datetime

# Geographic levels to fetch
GEOGRAPHY_LEVELS = {
    'state': 'state:*',
    'county': 'county:*',
    # 'place': 'place:*',  # Cities/towns - very large, enable selectively
    # 'tract': 'tract:*',  # Census tracts - very large
}

# Limit variables per request to avoid API limits
MAX_VARS_PER_REQUEST = 50

def build_variable_groups(variables):
    """Group variables for efficient API calls"""
    groups = []
    current_group = []
    
    for var in variables:
        current_group.append(var['variable_name'])
        if len(current_group) >= MAX_VARS_PER_REQUEST:
            groups.append(current_group)
            current_group = []
    
    if current_group:
        groups.append(current_group)
    
    return groups

def fetch_census_data(api_endpoint, variables, geography):
    """Fetch data from Census API"""
    if not api_endpoint or not variables:
        return None
    
    try:
        # Build the variables string
        var_string = ','.join(variables)
        
        params = {
            'get': f'NAME,{var_string}',
            'for': geography
        }
        
        response = get(api_endpoint, params=params, timeout=60)
        data = response.json()
        
        if data and len(data) > 1:
            return data
        
    except Exception as e:
        print(f"    Error fetching data: {e}")
    
    return None

def parse_api_response(data, dataset_info, variable_map):
    """Parse Census API response into records"""
    if not data or len(data) < 2:
        return []
    
    headers = data[0]
    records = []
    
    # Create header index map
    header_idx = {h: i for i, h in enumerate(headers)}
    
    # Process each row
    for row in data[1:]:
        base_record = {
            'dataset_type': dataset_info['dataset_type'],
            'vintage': dataset_info['vintage'],
            'geography_name': row[header_idx.get('NAME', 0)],
        }
        
        # Add geographic identifiers
        if 'state' in header_idx:
            base_record['state_fips'] = row[header_idx['state']]
        if 'county' in header_idx:
            base_record['county_fips'] = row[header_idx['county']]
        if 'place' in header_idx:
            base_record['place_fips'] = row[header_idx['place']]
        
        # Process each variable
        for var_name in variable_map.keys():
            if var_name in header_idx:
                value = row[header_idx[var_name]]
                
                # Create a record for this variable
                record = base_record.copy()
                record['variable'] = var_name
                record['value'] = value
                record['label'] = variable_map[var_name].get('label', '')
                
                # Try to convert to numeric if possible
                try:
                    if value and value not in ['null', 'N/A', '-']:
                        record['value_numeric'] = float(value)
                    else:
                        record['value_numeric'] = None
                except (ValueError, TypeError):
                    record['value_numeric'] = None
                
                records.append(record)
    
    return records

def process_dataset_data(dataset_info, variables):
    """Process data for a single dataset"""
    api_endpoint = dataset_info['api_endpoint']
    
    if not api_endpoint:
        return []
    
    print(f"  Fetching {dataset_info['dataset_path']} ({dataset_info['vintage']})")
    
    # Create variable map for labels
    variable_map = {v['variable_name']: v for v in variables}
    
    # Group variables for batching
    variable_groups = build_variable_groups(variables)
    
    all_records = []
    
    # Fetch data for each geography level
    for geo_name, geo_spec in GEOGRAPHY_LEVELS.items():
        print(f"    Geography: {geo_name}")
        
        # Fetch data for each variable group
        for i, var_group in enumerate(variable_groups):
            if i > 0 and i % 5 == 0:
                time.sleep(1)  # Rate limiting
            
            data = fetch_census_data(api_endpoint, var_group, geo_spec)
            
            if data:
                records = parse_api_response(data, dataset_info, variable_map)
                all_records.extend(records)
                print(f"      Fetched {len(records)} records for {len(var_group)} variables")
    
    return all_records

def process_data_fetcher(catalog_data, variables_data):
    """Main data fetching process"""
    # Group variables by dataset
    variables_by_dataset = {}
    for var in variables_data.to_pylist():
        key = f"{var['dataset_path']}_{var['vintage']}"
        if key not in variables_by_dataset:
            variables_by_dataset[key] = []
        variables_by_dataset[key].append(var)
    
    # Process high-priority datasets first
    datasets = catalog_data.to_pylist()
    datasets.sort(key=lambda x: (x['priority'], -x.get('vintage', 0) if x.get('vintage') else 0))
    
    # Limit to most recent datasets of each type for initial implementation
    processed_types = set()
    datasets_to_process = []
    
    for dataset in datasets:
        dtype = dataset['dataset_type']
        if dtype not in processed_types:
            datasets_to_process.append(dataset)
            processed_types.add(dtype)
            if len(datasets_to_process) >= 10:  # Limit for testing
                break
    
    print(f"Processing {len(datasets_to_process)} datasets...")
    
    all_records = []
    
    for dataset in datasets_to_process:
        key = f"{dataset['dataset_path']}_{dataset['vintage']}"
        dataset_variables = variables_by_dataset.get(key, [])
        
        if not dataset_variables:
            continue
        
        # Fetch data for this dataset
        records = process_dataset_data(dataset, dataset_variables)
        
        if records:
            all_records.extend(records)
            
            # Create dataset-specific table for upload
            dataset_records = [r for r in records if r['dataset_type'] == dataset['dataset_type']]
            if dataset_records:
                # Create PyArrow table for this dataset
                schema = pa.schema([
                    pa.field("dataset_type", pa.string()),
                    pa.field("vintage", pa.int32(), nullable=True),
                    pa.field("geography_name", pa.string()),
                    pa.field("state_fips", pa.string(), nullable=True),
                    pa.field("county_fips", pa.string(), nullable=True),
                    pa.field("place_fips", pa.string(), nullable=True),
                    pa.field("variable", pa.string()),
                    pa.field("value", pa.string()),
                    pa.field("value_numeric", pa.float64(), nullable=True),
                    pa.field("label", pa.string())
                ])
                
                table = pa.Table.from_pylist(dataset_records, schema=schema)
                
                # Upload this dataset
                dataset_name = f"{dataset['dataset_type']}_{dataset['vintage']}"
                print(f"  Uploading {len(table)} records as {dataset_name}")
                upload_data(table, dataset_name)
    
    print(f"Total records fetched: {len(all_records)}")
    
    # Save state
    save_state("data_fetcher", {
        "last_updated": datetime.now().isoformat(),
        "datasets_processed": len(datasets_to_process),
        "total_records": len(all_records)
    })
    
    # Return empty table since we upload per dataset
    return pa.Table.from_pylist([])