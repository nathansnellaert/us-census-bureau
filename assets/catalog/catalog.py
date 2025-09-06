"""Discover and catalog all available Census Bureau datasets"""
import pyarrow as pa
from datetime import datetime
from utils.http_client import get
from utils.io import load_state, save_state

CATALOG_URL = "https://api.census.gov/data.json"

# Priority dataset patterns we want to collect
PRIORITY_DATASETS = {
    'acs': {
        'name': 'American Community Survey',
        'min_year': 2010,
        'priority': 1
    },
    'cbp': {
        'name': 'County Business Patterns',
        'min_year': 2010,
        'priority': 1
    },
    'pep': {
        'name': 'Population Estimates Program',
        'min_year': 2010,
        'priority': 1
    },
    'zbp': {
        'name': 'ZIP Business Patterns',
        'min_year': 2010,
        'priority': 2
    },
    'ase': {
        'name': 'Annual Survey of Entrepreneurs',
        'min_year': 2014,
        'priority': 2
    },
    'abscs': {
        'name': 'Annual Business Survey',
        'min_year': 2017,
        'priority': 2
    },
    'bdstimeseries': {
        'name': 'Business Dynamics Statistics',
        'min_year': 2010,
        'priority': 2
    },
    'intltrade': {
        'name': 'International Trade',
        'min_year': 2013,
        'priority': 1
    },
    'sahie': {
        'name': 'Small Area Health Insurance Estimates',
        'min_year': 2010,
        'priority': 2
    },
    'saipe': {
        'name': 'Small Area Income and Poverty Estimates',
        'min_year': 2010,
        'priority': 1
    },
    'ecnbasic': {
        'name': 'Economic Census',
        'min_year': 2017,
        'priority': 1
    },
    'bps': {
        'name': 'Building Permits Survey',
        'min_year': 2010,
        'priority': 1
    }
}

def extract_dataset_type(dataset):
    """Extract the dataset type from the c_dataset array"""
    if 'c_dataset' in dataset and isinstance(dataset['c_dataset'], list):
        # Join dataset parts, e.g., ['acs', 'acs5'] -> 'acs/acs5'
        dataset_path = '/'.join(dataset['c_dataset'])
        
        # Check if any priority dataset pattern matches
        for pattern, info in PRIORITY_DATASETS.items():
            if pattern in dataset_path.lower():
                return pattern, dataset_path
    return None, None

def should_include_dataset(dataset):
    """Determine if we should include this dataset based on our priorities"""
    dataset_type, dataset_path = extract_dataset_type(dataset)
    
    if not dataset_type:
        return False
    
    # Check vintage year
    vintage = dataset.get('c_vintage')
    if vintage:
        try:
            year = int(vintage)
            min_year = PRIORITY_DATASETS[dataset_type]['min_year']
            if year < min_year:
                return False
        except (ValueError, TypeError):
            pass
    
    # Check if dataset is available
    if not dataset.get('c_isAvailable', True):
        return False
    
    return True

def parse_dataset_metadata(dataset):
    """Extract relevant metadata from a dataset entry"""
    dataset_type, dataset_path = extract_dataset_type(dataset)
    
    # Extract API endpoint
    api_endpoint = None
    if 'distribution' in dataset:
        for dist in dataset['distribution']:
            if dist.get('format') == 'API':
                api_endpoint = dist.get('accessURL')
                break
    
    return {
        'dataset_id': dataset.get('identifier', ''),
        'dataset_type': dataset_type,
        'dataset_path': dataset_path,
        'vintage': dataset.get('c_vintage'),
        'title': dataset.get('title', ''),
        'description': dataset.get('description', '')[:500] if dataset.get('description') else '',
        'api_endpoint': api_endpoint,
        'variables_link': dataset.get('c_variablesLink'),
        'geography_link': dataset.get('c_geographyLink'),
        'groups_link': dataset.get('c_groupsLink'),
        'examples_link': dataset.get('c_examplesLink'),
        'is_microdata': dataset.get('c_isMicrodata', False),
        'is_aggregate': dataset.get('c_isAggregate', False),
        'is_cube': dataset.get('c_isCube', False),
        'modified': dataset.get('modified', ''),
        'priority': PRIORITY_DATASETS.get(dataset_type, {}).get('priority', 99)
    }

def process_catalog():
    """Fetch and process the Census API catalog"""
    print("Fetching Census API catalog...")
    response = get(CATALOG_URL)
    catalog = response.json()
    
    print(f"Found {len(catalog.get('dataset', []))} total datasets")
    
    # Filter and parse datasets
    filtered_datasets = []
    dataset_counts = {}
    
    for dataset in catalog.get('dataset', []):
        if should_include_dataset(dataset):
            metadata = parse_dataset_metadata(dataset)
            filtered_datasets.append(metadata)
            
            # Count by type
            dtype = metadata['dataset_type']
            dataset_counts[dtype] = dataset_counts.get(dtype, 0) + 1
    
    print(f"Filtered to {len(filtered_datasets)} priority datasets:")
    for dtype, count in sorted(dataset_counts.items()):
        print(f"  {dtype}: {count} datasets")
    
    # Sort by priority and vintage (newest first)
    filtered_datasets.sort(key=lambda x: (x['priority'], -x.get('vintage', 0) if x.get('vintage') else 0))
    
    # Save state
    save_state("catalog", {
        "last_updated": datetime.now().isoformat(),
        "total_datasets": len(catalog.get('dataset', [])),
        "filtered_datasets": len(filtered_datasets),
        "dataset_counts": dataset_counts
    })
    
    # Create PyArrow table
    schema = pa.schema([
        pa.field("dataset_id", pa.string()),
        pa.field("dataset_type", pa.string()),
        pa.field("dataset_path", pa.string()),
        pa.field("vintage", pa.int32(), nullable=True),
        pa.field("title", pa.string()),
        pa.field("description", pa.string()),
        pa.field("api_endpoint", pa.string(), nullable=True),
        pa.field("variables_link", pa.string(), nullable=True),
        pa.field("geography_link", pa.string(), nullable=True),
        pa.field("groups_link", pa.string(), nullable=True),
        pa.field("examples_link", pa.string(), nullable=True),
        pa.field("is_microdata", pa.bool_()),
        pa.field("is_aggregate", pa.bool_()),
        pa.field("is_cube", pa.bool_()),
        pa.field("modified", pa.string()),
        pa.field("priority", pa.int32())
    ])
    
    return pa.Table.from_pylist(filtered_datasets, schema=schema)