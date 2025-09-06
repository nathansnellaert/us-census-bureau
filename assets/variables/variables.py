"""Discover variables for each Census dataset"""
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state
import time

# Priority variable patterns by dataset type
PRIORITY_VARIABLES = {
    'acs': {
        # Demographics
        'B01001': 'Sex by Age',
        'B02001': 'Race',
        'B03001': 'Hispanic or Latino Origin',
        
        # Economic
        'B19013': 'Median Household Income',
        'B19001': 'Household Income',
        'B17001': 'Poverty Status',
        'B22001': 'SNAP/Food Stamps',
        
        # Housing
        'B25001': 'Housing Units',
        'B25077': 'Median Home Value',
        'B25003': 'Tenure (Owner/Renter)',
        'B25061': 'Rent Asked',
        
        # Employment
        'B23025': 'Employment Status',
        'B24010': 'Occupation by Sex',
        'B08301': 'Means of Transportation to Work',
        'B08303': 'Travel Time to Work',
        
        # Education
        'B15003': 'Educational Attainment',
        'B14001': 'School Enrollment',
    },
    'cbp': {
        # All CBP variables are important
        'EMP': 'Number of Employees',
        'PAYANN': 'Annual Payroll',
        'ESTAB': 'Number of Establishments',
        'EMPSZES': 'Employment Size of Establishments',
        'PAYQTR1': 'First Quarter Payroll',
    },
    'pep': {
        'POP': 'Total Population',
        'DENSITY': 'Population Density',
        'BIRTHS': 'Births',
        'DEATHS': 'Deaths',
        'NATURALINC': 'Natural Increase',
        'INTERNATIONALMIG': 'International Migration',
        'DOMESTICMIG': 'Domestic Migration',
    },
    'saipe': {
        'SAEPOVRTALL_PT': 'Poverty Rate All Ages',
        'SAEPOVRT0_17_PT': 'Poverty Rate Age 0-17',
        'SAEMHI_PT': 'Median Household Income',
    },
    'sahie': {
        'NIC_PT': 'Percent Uninsured',
        'NIPR_PT': 'Percent Uninsured MOE',
        'NUI_PT': 'Number Uninsured',
    },
    'intltrade': {
        'IMPVAL': 'Import Value',
        'EXPVAL': 'Export Value',
        'BALANCE': 'Trade Balance',
    }
}

def is_priority_variable(dataset_type, var_name):
    """Check if a variable is in our priority list"""
    if dataset_type not in PRIORITY_VARIABLES:
        return True  # Include all variables for unknown dataset types
    
    priorities = PRIORITY_VARIABLES[dataset_type]
    
    # Check exact matches
    if var_name in priorities:
        return True
    
    # Check table prefixes (e.g., B01001_001E matches B01001)
    for priority_prefix in priorities.keys():
        if var_name.startswith(priority_prefix):
            return True
    
    # For CBP, PEP, etc., include most variables
    if dataset_type in ['cbp', 'pep', 'zbp', 'bps']:
        return not var_name.startswith('NAME') and not var_name in ['for', 'in', 'ucgid']
    
    return False

def parse_variable_metadata(var_name, var_info, dataset_type, dataset_path, vintage):
    """Parse variable metadata from the API response"""
    return {
        'dataset_type': dataset_type,
        'dataset_path': dataset_path,
        'vintage': vintage,
        'variable_name': var_name,
        'label': var_info.get('label', ''),
        'concept': var_info.get('concept', ''),
        'predicateType': var_info.get('predicateType', ''),
        'group': var_info.get('group', ''),
        'predicateOnly': var_info.get('predicateOnly', False)
    }

def fetch_dataset_variables(dataset_row):
    """Fetch variables for a single dataset"""
    variables_link = dataset_row['variables_link']
    
    if not variables_link:
        return []
    
    try:
        response = get(variables_link, timeout=30)
        data = response.json()
        
        variables = []
        variables_dict = data.get('variables', {})
        
        # Parse each variable
        for var_name, var_info in variables_dict.items():
            # Skip metadata variables
            if var_name in ['for', 'in', 'ucgid']:
                continue
            
            # Check if this is a priority variable
            if is_priority_variable(dataset_row['dataset_type'], var_name):
                metadata = parse_variable_metadata(
                    var_name, 
                    var_info,
                    dataset_row['dataset_type'],
                    dataset_row['dataset_path'],
                    dataset_row['vintage']
                )
                variables.append(metadata)
        
        return variables
        
    except Exception as e:
        print(f"  Error fetching variables for {dataset_row['dataset_path']}: {e}")
        return []

def process_variables(catalog_data):
    """Process variables for all datasets in the catalog"""
    all_variables = []
    dataset_variable_counts = {}
    
    # Convert to list for processing
    datasets = catalog_data.to_pylist()
    
    print(f"Fetching variables for {len(datasets)} datasets...")
    
    # Process in batches to avoid overwhelming the API
    for i, dataset in enumerate(datasets):
        if i > 0 and i % 10 == 0:
            print(f"  Processed {i}/{len(datasets)} datasets...")
            time.sleep(1)  # Rate limiting
        
        dataset_vars = fetch_dataset_variables(dataset)
        
        if dataset_vars:
            all_variables.extend(dataset_vars)
            key = f"{dataset['dataset_type']}/{dataset['vintage']}"
            dataset_variable_counts[key] = len(dataset_vars)
    
    print(f"Fetched {len(all_variables)} total variables")
    
    # Show summary by dataset type
    type_counts = {}
    for var in all_variables:
        dtype = var['dataset_type']
        type_counts[dtype] = type_counts.get(dtype, 0) + 1
    
    print("Variables by dataset type:")
    for dtype, count in sorted(type_counts.items()):
        print(f"  {dtype}: {count} variables")
    
    # Save state
    save_state("variables", {
        "last_updated": pa.compute.max(catalog_data['modified']).as_py() if len(catalog_data) > 0 else None,
        "total_variables": len(all_variables),
        "dataset_variable_counts": dataset_variable_counts
    })
    
    # Create PyArrow table
    schema = pa.schema([
        pa.field("dataset_type", pa.string()),
        pa.field("dataset_path", pa.string()),
        pa.field("vintage", pa.int32(), nullable=True),
        pa.field("variable_name", pa.string()),
        pa.field("label", pa.string()),
        pa.field("concept", pa.string()),
        pa.field("predicateType", pa.string()),
        pa.field("group", pa.string()),
        pa.field("predicateOnly", pa.bool_())
    ])
    
    return pa.Table.from_pylist(all_variables, schema=schema)