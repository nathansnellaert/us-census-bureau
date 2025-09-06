import os
os.environ['CONNECTOR_NAME'] = 'us-census-bureau'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.catalog.catalog import process_catalog
from assets.variables.variables import process_variables
from assets.data_fetcher.data_fetcher import process_data_fetcher

def main():
    validate_environment()
    
    # DAG: catalog → variables → data_fetcher
    
    # Step 1: Discover available datasets
    print("\n=== Step 1: Discovering Census datasets ===")
    catalog_data = process_catalog()
    print(f"Discovered {len(catalog_data)} priority datasets")
    
    # Step 2: Discover variables for each dataset
    print("\n=== Step 2: Discovering variables ===")
    variables_data = process_variables(catalog_data)
    print(f"Discovered {len(variables_data)} priority variables")
    
    # Step 3: Fetch actual data using discovered metadata
    print("\n=== Step 3: Fetching data ===")
    process_data_fetcher(catalog_data, variables_data)
    
    print("\n=== Census data pipeline complete ===")

if __name__ == "__main__":
    main()