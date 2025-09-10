"""Test a single dataset fetch from Census Bureau"""
import os

# Set environment variables
os.environ['CONNECTOR_NAME'] = 'us-census-bureau'
os.environ['RUN_ID'] = 'test-single-fetch'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CACHE_REQUESTS'] = 'false'
os.environ['CATALOG_TYPE'] = 'local'
os.environ['DATA_DIR'] = 'data'

from utils.http_client import get

# Test fetching a simple ACS dataset
print("Testing single Census API call...")

api_endpoint = "https://api.census.gov/data/2022/acs/acs5"
params = {
    'get': 'NAME,B01003_001E',  # Total population
    'for': 'state:01'  # Just Alabama
}

try:
    response = get(api_endpoint, params=params, timeout=30)
    data = response.json()
    print(f"Success! Response: {data}")
except Exception as e:
    print(f"Error: {e}")

# Test the catalog fetch
print("\n\nTesting catalog fetch (should be fast with cache)...")
from assets.catalog.catalog import process_catalog
catalog = process_catalog()
print(f"Catalog has {len(catalog)} datasets")