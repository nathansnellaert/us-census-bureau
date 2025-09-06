import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
import json

API_BASE_URL = "https://api.census.gov/data"
API_KEY = None  # Optional - Census API is free but key increases rate limits

# Key ACS tables to fetch
ACS_TABLES = {
    "B01003": "Total Population",
    "B25001": "Housing Units",
    "B19013": "Median Household Income",
    "B15003": "Educational Attainment",
    "B08303": "Travel Time to Work"
}

def fetch_acs_data(year: int, table: str, geography: str = "state:*"):
    endpoint = f"{API_BASE_URL}/{year}/acs/acs5"
    
    # Get all variables for the table
    variables = f"group({table})"
    
    params = {
        "get": f"NAME,{variables}",
        "for": geography
    }
    
    if API_KEY:
        params["key"] = API_KEY
    
    response = get(endpoint, params=params)
    return response.json()

def process_acs():
    state = load_state("acs")
    last_year = state.get("last_year_processed", 2018)
    
    all_records = []
    current_year = datetime.now().year
    
    # Process data from last_year + 1 to most recent available (usually 2 years behind)
    for year in range(last_year + 1, current_year - 1):
        for table_id, table_name in ACS_TABLES.items():
            try:
                # Try fetching state-level data
                data = fetch_acs_data(year, table_id, "state:*")
                
                if data and len(data) > 1:
                    headers = data[0]
                    for row in data[1:]:
                        if len(row) == len(headers):
                            row_dict = dict(zip(headers, row))
                            
                            # Extract relevant variables
                            for key, value in row_dict.items():
                                if key.startswith(table_id) and key.endswith("E"):  # Estimate values
                                    record = {
                                        "year": year,
                                        "dataset": "acs5",
                                        "table_id": table_id,
                                        "table_name": table_name,
                                        "state_fips": row_dict.get("state", ""),
                                        "state_name": row_dict.get("NAME", ""),
                                        "variable": key,
                                        "estimate": float(value) if value and value != "-" else None
                                    }
                                    all_records.append(record)
            except Exception as e:
                print(f"Error fetching {table_id} for {year}: {e}")
                # If year doesn't exist, skip to next table
                if "404" in str(e):
                    break
                continue
        
        # Update state after each year
        if all_records:
            save_state("acs", {"last_year_processed": year})
    
    if all_records:
        return pa.Table.from_pylist(all_records)
    
    return pa.Table.from_pylist([])