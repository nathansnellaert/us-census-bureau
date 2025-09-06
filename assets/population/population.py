import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
import json

API_BASE_URL = "https://api.census.gov/data"
API_KEY = None  # Optional

def fetch_population_estimates(year: int):
    endpoint = f"{API_BASE_URL}/{year}/pep/population"
    
    params = {
        "get": "NAME,POP,DENSITY",
        "for": "state:*"
    }
    
    if API_KEY:
        params["key"] = API_KEY
    
    try:
        response = get(endpoint, params=params)
        return response.json()
    except:
        # PEP endpoint structure varies by year
        # Try alternate endpoint
        endpoint = f"{API_BASE_URL}/{year}/pep/charagegroups"
        params = {
            "get": "NAME,POP",
            "for": "state:*"
        }
        if API_KEY:
            params["key"] = API_KEY
        try:
            response = get(endpoint, params=params)
            return response.json()
        except:
            return None

def process_population():
    state = load_state("population")
    last_year = state.get("last_year_processed", 2018)
    
    all_records = []
    current_year = datetime.now().year
    
    for year in range(last_year + 1, current_year):
        try:
            data = fetch_population_estimates(year)
            
            if data and len(data) > 1:
                headers = data[0]
                for row in data[1:]:
                    if len(row) == len(headers):
                        row_dict = dict(zip(headers, row))
                        
                        record = {
                            "year": year,
                            "dataset": "population_estimates",
                            "state_fips": row_dict.get("state", ""),
                            "state_name": row_dict.get("NAME", ""),
                            "population": float(row_dict.get("POP", 0)) if row_dict.get("POP") and row_dict.get("POP") != "-" else None,
                            "population_density": float(row_dict.get("DENSITY", 0)) if row_dict.get("DENSITY") and row_dict.get("DENSITY") != "-" else None
                        }
                        all_records.append(record)
                
                # Update state after successful year
                save_state("population", {"last_year_processed": year})
                
        except Exception as e:
            print(f"Error fetching population estimates for {year}: {e}")
            continue
    
    if all_records:
        return pa.Table.from_pylist(all_records)
    
    return pa.Table.from_pylist([])