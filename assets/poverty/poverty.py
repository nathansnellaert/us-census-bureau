import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
import json

API_BASE_URL = "https://api.census.gov/data"
API_KEY = None  # Optional

def fetch_saipe_data(year: int):
    endpoint = f"{API_BASE_URL}/timeseries/poverty/saipe"
    
    params = {
        "get": "NAME,SAEPOVRTALL_PT,SAEMHI_PT",
        "for": "state:*",
        "time": str(year)
    }
    
    if API_KEY:
        params["key"] = API_KEY
    
    try:
        response = get(endpoint, params=params)
        return response.json()
    except:
        # Try alternate endpoint structure
        endpoint = f"{API_BASE_URL}/{year}/poverty/saipe"
        params = {
            "get": "NAME,SAEPOVRTALL_PT,SAEMHI_PT",
            "for": "state:*"
        }
        if API_KEY:
            params["key"] = API_KEY
        try:
            response = get(endpoint, params=params)
            return response.json()
        except:
            return None

def process_poverty():
    state = load_state("poverty")
    last_year = state.get("last_year_processed", 2018)
    
    all_records = []
    current_year = datetime.now().year
    
    for year in range(last_year + 1, current_year):
        try:
            data = fetch_saipe_data(year)
            
            if data and len(data) > 1:
                headers = data[0]
                for row in data[1:]:
                    if len(row) == len(headers):
                        row_dict = dict(zip(headers, row))
                        
                        record = {
                            "year": year,
                            "dataset": "saipe",
                            "state_fips": row_dict.get("state", ""),
                            "state_name": row_dict.get("NAME", ""),
                            "poverty_rate_all_ages": float(row_dict.get("SAEPOVRTALL_PT", 0)) if row_dict.get("SAEPOVRTALL_PT") and row_dict.get("SAEPOVRTALL_PT") != "." else None,
                            "median_household_income": float(row_dict.get("SAEMHI_PT", 0)) if row_dict.get("SAEMHI_PT") and row_dict.get("SAEMHI_PT") != "." else None
                        }
                        all_records.append(record)
                
                # Update state after successful year
                save_state("poverty", {"last_year_processed": year})
                
        except Exception as e:
            print(f"Error fetching SAIPE data for {year}: {e}")
            continue
    
    if all_records:
        return pa.Table.from_pylist(all_records)
    
    return pa.Table.from_pylist([])