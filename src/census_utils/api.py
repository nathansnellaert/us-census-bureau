"""Census API utilities for US Census Bureau connector."""

from subsets_utils import get
from .constants import CURRENT_YEAR


def fetch_census_data(api_endpoint, variables, geography, time_param=None):
    """Fetch data from Census API.

    Args:
        api_endpoint: The Census API endpoint URL
        variables: List of variable names to fetch
        geography: Geography specification (e.g., 'state:*', 'county:*')
        time_param: Optional time parameter for timeseries datasets

    Returns:
        List of lists (header row + data rows) or None if no data
    """
    if not api_endpoint or not variables:
        return None

    try:
        var_string = ','.join(variables)
        params = {'get': f'NAME,{var_string}', 'for': geography}
        if time_param:
            params['time'] = time_param
        response = get(api_endpoint, params=params, timeout=60)
        if response.status_code != 200:
            # 204 No Content usually means data not available for this time period
            if response.status_code != 204:
                print(f"      API error {response.status_code}: {response.text[:100]}")
            return None
        data = response.json()
        return data if data and len(data) > 1 else None
    except Exception as e:
        print(f"      Fetch error: {e}")
        return None


def find_latest_available_year(api_endpoint, test_variable, geography='state:01'):
    """Find the most recent year with available data for timeseries datasets.

    Args:
        api_endpoint: The Census API endpoint URL
        test_variable: A variable name to test availability
        geography: Geography to test against (default: Alabama)

    Returns:
        The most recent year with data, or None if not found
    """
    for year in range(CURRENT_YEAR - 1, CURRENT_YEAR - 5, -1):
        params = {'get': f'NAME,{test_variable}', 'for': geography, 'time': str(year)}
        try:
            response = get(api_endpoint, params=params, timeout=15)
            if response.status_code == 200:
                return year
        except Exception:
            continue
    return None
