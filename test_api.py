import requests
import json
import pandas as pd
from typing import Dict, List
import time

class TexasComptrollerAPI:
    """Class to interact with Texas Comptroller restaurant data APIs"""

    def __init__(self):
        self.endpoints = {
            'odata': 'https://data.texas.gov/api/odata/v4/naix-2893',
            'json': 'https://data.texas.gov/api/v3/views/naix-2893/query.json',
            'csv': 'https://data.texas.gov/api/v3/views/naix-2893/query.csv'
        }

    def test_endpoints(self) -> Dict[str, Dict]:
        """Test all endpoints and return their performance and data structure"""
        results = {}

        for name, url in self.endpoints.items():
            print(f"Testing {name} endpoint: {url}")
            start_time = time.time()

            try:
                response = requests.get(url, timeout=30)
                response_time = time.time() - start_time

                results[name] = {
                    'url': url,
                    'status_code': response.status_code,
                    'response_time': response_time,
                    'content_length': len(response.content),
                    'success': response.status_code == 200
                }

                if response.status_code == 200:
                    if name == 'json':
                        data = response.json()
                        results[name]['record_count'] = len(data.get('data', []))
                        results[name]['columns'] = data.get('columns', [])
                    elif name == 'csv':
                        # For CSV, we'll just check if it's valid
                        results[name]['is_csv'] = True

                print(f"  Status: {response.status_code}, Time: {response_time:.2f}s, Size: {len(response.content)} bytes")

            except Exception as e:
                results[name] = {
                    'url': url,
                    'error': str(e),
                    'success': False
                }
                print(f"  Error: {e}")

        return results

    def get_sample_data(self, endpoint_type: str = 'odata', limit: int = 5) -> pd.DataFrame:
        """Get sample data from the specified endpoint"""
        url = self.endpoints.get(endpoint_type)
        if not url:
            raise ValueError(f"Unknown endpoint type: {endpoint_type}")

        try:
            if endpoint_type == 'odata':
                # OData v4 format - get data from @odata.context and value
                response = requests.get(url, timeout=30)
                data = response.json()
                df = pd.DataFrame(data['value'][:limit])
            elif endpoint_type == 'json':
                response = requests.get(url, timeout=30)
                data = response.json()
                df = pd.DataFrame(data['data'][:limit], columns=data['columns'])
            elif endpoint_type == 'csv':
                df = pd.read_csv(url, nrows=limit)
            else:
                raise ValueError(f"Unsupported endpoint type for sample data: {endpoint_type}")

            return df

        except Exception as e:
            print(f"Error getting sample data: {e}")
            return pd.DataFrame()

def main():
    """Test the API endpoints and analyze data structure"""
    api = TexasComptrollerAPI()

    print("=== Testing Texas Comptroller Restaurant Data APIs ===\n")

    # Test all endpoints
    results = api.test_endpoints()

    print("\n=== Endpoint Analysis ===")
    for name, result in results.items():
        print(f"\n{name.upper()} ENDPOINT:")
        for key, value in result.items():
            if key != 'url':
                print(f"  {key}: {value}")

    # Get sample data from the best performing endpoint
    successful_endpoints = [name for name, result in results.items() if result.get('success', False)]

    if successful_endpoints:
        best_endpoint = min(successful_endpoints,
                          key=lambda x: results[x].get('response_time', float('inf')))

        print(f"\n=== Sample Data from {best_endpoint.upper()} endpoint ===")
        sample_df = api.get_sample_data(best_endpoint)

        if not sample_df.empty:
            print(f"Shape: {sample_df.shape}")
            print(f"Columns: {list(sample_df.columns)}")
            print("\nFirst few rows:")
            print(sample_df.head())
        else:
            print("No sample data retrieved")
    else:
        print("No endpoints are currently accessible")

if __name__ == "__main__":
    main()