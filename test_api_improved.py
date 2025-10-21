#!/usr/bin/env python3
"""
Test script for the improved Texas Comptroller API client
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from tabc_scrape.data.api_client import TexasComptrollerAPI
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Test the API client"""
    print("=== Testing Improved Texas Comptroller API Client ===\n")

    # Initialize API client
    api = TexasComptrollerAPI()

    # Test connection
    print("Testing API connection...")
    if api.test_connection():
        print("API connection successful")
    else:
        print("API connection failed")
        return

    # Get sample data (limit to 10 for testing)
    print("\nFetching sample restaurant data...")
    try:
        df = api.get_restaurants_dataframe(limit=10)
        if not df.empty:
            print(f"Successfully retrieved {len(df)} restaurant records")
            print(f"DataFrame shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")

            print("\nSample data:")
            print(df[['location_name', 'location_address', 'location_city', 'total_receipts']].head())

            # Test full address property
            print("\nTesting full address formatting:")
            for _, row in df.head(3).iterrows():
                print(f"  {row['location_name']}: {row['full_address']}")
        else:
            print("No data retrieved")
    except Exception as e:
        print(f"✗ Error retrieving data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()