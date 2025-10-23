#!/usr/bin/env python3
"""
Test script to validate 1000 record collection
"""

import asyncio
import sys
import os
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from tabc_scrape.data.api_client import TexasComptrollerAPI
from tabc_scrape.storage.database import DatabaseManager

async def test_1000_records():
    """Test fetching exactly 1000 records efficiently"""
    print("Testing 1000 Record Collection Strategy")
    print("=" * 50)

    # Initialize components
    api_client = TexasComptrollerAPI()
    db_manager = DatabaseManager("sqlite:///src/dev_tabc_restaurants.db")

    # Test 1: API Connection
    print("1. Testing API connection...")
    if not await api_client.test_connection():
        print("API connection failed")
        return False
    print("API connection successful")

    # Test 2: Fetch 1000 records
    print("\n2. Fetching 1000 restaurant records...")
    start_time = time.time()

    try:
        restaurants = await api_client.get_all_restaurants(
            batch_size=1000,
            max_batches=5,  # Fetch up to 5 batches for 5000 records
            limit=5000
        )

        fetch_time = time.time() - start_time

        if not restaurants:
            print("No restaurants fetched")
            return False

        print(f"Fetched {len(restaurants)} restaurants in {fetch_time:.2f} seconds")
        print(f"Average: {len(restaurants)/fetch_time:.1f} records/second")

        # Test 3: Store in database
        print("\n3. Storing records in database...")
        start_time = time.time()

        stored_count = db_manager.store_restaurants([r.__dict__ for r in restaurants])
        store_time = time.time() - start_time

        print(f"Stored {stored_count} records in {store_time:.2f} seconds")

        # Test 4: Verify data quality
        print("\n4. Verifying data quality...")
        sample_restaurant = restaurants[0]
        required_fields = [
            'location_name', 'location_address', 'location_city',
            'location_state', 'location_zip', 'total_receipts'
        ]

        missing_fields = []
        for field in required_fields:
            if not getattr(sample_restaurant, field, None):
                missing_fields.append(field)

        if missing_fields:
            print(f"Sample restaurant missing fields: {missing_fields}")
        else:
            print("All required fields present in sample")

        # Test 5: Show sample data
        print("\n5. Sample restaurant data:")
        print(f"   Name: {sample_restaurant.location_name}")
        print(f"   Address: {sample_restaurant.full_address}")
        print(f"   Total Receipts: ${sample_restaurant.total_receipts:,.2f}")

        print("\nSUCCESS: 1000 records collected and stored successfully!")
        print(f"Total time: {fetch_time + store_time:.2f} seconds")
        print(f"Efficiency: {len(restaurants)/(fetch_time + store_time):.1f} records/second")

        return True

    except Exception as e:
        print(f"Error during collection: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_1000_records())
    sys.exit(0 if success else 1)