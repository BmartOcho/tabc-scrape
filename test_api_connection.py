import asyncio
import logging
import sys
import os

# Set environment variables for this session
os.environ['API_KEY_ID'] = '1qah3gxsa7hq22uv7pq1qb6j2'
os.environ['API_KEY_SECRET'] = 'owqiugc9q94bnvh5r11n3ibvhq1zs2k72lnz8re98woh0neug'
os.environ['APP_TOKEN'] = '2d7MaXhf0SeDpdQ1gEmJ80Jjy'

# Add src to path
sys.path.append('src')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

from tabc_scrape.data.api_client import TexasComptrollerAPI

async def test_api_connection():
    print("=== API CONNECTION TEST ===")

    api = TexasComptrollerAPI()

    print("Testing basic connection...")
    connected = await api.test_connection()
    print(f"Connection test result: {connected}")

    if not connected:
        print("FAILED: Basic connection test failed")
        print("This suggests authentication or network issues")
    else:
        print("SUCCESS: Basic connection test passed")

    print("\nTesting data retrieval...")
    try:
        restaurants = await api.get_all_restaurants(batch_size=1)
        print(f"SUCCESS: Successfully fetched {len(restaurants)} restaurants")

        if restaurants:
            print("Sample restaurant data:")
            restaurant = restaurants[0]
            print(f"  - Name: {restaurant.location_name}")
            print(f"  - Address: {restaurant.location_address}")
            print(f"  - City: {restaurant.location_city}")
    except Exception as e:
        print(f"ERROR: Error fetching restaurants: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_api_connection())