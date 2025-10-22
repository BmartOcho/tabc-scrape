import asyncio
import logging
from src.tabc_scrape.data.api_client import TexasComptrollerAPI

# Set up logging
logging.basicConfig(level=logging.INFO)

async def test_api():
    api = TexasComptrollerAPI()
    print("Testing connection...")
    connected = await api.test_connection()
    print(f"Connection test: {connected}")

    print("Fetching sample data...")
    restaurants = await api.get_all_restaurants(batch_size=10)
    print(f"Fetched {len(restaurants)} restaurants")

if __name__ == "__main__":
    asyncio.run(test_api())