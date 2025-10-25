"""
Texas Comptroller API client for restaurant data collection
"""

import time
import logging
import asyncio
from typing import Dict, List, Optional, Any
import aiohttp
import pandas as pd
from dataclasses import dataclass

from ..config import config
from ..storage.cache import get_api_cache, set_api_cache

logger = logging.getLogger(__name__)

@dataclass
class RestaurantRecord:
    """Data class for restaurant records from TABC API"""
    id: str
    taxpayer_number: str
    taxpayer_name: str
    taxpayer_address: str
    taxpayer_city: str
    taxpayer_state: str
    taxpayer_zip: str
    taxpayer_county: str
    location_number: str
    location_name: str
    location_address: str
    location_city: str
    location_state: str
    location_zip: str
    location_county: str
    tabc_permit_number: str
    responsibility_begin_date: str
    responsibility_end_date: str
    obligation_end_date: str
    liquor_receipts: float
    wine_receipts: float
    beer_receipts: float
    cover_charge_receipts: float
    total_receipts: float

    @property
    def full_address(self) -> str:
        """Get full address for geocoding"""
        return f"{self.location_address}, {self.location_city}, {self.location_state} {self.location_zip}"

    @property
    def is_active(self) -> bool:
        """Check if business is currently active based on responsibility end date"""
        if not self.responsibility_end_date or self.responsibility_end_date.strip() == '':
            # No end date means business is still active
            return True

        try:
            from datetime import datetime
            # Convert YYYYMMDD to date object
            end_date = datetime.strptime(self.responsibility_end_date, '%Y%m%d')
            current_date = datetime.now()

            # If end date is in the future or today, business is active
            return end_date.date() >= current_date.date()
        except ValueError:
            # If date parsing fails, assume active (better to include than exclude)
            logger.warning(f"Could not parse responsibility_end_date: {self.responsibility_end_date}")
            return True

class TexasComptrollerAPI:
    """Client for Texas Comptroller restaurant data API"""

    def __init__(self):
        self.base_url = config.api.base_url
        self.timeout = config.api.timeout
        self.max_retries = config.api.max_retries
        self.backoff_factor = config.api.backoff_factor

        # aiohttp session will be created per request for simplicity

    async def _make_request(self, url: str) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic and caching"""
        # Check cache first
        cached_response = await get_api_cache(url)
        if cached_response is not None:
            logger.info(f"Returning cached response for {url}")
            return cached_response

        # Log authentication status for debugging
        api_key_id = getattr(config.api, 'api_key_id', None)
        api_key_secret = getattr(config.api, 'api_key_secret', None)
        app_token = getattr(config.api, 'app_token', None)
        logger.info(f"API Authentication - Key ID present: {api_key_id is not None}")
        logger.info(f"API Authentication - Key Secret present: {api_key_secret is not None}")
        logger.info(f"API Authentication - App Token present: {app_token is not None}")

        headers = {}
        if api_key_id and api_key_secret:
            headers['X-API-Key-ID'] = api_key_id
            headers['X-API-Key-Secret'] = api_key_secret
            logger.info("Using API key authentication in request headers")
        else:
            logger.warning("No API keys configured - requests may fail due to authentication")

        if app_token:
            headers['X-App-Token'] = app_token
            logger.info("Using App Token in request headers")
        else:
            logger.warning("No App Token configured - this may be required for Socrata API")

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Making request to {url} (attempt {attempt + 1})")
                logger.info(f"Request headers: {list(headers.keys())}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=self.timeout), ssl=False) as response:
                        if response.status == 200:
                            response_data = await response.json()
                            logger.info(f"API request successful for {url}, status: {response.status}")
                            logger.info(f"Response headers: {dict(response.headers)}")
                            logger.info(f"Response data keys: {list(response_data.keys()) if isinstance(response_data, dict) else 'Not a dict'}")
                            if isinstance(response_data, dict) and 'value' in response_data:
                                logger.info(f"Number of records in response: {len(response_data['value'])}")
                            # Cache successful response
                            if await set_api_cache(url, response_data):
                                logger.info(f"Cached response for {url}")
                            return response_data
                        elif response.status == 429:
                            # Rate limited, wait and retry
                            wait_time = self.backoff_factor * (2 ** attempt)
                            logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"API request failed with status {response.status}")
                            logger.error(f"Response headers: {dict(response.headers)}")
                            try:
                                error_text = await response.text()
                                logger.error(f"Error response body: {error_text[:500]}")  # First 500 chars
                            except:
                                logger.error("Could not read error response body")
                            return None

            except aiohttp.ClientError as e:
                logger.error(f"Request error: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor * (2 ** attempt)
                    logger.info(f"Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    return None

        return None

    async def get_all_restaurants(self, batch_size: int = 1000, max_batches: int = 100, limit: Optional[int] = None) -> List[RestaurantRecord]:
        """
        Fetch restaurant records from the API

        Args:
            batch_size: Number of records to fetch per request
            max_batches: Maximum number of batches to fetch (safety limit)
            limit: Maximum number of records to fetch (None for all)

        Returns:
            List of RestaurantRecord objects
        """
        logger.info("Fetching restaurant data from Texas Comptroller API")

        restaurants = []
        skip = 0
        batch_count = 0

        while batch_count < max_batches:
            # Build query URL with pagination - start with basic fields only
            query_url = (
                f"{self.base_url}?$top={batch_size}&$skip={skip}&"
                "$select=__id,taxpayer_number,taxpayer_name,taxpayer_address,taxpayer_city,"
                "taxpayer_state,taxpayer_zip,taxpayer_county,location_number,location_name,"
                "location_address,location_city,location_state,location_zip,location_county,"
                "tabc_permit_number,total_receipts"
            )

            logger.info(f"Fetching batch from: {query_url}")
            data = await self._make_request(query_url)
            if not data:
                logger.error(f"Failed to fetch batch starting at {skip} - no data")
                break
            if 'value' not in data:
                logger.error(f"Failed to fetch batch starting at {skip} - no 'value' key in response: {list(data.keys())}")
                break

            batch = data['value']
            logger.info(f"Fetched batch of {len(batch)} records (offset: {skip})")

            # If we got fewer records than requested, we've likely reached the end
            if len(batch) < batch_size:
                logger.info(f"Received {len(batch)} records, less than batch_size {batch_size} - likely reached end of data")

            # Check if we've reached the limit before processing
            if limit and len(restaurants) >= limit:
                logger.info(f"Already reached limit of {limit} records")
                break

            # If limit is set, take only what's needed
            if limit:
                remaining = limit - len(restaurants)
                if len(batch) > remaining:
                    batch = batch[:remaining]
                    logger.info(f"Limiting batch to {remaining} records to reach limit")

            # Convert to RestaurantRecord objects
            for record in batch:
                try:
                    restaurant = RestaurantRecord(
                        id=record.get('__id', ''),
                        taxpayer_number=record.get('taxpayer_number', ''),
                        taxpayer_name=record.get('taxpayer_name', ''),
                        taxpayer_address=record.get('taxpayer_address', ''),
                        taxpayer_city=record.get('taxpayer_city', ''),
                        taxpayer_state=record.get('taxpayer_state', ''),
                        taxpayer_zip=record.get('taxpayer_zip', ''),
                        taxpayer_county=record.get('taxpayer_county', ''),
                        location_number=record.get('location_number', ''),
                        location_name=record.get('location_name', ''),
                        location_address=record.get('location_address', ''),
                        location_city=record.get('location_city', ''),
                        location_state=record.get('location_state', ''),
                        location_zip=record.get('location_zip', ''),
                        location_county=record.get('location_county', ''),
                        tabc_permit_number=record.get('tabc_permit_number', ''),
                        responsibility_begin_date='',  # Will try to add these fields later
                        responsibility_end_date='',
                        obligation_end_date='',
                        liquor_receipts=0.0,  # Will try to add these fields later
                        wine_receipts=0.0,
                        beer_receipts=0.0,
                        cover_charge_receipts=0.0,
                        total_receipts=float(record.get('total_receipts', 0) or 0)
                    )
                    restaurants.append(restaurant)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing record {record.get('__id', 'unknown')}: {e}")
                    continue

            # If we got no data or less than requested, we've likely reached the end
            if not batch or len(batch) < batch_size:
                logger.info("Reached end of data or got empty batch")
                break

            # Check if we've reached the limit after adding records
            if limit and len(restaurants) >= limit:
                logger.info(f"Reached limit of {limit} records")
                break

            skip += batch_size
            batch_count += 1

            # Add delay to be respectful to the API
            await asyncio.sleep(0.5)

        logger.info(f"Successfully fetched {len(restaurants)} restaurant records")
        return restaurants

    async def get_active_restaurants(self, batch_size: int = 1000, limit: Optional[int] = None) -> List[RestaurantRecord]:
        """
        Fetch only active restaurant records (excluding closed businesses)

        Args:
            batch_size: Number of records to fetch per request
            limit: Maximum number of records to fetch (None for all)

        Returns:
            List of active RestaurantRecord objects
        """
        logger.info("Fetching active restaurant data from Texas Comptroller API")

        # Get all restaurants first
        all_restaurants = await self.get_all_restaurants(batch_size, limit=limit)

        # Filter for active businesses only
        active_restaurants = [r for r in all_restaurants if r.is_active]

        logger.info(f"Filtered {len(all_restaurants)} total restaurants to {len(active_restaurants)} active restaurants")
        return active_restaurants

    async def get_restaurants_dataframe(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get restaurant data as a pandas DataFrame

        Args:
            limit: Maximum number of records to return (None for all)

        Returns:
            DataFrame with restaurant data
        """
        restaurants = await self.get_all_restaurants()

        if not restaurants:
            logger.warning("No restaurant data retrieved")
            return pd.DataFrame()

        # Convert to DataFrame
        df_data = []
        for restaurant in restaurants[:limit] if limit else restaurants:
            df_data.append({
                'id': restaurant.id,
                'taxpayer_number': restaurant.taxpayer_number,
                'taxpayer_name': restaurant.taxpayer_name,
                'taxpayer_address': restaurant.taxpayer_address,
                'taxpayer_city': restaurant.taxpayer_city,
                'taxpayer_state': restaurant.taxpayer_state,
                'taxpayer_zip': restaurant.taxpayer_zip,
                'taxpayer_county': restaurant.taxpayer_county,
                'location_number': restaurant.location_number,
                'location_name': restaurant.location_name,
                'location_address': restaurant.location_address,
                'location_city': restaurant.location_city,
                'location_state': restaurant.location_state,
                'location_zip': restaurant.location_zip,
                'location_county': restaurant.location_county,
                'tabc_permit_number': restaurant.tabc_permit_number,
                'responsibility_begin_date': restaurant.responsibility_begin_date,
                'responsibility_end_date': restaurant.responsibility_end_date,
                'obligation_end_date': restaurant.obligation_end_date,
                'liquor_receipts': restaurant.liquor_receipts,
                'wine_receipts': restaurant.wine_receipts,
                'beer_receipts': restaurant.beer_receipts,
                'cover_charge_receipts': restaurant.cover_charge_receipts,
                'total_receipts': restaurant.total_receipts,
                'full_address': restaurant.full_address
            })

        df = pd.DataFrame(df_data)

        # Set proper data types
        numeric_columns = ['liquor_receipts', 'wine_receipts', 'beer_receipts',
                           'cover_charge_receipts', 'total_receipts']
        df[numeric_columns] = df[numeric_columns].astype(float)

        logger.info(f"Created DataFrame with shape {df.shape}")
        return df

    async def test_connection(self) -> bool:
        """Test if the API is accessible"""
        try:
            # Test with a simple data request instead of count endpoint
            test_url = f"{self.base_url}?$top=1&$select=__id,taxpayer_number,taxpayer_name"
            async with aiohttp.ClientSession() as session:
                async with session.get(test_url, timeout=aiohttp.ClientTimeout(total=10), ssl=False) as response:
                    return response.status == 200
        except:
            return False