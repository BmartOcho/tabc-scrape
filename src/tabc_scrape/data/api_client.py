"""
Texas Comptroller API client for restaurant data collection
"""

import time
import logging
from typing import Dict, List, Optional, Any
import requests
import pandas as pd
from dataclasses import dataclass

from ..config import config

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

class TexasComptrollerAPI:
    """Client for Texas Comptroller restaurant data API"""

    def __init__(self):
        self.base_url = config.api.base_url
        self.timeout = config.api.timeout
        self.max_retries = config.api.max_retries
        self.backoff_factor = config.api.backoff_factor

    def _make_request(self, url: str) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Making request to {url} (attempt {attempt + 1})")
                response = requests.get(url, timeout=self.timeout)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limited, wait and retry
                    wait_time = self.backoff_factor * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"API request failed with status {response.status_code}")
                    return None

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor * (2 ** attempt)
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return None

        return None

    def get_all_restaurants(self, batch_size: int = 1000) -> List[RestaurantRecord]:
        """
        Fetch all restaurant records from the API

        Args:
            batch_size: Number of records to fetch per request

        Returns:
            List of RestaurantRecord objects
        """
        logger.info("Fetching all restaurant data from Texas Comptroller API")

        # First, get total count
        count_url = f"{self.base_url}/$count"
        count_response = self._make_request(count_url)

        if not count_response:
            logger.error("Failed to get total count")
            return []

        try:
            # OData count endpoint returns a plain text number
            if isinstance(count_response, dict):
                total_count = count_response.get('value', 0)
            else:
                total_count = int(str(count_response).strip())

            logger.info(f"Total restaurants: {total_count}")
        except (ValueError, AttributeError, TypeError):
            logger.error("Invalid count response")
            return []

        restaurants = []
        skip = 0

        while skip < total_count:
            # Build query URL with pagination
            query_url = (
                f"{self.base_url}?$top={batch_size}&$skip={skip}&"
                "$select=__id,taxpayer_number,taxpayer_name,taxpayer_address,taxpayer_city,"
                "taxpayer_state,taxpayer_zip,taxpayer_county,location_number,location_name,"
                "location_address,location_city,location_state,location_zip,location_county,"
                "tabc_permit_number,responsibility_begin_date,responsibility_end_date,"
                "obligation_end_date,liquor_receipts,wine_receipts,beer_receipts,"
                "cover_charge_receipts,total_receipts"
            )

            data = self._make_request(query_url)
            if not data or 'value' not in data:
                logger.error(f"Failed to fetch batch starting at {skip}")
                break

            batch = data['value']
            logger.info(f"Fetched batch of {len(batch)} records (offset: {skip})")

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
                        responsibility_begin_date=record.get('responsibility_begin_date', ''),
                        responsibility_end_date=record.get('responsibility_end_date', ''),
                        obligation_end_date=record.get('obligation_end_date', ''),
                        liquor_receipts=float(record.get('liquor_receipts', 0) or 0),
                        wine_receipts=float(record.get('wine_receipts', 0) or 0),
                        beer_receipts=float(record.get('beer_receipts', 0) or 0),
                        cover_charge_receipts=float(record.get('cover_charge_receipts', 0) or 0),
                        total_receipts=float(record.get('total_receipts', 0) or 0)
                    )
                    restaurants.append(restaurant)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing record {record.get('__id', 'unknown')}: {e}")
                    continue

            skip += batch_size

            # Add delay to be respectful to the API
            time.sleep(0.5)

        logger.info(f"Successfully fetched {len(restaurants)} restaurant records")
        return restaurants

    def get_restaurants_dataframe(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get restaurant data as a pandas DataFrame

        Args:
            limit: Maximum number of records to return (None for all)

        Returns:
            DataFrame with restaurant data
        """
        restaurants = self.get_all_restaurants()

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

    def test_connection(self) -> bool:
        """Test if the API is accessible"""
        try:
            response = requests.get(f"{self.base_url}/$count", timeout=10)
            return response.status_code == 200
        except:
            return False