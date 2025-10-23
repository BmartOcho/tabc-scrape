"""
Web scraper for restaurant square footage data
"""

import logging
import re
import time
import asyncio
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import quote, urlencode
import json
from pydantic import BaseModel, Field, validator

logger = logging.getLogger(__name__)

class ScrapingInput(BaseModel):
    """Input validation for scraping requests"""
    restaurant_name: str = Field(..., min_length=1, max_length=200, description="Restaurant name")
    address: str = Field(..., min_length=1, max_length=500, description="Restaurant address")
    county: str = Field(default="", max_length=100, description="County name")

    @validator('restaurant_name', 'address', 'county')
    def sanitize_string(cls, v):
        if not v:
            return v
        # Remove potentially harmful characters
        v = re.sub(r'[<>"/\\|?*]', '', v)
        # Limit length and strip whitespace
        return v.strip()[:200] if 'name' in cls.__name__.lower() else v.strip()[:500]

@dataclass
class SquareFootageResult:
    """Result of square footage scraping"""
    restaurant_name: str
    address: str
    square_footage: Optional[int]
    source: str
    confidence: float
    source_url: Optional[str] = None
    property_details: Optional[Dict[str, Any]] = None

class SquareFootageScraper:
    """Scraper for restaurant square footage information"""

    def __init__(self):
        # aiohttp will be used per request

        # Common square footage patterns in text
        self.sqft_patterns = [
            r'(\d{1,3}(?:,\d{3})*)\s*(?:sq\.?\s*ft\.?|square\s+feet?|sqft)',
            r'(\d{1,3}(?:,\d{3})*)\s*(?:sf|square\s+foot)',
            r'building\s+size[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'property\s+size[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'restaurant\s+size[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'total\s+area[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'floor\s+area[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'leasable\s+area[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'(\d{1,3}(?:,\d{3})*)\s*(?:sq\s+ft|square\s+feet)',
            r'building\s+area[:\s]+(\d{1,3}(?:,\d{3})*)',
            r'(\d{1,3}(?:,\d{3})*)\s*(?:square\s+feet|sqft|sf)',
        ]

        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.sqft_patterns]

    def _extract_square_footage_from_text(self, text: str) -> Optional[int]:
        """Extract square footage numbers from text"""
        for pattern in self.compiled_patterns:
            matches = pattern.findall(text)
            if matches:
                # Get the largest number found (most likely the building size)
                numbers = []
                for match in matches:
                    if isinstance(match, tuple):
                        number_str = match[0]
                    else:
                        number_str = match

                    # Remove commas and convert to int
                    try:
                        number = int(number_str.replace(',', ''))
                        if 100 <= number <= 100000:  # Reasonable range for restaurant size
                            numbers.append(number)
                    except ValueError:
                        continue

                if numbers:
                    return max(numbers)  # Return the largest valid number

        return None

    async def _search_google(self, query: str, num_results: int = 5) -> List[str]:
        """Search Google and return top result URLs"""
        try:
            search_url = f"https://www.google.com/search?q={quote(query)}&num={num_results}"
            async with aiohttp.ClientSession() as session:
                async with session.get(search_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        logger.warning(f"Google search failed with status {response.status}")
                        return []

                    soup = BeautifulSoup(await response.text(), 'html.parser')

                    # Extract URLs from search results
                    urls = []
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if href.startswith('/url?q='):
                            # Extract the actual URL from Google's redirect
                            url = href.split('/url?q=')[1].split('&')[0]
                            if url.startswith('https') and 'google.com' not in url:  # Prefer HTTPS
                                urls.append(url)
                                if len(urls) >= num_results:
                                    break

                    return urls

        except Exception as e:
            logger.error(f"Error searching Google: {e}")
            return []

    async def _scrape_property_appraiser(self, county: str, address: str) -> Optional[int]:
        """Scrape county property appraiser websites"""
        # Common Texas county property appraiser URLs
        county_urls = {
            'harris': 'https://www.hcad.org/property-search/',
            'dallas': 'https://www.dcad.org/search/',
            'tarrant': 'https://www.tad.org/search/',
            'travis': 'https://www.traviscad.org/property-search/',
            'collin': 'https://www.collincad.org/property-search/',
            'bexar': 'https://www.bcad.org/search/',
            'el paso': 'https://www.epcad.org/search/',
            'hidalgo': 'https://www.hidalgocad.org/search/',
            'fort bend': 'https://www.fbcad.org/search/',
            'montgomery': 'https://www.mcad-tx.org/search/',
            'williamson': 'https://www.wcad.org/search/',
            'galveston': 'https://www.galvestoncad.org/search/',
            'denton': 'https://www.dentoncad.com/search/',
            'cameron': 'https://www.cameroncad.org/search/',
            'nuevo': 'https://www.nuecescad.org/search/',
        }

        base_url = county_urls.get(county.lower())
        if not base_url:
            return None

        try:
            # This is a simplified example - real implementation would need
            # to handle each county's specific search interface
            search_query = f"{address} restaurant"
            search_url = f"{base_url}?q={quote(search_query)}"

            async with aiohttp.ClientSession() as session:
                async with session.get(search_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        text = await response.text()
                        sqft = self._extract_square_footage_from_text(text)
                        if sqft:
                            return sqft

        except Exception as e:
            logger.error(f"Error scraping {county} property records: {e}")

        return None

    async def _scrape_restaurant_websites(self, restaurant_name: str, address: str) -> Optional[int]:
        """Scrape restaurant's own website for square footage info"""
        try:
            # Search for restaurant website
            query = f'"{restaurant_name}" {address} official site OR website'
            urls = await self._search_google(query, num_results=5)

            for url in urls:
                if any(domain in url.lower() for domain in ['.com', '.net', '.org', '.biz']):
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                                if response.status == 200:
                                    text = await response.text()
                                    sqft = self._extract_square_footage_from_text(text)
                                    if sqft:
                                        return sqft
                    except:
                        continue

        except Exception as e:
            logger.error(f"Error scraping restaurant websites: {e}")

        return None

    async def _scrape_commercial_real_estate(self, restaurant_name: str, address: str) -> Optional[int]:
        """Scrape commercial real estate websites"""
        try:
            # Search for restaurant in commercial listings
            query = f'"{restaurant_name}" {address} commercial real estate OR for lease OR for sale'
            urls = await self._search_google(query, num_results=5)

            for url in urls:
                if any(site in url.lower() for site in ['loopnet.com', 'crexi.com', 'showcase.com', 'costar.com', 'properties.com']):
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                                if response.status == 200:
                                    text = await response.text()
                                    sqft = self._extract_square_footage_from_text(text)
                                    if sqft:
                                        return sqft
                    except:
                        continue

        except Exception as e:
            logger.error(f"Error scraping commercial real estate sites: {e}")

        return None

    async def scrape_square_footage(self, restaurant_name: str, address: str, county: str = "") -> SquareFootageResult:
        """
        Scrape square footage for a restaurant using multiple sources

        Args:
            restaurant_name: Name of the restaurant
            address: Restaurant address
            county: County name (optional, for property records)

        Returns:
            SquareFootageResult with scraped data
        """
        # Validate and sanitize inputs
        input_data = ScrapingInput(restaurant_name=restaurant_name, address=address, county=county)
        restaurant_name = input_data.restaurant_name
        address = input_data.address
        county = input_data.county

        logger.info(f"Scraping square footage for {restaurant_name} at {address}")

        sources_tried = []
        square_footage = None
        source = "none"
        confidence = 0.0

        # Try 1: County Property Appraiser Records
        if county:
            sources_tried.append("county_records")
            square_footage = await self._scrape_property_appraiser(county, address)
            if square_footage:
                source = "county_records"
                confidence = 0.9
                logger.info(f"Found square footage from county records: {square_footage}")

        # Try 2: Restaurant Website
        if not square_footage:
            sources_tried.append("restaurant_website")
            square_footage = await self._scrape_restaurant_websites(restaurant_name, address)
            if square_footage:
                source = "restaurant_website"
                confidence = 0.7
                logger.info(f"Found square footage from restaurant website: {square_footage}")

        # Try 3: Commercial Real Estate Listings
        if not square_footage:
            sources_tried.append("commercial_real_estate")
            square_footage = await self._scrape_commercial_real_estate(restaurant_name, address)
            if square_footage:
                source = "commercial_real_estate"
                confidence = 0.8
                logger.info(f"Found square footage from commercial listings: {square_footage}")

        # Try 4: Google Search for permits/records
        if not square_footage:
            sources_tried.append("google_search")
            query = f'"{restaurant_name}" {address} square footage OR building size OR property records OR commercial listing'
            urls = await self._search_google(query, num_results=10)  # Increase num_results for better coverage

            for url in urls:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                            if response.status == 200:
                                text = await response.text()
                                sqft = self._extract_square_footage_from_text(text)
                                if sqft:
                                    square_footage = sqft
                                    source = "google_search"
                                    confidence = 0.5
                                    logger.info(f"Found square footage from Google search: {square_footage}")
                                    break
                except:
                    continue

        # Calculate final confidence based on source and data quality
        if square_footage:
            # Adjust confidence based on source reliability
            source_confidence = {
                'county_records': 0.9,
                'commercial_real_estate': 0.8,
                'restaurant_website': 0.7,
                'google_search': 0.5
            }
            confidence = source_confidence.get(source, 0.3)

            # Reduce confidence for very large or very small values
            if square_footage > 50000 or square_footage < 500:
                confidence *= 0.7

        result = SquareFootageResult(
            restaurant_name=restaurant_name,
            address=address,
            square_footage=square_footage,
            source=source,
            confidence=confidence,
            source_url=None,  # Could be populated with the URL where data was found
            property_details={'sources_tried': sources_tried} if sources_tried else None
        )

        logger.info(f"Square footage scraping result: {square_footage} sqft from {source} (confidence: {confidence:.2f})")
        return result

    async def scrape_multiple_restaurants(self, restaurants: List[Dict[str, Any]]) -> Dict[str, SquareFootageResult]:
        """
        Scrape square footage for multiple restaurants

        Args:
            restaurants: List of restaurant data dictionaries

        Returns:
            Dictionary mapping restaurant IDs to results
        """
        results = {}

        for restaurant in restaurants:
            restaurant_id = restaurant.get('id', restaurant.get('location_name', 'unknown'))
            name = restaurant.get('location_name', '')
            address = restaurant.get('full_address', restaurant.get('location_address', ''))
            county = restaurant.get('location_county', '')

            # Add small delay between requests to be respectful
            if results:  # Don't delay the first request
                await asyncio.sleep(1)

            try:
                result = await self.scrape_square_footage(name, address, county)
                results[restaurant_id] = result
            except Exception as e:
                logger.error(f"Error scraping {name}: {e}")
                results[restaurant_id] = SquareFootageResult(
                    restaurant_name=name,
                    address=address,
                    square_footage=None,
                    source='error',
                    confidence=0.0
                )

        return results

    def get_scraping_stats(self, results: Dict[str, SquareFootageResult]) -> Dict[str, Any]:
        """Get statistics about scraping results"""
        total = len(results)
        successful = sum(1 for r in results.values() if r.square_footage is not None)
        failed = total - successful

        sources = {}
        for result in results.values():
            if result.square_footage is not None:
                sources[result.source] = sources.get(result.source, 0) + 1

        avg_confidence = sum(r.confidence for r in results.values() if r.square_footage is not None) / max(successful, 1)

        return {
            'total_restaurants': total,
            'successful_scrapes': successful,
            'failed_scrapes': failed,
            'success_rate': successful / total if total > 0 else 0,
            'sources_used': sources,
            'average_confidence': avg_confidence
        }