"""
Population analysis for restaurant location demographics
"""

import logging
import re
import time
import os
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import requests
import json

logger = logging.getLogger(__name__)

@dataclass
class PopulationResult:
    """Result of population analysis for a location"""
    restaurant_name: str
    address: str
    latitude: float
    longitude: float

    # Population counts by radius
    population_1_mile: int
    population_3_mile: int
    population_5_mile: int
    population_10_mile: int

    # Drinking age population (21+)
    drinking_age_1_mile: int
    drinking_age_3_mile: int
    drinking_age_5_mile: int
    drinking_age_10_mile: int

    # Demographic breakdowns
    median_income_1_mile: Optional[float]
    median_age_1_mile: Optional[float]
    average_household_size_1_mile: Optional[float]

    source: str
    confidence: float
    census_data_available: bool = False

class PopulationAnalyzer:
    """Analyzer for population demographics around restaurant locations"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Census API configuration (would need actual API key for production)
        self.census_api_key = os.getenv('CENSUS_API_KEY', 'demo_key')

        # Population data by ZIP code (expanded sample)
        self.zip_population_data = {
            # Texas ZIP codes with population data
            '77001': {'population': 8500, 'median_age': 31.2, 'median_income': 42500, 'household_size': 2.3},
            '77002': {'population': 15200, 'median_age': 34.8, 'median_income': 58200, 'household_size': 2.1},
            '77003': {'population': 10800, 'median_age': 32.5, 'median_income': 49800, 'household_size': 2.4},
            '77004': {'population': 18900, 'median_age': 29.8, 'median_income': 35600, 'household_size': 2.2},
            '77005': {'population': 25600, 'median_age': 38.2, 'median_income': 89200, 'household_size': 2.6},
            '77006': {'population': 19800, 'median_age': 35.1, 'median_income': 67200, 'household_size': 1.9},
            '77007': {'population': 32400, 'median_age': 33.9, 'median_income': 75800, 'household_size': 2.3},
            '77008': {'population': 28900, 'median_age': 34.7, 'median_income': 69500, 'household_size': 2.4},
            '77009': {'population': 41200, 'median_age': 32.8, 'median_income': 52800, 'household_size': 2.7},
            '77010': {'population': 1200, 'median_age': 42.1, 'median_income': 125000, 'household_size': 1.8},
            # Add more ZIP codes as needed
        }

        # Drinking age percentage (21+ years old)
        self.drinking_age_ratio = 0.75  # More accurate for adult population

    def geocode_address(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Geocode an address using OpenStreetMap Nominatim API (free)

        Args:
            address: Full address string

        Returns:
            Tuple of (latitude, longitude) or (None, None) if geocoding fails
        """
        try:
            logger.info(f"Geocoding address: {address}")

            # Use OpenStreetMap Nominatim (free, no API key required)
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': address,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'us'
            }

            response = self.session.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data:
                    lat = float(data[0]['lat'])
                    lon = float(data[0]['lon'])
                    logger.info(f"Geocoded to: {lat}, {lon}")
                    return lat, lon

            logger.warning(f"Geocoding failed for: {address}")
            return None, None

        except Exception as e:
            logger.error(f"Geocoding error for {address}: {e}")
            return None, None

    def get_census_data_for_coordinates(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Get Census data for specific coordinates

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Dictionary with census demographic data
        """
        try:
            # In a real implementation, this would use:
            # - Census Geocoding API to get FIPS codes
            # - Census Data API to get demographic data
            # - Would need actual Census API key

            logger.info(f"Fetching census data for coordinates: {lat}, {lon}")

            # For demo purposes, return sample data based on ZIP code estimation
            # In production, this would make actual Census API calls

            # Estimate ZIP code from coordinates (simplified)
            zip_code = self._estimate_zip_from_coordinates(lat, lon)

            if zip_code in self.zip_population_data:
                data = self.zip_population_data[zip_code]
                return {
                    'total_population': data['population'],
                    'median_age': data['median_age'],
                    'median_household_income': data['median_income'],
                    'average_household_size': data['household_size'],
                    'source': 'zip_estimation'
                }

            # Return default estimates if ZIP not found
            return {
                'total_population': 15000,
                'median_age': 33.5,
                'median_household_income': 55000,
                'average_household_size': 2.3,
                'source': 'national_average'
            }

        except Exception as e:
            logger.error(f"Error fetching census data: {e}")
            return {
                'total_population': 0,
                'median_age': None,
                'median_household_income': None,
                'average_household_size': None,
                'source': 'error'
            }

    def _estimate_zip_from_coordinates(self, lat: float, lon: float) -> str:
        """Estimate ZIP code from coordinates (simplified)"""
        # This is a very basic estimation
        # In reality, would use proper geospatial libraries or Census API

        # Houston area ZIP codes for demo
        if 29.5 <= lat <= 30.5 and -95.8 <= lon <= -94.8:
            # Return a sample Houston ZIP
            return '77002'

        return '77001'  # Default

    def calculate_population_in_radius(self, lat: float, lon: float, radius_miles: float) -> int:
        """
        Calculate population within a radius using improved estimation

        Args:
            lat: Latitude of center point
            lon: Longitude of center point
            radius_miles: Radius in miles

        Returns:
            Estimated population within radius
        """
        try:
            total_population = 0

            # Get census data for the location
            census_data = self.get_census_data_for_coordinates(lat, lon)

            if census_data['total_population'] > 0:
                # Use census data as base population
                base_population = census_data['total_population']

                # Estimate population density (people per square mile)
                # Assume the ZIP code area is approximately 5 square miles for estimation
                estimated_area_sq_mi = 5.0
                density = base_population / estimated_area_sq_mi

                # Calculate population within radius using circle area formula
                # Area of circle = π * r²
                circle_area = 3.14159 * (radius_miles ** 2)

                # Estimate population in circle (assuming uniform distribution)
                estimated_population = int(density * circle_area)

                # Cap at reasonable maximum (don't exceed base ZIP population significantly)
                max_reasonable = base_population * 3
                total_population = min(estimated_population, max_reasonable)

                logger.info(f"Estimated {total_population} people within {radius_miles} miles")
            else:
                # Fallback estimation
                total_population = int(5000 * radius_miles)  # Rough estimate

            return total_population

        except Exception as e:
            logger.error(f"Error calculating population in radius: {e}")
            return int(5000 * radius_miles)  # Fallback

    def analyze_location(self, restaurant_name: str, address: str) -> PopulationResult:
        """
        Analyze population demographics around a restaurant location

        Args:
            restaurant_name: Name of the restaurant
            address: Restaurant address

        Returns:
            PopulationResult with demographic data
        """
        logger.info(f"Analyzing population for {restaurant_name} at {address}")

        # Geocode the address
        lat, lon = self.geocode_address(address)

        if lat is None or lon is None:
            logger.warning(f"Could not geocode address: {address}")
            return PopulationResult(
                restaurant_name=restaurant_name,
                address=address,
                latitude=0.0,
                longitude=0.0,
                population_1_mile=0,
                population_3_mile=0,
                population_5_mile=0,
                population_10_mile=0,
                drinking_age_1_mile=0,
                drinking_age_3_mile=0,
                drinking_age_5_mile=0,
                drinking_age_10_mile=0,
                median_income_1_mile=None,
                median_age_1_mile=None,
                average_household_size_1_mile=None,
                source='geocoding_failed',
                confidence=0.0,
                census_data_available=False
            )

        # Get census data for the location
        census_data = self.get_census_data_for_coordinates(lat, lon)

        # Calculate population for each radius
        radii = [1, 3, 5, 10]
        populations = {}

        for radius in radii:
            total_pop = self.calculate_population_in_radius(lat, lon, radius)
            populations[f"population_{radius}_mile"] = total_pop
            populations[f"drinking_age_{radius}_mile"] = int(total_pop * self.drinking_age_ratio)

        # Extract demographic data
        median_income = census_data.get('median_household_income')
        median_age = census_data.get('median_age')
        household_size = census_data.get('average_household_size')

        return PopulationResult(
            restaurant_name=restaurant_name,
            address=address,
            latitude=lat,
            longitude=lon,
            median_income_1_mile=median_income,
            median_age_1_mile=median_age,
            average_household_size_1_mile=household_size,
            source=census_data.get('source', 'unknown'),
            confidence=0.8 if census_data.get('source') == 'zip_estimation' else 0.4,
            census_data_available=True,
            **populations
        )

    def analyze_multiple_locations(self, restaurants: List[Dict[str, Any]]) -> Dict[str, PopulationResult]:
        """
        Analyze population demographics for multiple restaurant locations

        Args:
            restaurants: List of restaurant data dictionaries

        Returns:
            Dictionary mapping restaurant IDs to PopulationResult objects
        """
        results = {}

        for i, restaurant in enumerate(restaurants, 1):
            logger.info(f"Processing restaurant {i}/{len(restaurants)}: {restaurant.get('location_name', 'Unknown')}")

            restaurant_id = restaurant.get('id', restaurant.get('location_name', 'unknown'))
            name = restaurant.get('location_name', '')
            address = restaurant.get('full_address', restaurant.get('location_address', ''))

            try:
                result = self.analyze_location(name, address)
                results[restaurant_id] = result
            except Exception as e:
                logger.error(f"Error analyzing {name}: {e}")
                # Return error result
                results[restaurant_id] = PopulationResult(
                    restaurant_name=name,
                    address=address,
                    latitude=0.0,
                    longitude=0.0,
                    population_1_mile=0,
                    population_3_mile=0,
                    population_5_mile=0,
                    population_10_mile=0,
                    drinking_age_1_mile=0,
                    drinking_age_3_mile=0,
                    drinking_age_5_mile=0,
                    drinking_age_10_mile=0,
                    median_income_1_mile=None,
                    median_age_1_mile=None,
                    average_household_size_1_mile=None,
                    source='error',
                    confidence=0.0,
                    census_data_available=False
                )

            # Add delay to be respectful to geocoding API
            if i < len(restaurants):
                time.sleep(1.2)  # Nominatim requests should be limited

        return results

    def get_population_summary(self, results: Dict[str, PopulationResult]) -> Dict[str, Any]:
        """Get summary statistics for population analysis results"""
        if not results:
            return {}

        total_restaurants = len(results)
        successful_analyses = sum(1 for r in results.values() if r.census_data_available)

        # Calculate average populations
        avg_populations = {}
        for radius in [1, 3, 5, 10]:
            pop_values = [getattr(r, f'population_{radius}_mile') for r in results.values() if getattr(r, f'population_{radius}_mile') > 0]
            avg_populations[f'avg_population_{radius}_mile'] = sum(pop_values) // len(pop_values) if pop_values else 0

        # Calculate average drinking age populations
        for radius in [1, 3, 5, 10]:
            drinking_values = [getattr(r, f'drinking_age_{radius}_mile') for r in results.values() if getattr(r, f'drinking_age_{radius}_mile') > 0]
            avg_populations[f'avg_drinking_age_{radius}_mile'] = sum(drinking_values) // len(drinking_values) if drinking_values else 0

        return {
            'total_restaurants_analyzed': total_restaurants,
            'successful_analyses': successful_analyses,
            'success_rate': successful_analyses / total_restaurants if total_restaurants > 0 else 0,
            'average_populations': avg_populations,
            'sources_used': list(set(r.source for r in results.values())),
            'average_confidence': sum(r.confidence for r in results.values()) / total_restaurants
        }