"""
TABC Restaurant Data Scraper and Enrichment System

This package provides tools to:
1. Collect restaurant data from Texas Comptroller API
2. Enrich data with square footage information
3. Calculate population demographics within radius
4. Classify restaurant concepts
5. Store and analyze enriched data
"""

__version__ = "1.0.0"
__author__ = "TABC Scraper System"

from .data.api_client import TexasComptrollerAPI
from .scraping.square_footage import SquareFootageScraper
from .scraping.concept_classifier import EnhancedRestaurantConceptClassifier
from .analysis.population import PopulationAnalyzer
from .storage.database import DatabaseManager

__all__ = [
    'TexasComptrollerAPI',
    'SquareFootageScraper',
    'EnhancedRestaurantConceptClassifier',
    'PopulationAnalyzer',
    'DatabaseManager'
]