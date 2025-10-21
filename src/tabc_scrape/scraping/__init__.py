"""
Web scraping module for restaurant data enrichment
"""

from .square_footage import SquareFootageScraper
from .concept_classifier import EnhancedRestaurantConceptClassifier, ConceptClassification, WebSourceData

__all__ = ['SquareFootageScraper', 'RestaurantConceptClassifier']