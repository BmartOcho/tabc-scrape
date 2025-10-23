#!/usr/bin/env python3
"""
Test script for the square footage scraper
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from tabc_scrape.scraping.square_footage import SquareFootageScraper
import logging
import pytest

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_square_footage_scraping():
    """Test the square footage scraping functionality"""
    print("=== Testing Square Footage Scraper ===\n")

    # Initialize scraper
    scraper = SquareFootageScraper()

    # Test cases with known restaurants
    test_restaurants = [
        {
            'id': 'test_1',
            'location_name': 'McDonalds',
            'full_address': '123 Main St, Houston, TX 77001',
            'location_county': 'Harris'
        },
        {
            'id': 'test_2',
            'location_name': 'Chipotle Mexican Grill',
            'full_address': '456 Oak Ave, Dallas, TX 75201',
            'location_county': 'Dallas'
        },
        {
            'id': 'test_3',
            'location_name': 'Starbucks Coffee',
            'full_address': '789 Pine St, Austin, TX 78701',
            'location_county': 'Travis'
        }
    ]

    print("Testing square footage scraping for sample restaurants...\n")

    # Test individual restaurant scraping
    for restaurant in test_restaurants[:1]:  # Test just the first one for now
        print(f"Testing: {restaurant['location_name']}")
        print(f"Address: {restaurant['full_address']}")
        print(f"County: {restaurant['location_county']}")

        try:
            result = await scraper.scrape_square_footage(
                restaurant['location_name'],
                restaurant['full_address'],
                restaurant['location_county']
            )

            print(f"Result: {result.square_footage} sq ft")
            print(f"Source: {result.source}")
            print(f"Confidence: {result.confidence:.2f}")
            print(f"Sources tried: {result.property_details.get('sources_tried', []) if result.property_details else 'None'}")

        except Exception as e:
            print(f"Error: {e}")

        print("-" * 50)

    # Test multiple restaurant scraping
    print("\nTesting batch scraping...")
    try:
        results = await scraper.scrape_multiple_restaurants(test_restaurants)

        # Print statistics
        stats = scraper.get_scraping_stats(results)
        print("\nScraping Statistics:")
        print(f"Total restaurants: {stats['total_restaurants']}")
        print(f"Successful scrapes: {stats['successful_scrapes']}")
        print(f"Success rate: {stats['success_rate']:.2%}")
        print(f"Average confidence: {stats['average_confidence']:.2f}")
        print(f"Sources used: {stats['sources_used']}")

        # Print individual results
        print("\nIndividual Results:")
        for restaurant_id, result in results.items():
            status = "SUCCESS" if result.square_footage else "FAILED"
            sqft = result.square_footage or "Not found"
            print(f"  {status} {restaurant_id}: {sqft} sq ft ({result.source}, confidence: {result.confidence:.2f})")

    except Exception as e:
        print(f"Error in batch scraping: {e}")
        import traceback
        traceback.print_exc()

def test_text_extraction():
    """Test the text extraction functionality"""
    print("\n=== Testing Text Extraction ===")

    scraper = SquareFootageScraper()

    # Test cases with sample text containing square footage
    test_texts = [
        "This restaurant is 2,500 square feet and serves great food.",
        "Building size: 5,000 SF with modern amenities.",
        "The property is approximately 1,200 sq ft.",
        "Restaurant size 3500 square feet in prime location.",
        "No size information here."
    ]

    print("Testing square footage extraction from text:")
    for i, text in enumerate(test_texts, 1):
        sqft = scraper._extract_square_footage_from_text(text)
        print(f"  Test {i}: '{text}' -> {sqft} sq ft")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_square_footage_scraping())
    test_text_extraction()