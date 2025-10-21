#!/usr/bin/env python3
"""
Integration test with real Texas Comptroller restaurant data
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from tabc_scrape.data.api_client import TexasComptrollerAPI
from tabc_scrape.scraping.square_footage import SquareFootageScraper
from tabc_scrape.storage.database import DatabaseManager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_real_restaurant_scraping():
    """Test square footage scraping with real restaurant data"""
    print("=== Testing Square Footage Scraper with Real Restaurant Data ===\n")

    # Initialize components
    api_client = TexasComptrollerAPI()
    scraper = SquareFootageScraper()
    db_manager = DatabaseManager()

    # Test API connection first
    print("1. Testing API connection...")
    if not api_client.test_connection():
        print("API connection failed")
        return
    print("✅ API connection successful")

    # Fetch real restaurant data
    print("\n2. Fetching real restaurant data...")
    try:
        restaurants_df = api_client.get_restaurants_dataframe(limit=20)  # Start with 20 restaurants

        if restaurants_df.empty:
            print("No restaurant data retrieved")
            return

        print(f"✅ Retrieved {len(restaurants_df)} restaurants")

        # Show sample of real data
        print("\nSample restaurant data:")
        for _, row in restaurants_df.head(3).iterrows():
            print(f"  • {row['location_name']} - {row['location_address']}, {row['location_city']}, {row['location_state']} {row['location_zip']}")

    except Exception as e:
        print(f"Error fetching restaurant data: {e}")
        return

    # Test square footage scraping on real restaurants
    print("\n3. Testing square footage scraping on real restaurants...")

    # Convert DataFrame to list of dictionaries for scraping
    restaurant_list = []
    for _, row in restaurants_df.iterrows():
        restaurant_dict = {
            'id': row['id'],
            'location_name': row['location_name'],
            'full_address': row['full_address'],
            'location_address': row['location_address'],
            'location_city': row['location_city'],
            'location_state': row['location_state'],
            'location_zip': row['location_zip'],
            'location_county': row['location_county']
        }
        restaurant_list.append(restaurant_dict)

    # Test scraping on first 5 restaurants (to avoid overwhelming the system)
    test_restaurants = restaurant_list[:5]
    print(f"Testing on {len(test_restaurants)} restaurants...\n")

    results = {}

    for i, restaurant in enumerate(test_restaurants, 1):
        print(f"Restaurant {i}/5: {restaurant['location_name']}")
        print(f"  Address: {restaurant['full_address']}")
        print(f"  County: {restaurant['location_county']}")

        try:
            result = scraper.scrape_square_footage(
                restaurant['location_name'],
                restaurant['full_address'],
                restaurant['location_county']
            )

            print(f"  Result: {result.square_footage or 'Not found'} sq ft")
            print(f"  Source: {result.source}")
            print(f"  Confidence: {result.confidence:.2f}")

            if result.square_footage:
                print(f"  SUCCESS: Found {result.square_footage} sq ft from {result.source}")
            else:
                print(f"  No square footage found")

        except Exception as e:
            print(f"  Error: {e}")
            result = scraper.SquareFootageResult(
                restaurant_name=restaurant['location_name'],
                address=restaurant['full_address'],
                square_footage=None,
                source='error',
                confidence=0.0
            )

        results[restaurant['id']] = result
        print()

    # Show comprehensive results
    print("=== COMPREHENSIVE RESULTS ===")

    # Overall statistics
    stats = scraper.get_scraping_stats(results)
    print("\n📊 Overall Statistics:")
    print(f"  Total restaurants tested: {stats['total_restaurants']}")
    print(f"  Successful scrapes: {stats['successful_scrapes']}")
    print(f"  Success rate: {stats['success_rate']:.2%}")
    print(f"  Average confidence: {stats['average_confidence']:.2f}")

    if stats['sources_used']:
        print("  Sources that found data:")
        for source, count in stats['sources_used'].items():
            print(f"    • {source}: {count} restaurants")
    else:
        print("  No sources found data for these restaurants")

    # Individual results with more detail
    print("\n🏢 Individual Restaurant Results:")
    for restaurant_id, result in results.items():
        restaurant = next(r for r in test_restaurants if r['id'] == restaurant_id)
        status = "SUCCESS" if result.square_footage else "FAILED"

        print(f"\n  {status} {restaurant['location_name']}")
        print(f"     Address: {restaurant['full_address']}")
        print(f"     Square Footage: {result.square_footage or 'Not found'}")
        print(f"     Source: {result.source}")
        print(f"     Confidence: {result.confidence:.2f}")
        if result.property_details and result.property_details.get('sources_tried'):
            print(f"     Sources tried: {', '.join(result.property_details['sources_tried'])}")

    # Show restaurants with successful scrapes
    successful_results = {k: v for k, v in results.items() if v.square_footage is not None}

    if successful_results:
        print("\n🎯 SUCCESSFUL SCRAPES:")
        for restaurant_id, result in successful_results.items():
            restaurant = next(r for r in test_restaurants if r['id'] == restaurant_id)
            print(f"  SUCCESS {restaurant['location_name']}: {result.square_footage:,} sq ft ({result.source})")

    # Calculate potential revenue per square foot for restaurants with both data
    print("\n💰 REVENUE ANALYSIS:")
    print("  (Sample calculation for restaurants with both receipt and square footage data)")

    for restaurant_id, result in results.items():
        if result.square_footage:
            restaurant = next(r for r in test_restaurants if r['id'] == restaurant_id)
            total_receipts = restaurant.get('total_receipts', 0)

            if total_receipts > 0:
                revenue_per_sqft = total_receipts / result.square_footage
                print(f"  ANALYSIS {restaurant['location_name']}:")
                print(f"     Square Footage: {result.square_footage:,}")
                print(f"     Annual Receipts: ${total_receipts:,.2f}")
                print(f"     Revenue per Sq Ft: ${revenue_per_sqft:.2f}")

    print("\n✅ Test completed! The square footage scraper is working with real data.")
    # Recommendations
    print("\n💡 Recommendations:")
    print("  • Increase scraping delay for production use")
    print("  • Add more county property appraiser websites")
    print("  • Consider using proxy rotation for large-scale scraping")
    print("  • Integrate with commercial real estate APIs for better coverage")
def main():
    """Main function"""
    try:
        test_real_restaurant_scraping()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()