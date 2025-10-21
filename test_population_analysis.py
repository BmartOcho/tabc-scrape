#!/usr/bin/env python3
"""
Test script for population analysis functionality
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from tabc_scrape.analysis.population import PopulationAnalyzer
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_geocoding():
    """Test the geocoding functionality"""
    print("=== Testing Geocoding Functionality ===\n")

    analyzer = PopulationAnalyzer()

    test_addresses = [
        "1600 Pennsylvania Avenue NW, Washington, DC 20500",
        "5945 Bellaire Blvd, Houston, TX 77081",
        "1310 W Campbell Rd, Richardson, TX 75080",
        "8427 Boulevard 26, North Richland Hills, TX 76180"
    ]

    print("Testing address geocoding:")
    for i, address in enumerate(test_addresses, 1):
        print(f"\n{i}. {address}")
        try:
            lat, lon = analyzer.geocode_address(address)
            if lat and lon:
                print(f"   SUCCESS: Geocoded to: {lat:.4f}, {lon:.4f}")
            else:
                print("   FAILED: Geocoding failed")
        except Exception as e:
            print(f"   ERROR: {e}")

def test_population_analysis():
    """Test population analysis with sample restaurants"""
    print("\n=== Testing Population Analysis ===\n")

    analyzer = PopulationAnalyzer()

    # Sample restaurant data
    test_restaurants = [
        {
            'id': 'test_1',
            'location_name': 'Honduras Maya Cafe',
            'full_address': '5945 Bellaire Blvd, Houston, TX 77081',
            'location_county': 'Harris'
        },
        {
            'id': 'test_2',
            'location_name': 'Mermaid Karaoke',
            'full_address': '1310 W Campbell Rd, Richardson, TX 75080',
            'location_county': 'Dallas'
        },
        {
            'id': 'test_3',
            'location_name': 'Japanese Grill',
            'full_address': '8427 Boulevard 26, North Richland Hills, TX 76180',
            'location_county': 'Tarrant'
        }
    ]

    print(f"Testing population analysis for {len(test_restaurants)} restaurants...\n")

    results = {}

    for restaurant in test_restaurants:
        print(f"Analyzing: {restaurant['location_name']}")
        print(f"Address: {restaurant['full_address']}")

        try:
            result = analyzer.analyze_location(
                restaurant['location_name'],
                restaurant['full_address']
            )

            print(f"  Location: {result.latitude:.4f}, {result.longitude:.4f}")
            print(f"  Population (1 mile): {result.population_1_mile:,}")
            print(f"  Population (3 miles): {result.population_3_mile:,}")
            print(f"  Population (5 miles): {result.population_5_mile:,}")
            print(f"  Population (10 miles): {result.population_10_mile:,}")
            print(f"  Drinking age (1 mile): {result.drinking_age_1_mile:,}")
            print(f"  Median age: {result.median_age_1_mile}")
            print(f"  Median income: ${result.median_income_1_mile:,}")
            print(f"  Confidence: {result.confidence:.2f}")
            print(f"  Source: {result.source}")

        except Exception as e:
            print(f"  Error: {e}")
            result = None

        results[restaurant['id']] = result
        print()

    # Show summary statistics
    print("=== POPULATION ANALYSIS SUMMARY ===")

    successful_analyses = sum(1 for r in results.values() if r and r.census_data_available)

    print("\n📊 Analysis Results:")
    print(f"  Restaurants analyzed: {len(test_restaurants)}")
    print(f"  Successful geocoding: {successful_analyses}")
    print(f"  Success rate: {successful_analyses/len(test_restaurants):.1%}")

    if successful_analyses > 0:
        # Calculate averages
        avg_pop_1 = sum(r.population_1_mile for r in results.values() if r and r.population_1_mile > 0) // successful_analyses
        avg_pop_3 = sum(r.population_3_mile for r in results.values() if r and r.population_3_mile > 0) // successful_analyses
        avg_pop_5 = sum(r.population_5_mile for r in results.values() if r and r.population_5_mile > 0) // successful_analyses

        print("\n📈 Average Populations:")
        print(f"  1-mile radius: {avg_pop_1:,}")
        print(f"  3-mile radius: {avg_pop_3:,}")
        print(f"  5-mile radius: {avg_pop_5:,}")

        # Show detailed results for successful analyses
        print("\n🏘️  Detailed Results:")
        for restaurant_id, result in results.items():
            if result and result.census_data_available:
                restaurant = next(r for r in test_restaurants if r['id'] == restaurant_id)
                print(f"\n  📍 {restaurant['location_name']}:")
                print(f"     1-mile population: {result.population_1_mile:,} ({result.drinking_age_1_mile:,} drinking age)")
                print(f"     3-mile population: {result.population_3_mile:,} ({result.drinking_age_3_mile:,} drinking age)")
                print(f"     5-mile population: {result.population_5_mile:,} ({result.drinking_age_5_mile:,} drinking age)")
                print(f"     10-mile population: {result.population_10_mile:,} ({result.drinking_age_10_mile:,} drinking age)")
                print(f"     Median age: {result.median_age_1_mile}")
                print(f"     Median income: ${result.median_income_1_mile:,}")

def test_batch_analysis():
    """Test batch population analysis"""
    print("\n=== Testing Batch Population Analysis ===\n")

    analyzer = PopulationAnalyzer()

    # Create a larger sample of restaurants
    sample_restaurants = [
        {'id': f'sample_{i}', 'location_name': f'Restaurant_{i}', 'full_address': f'{1000+i} Main St, City_{i}, TX 77{i:03d}'}
        for i in range(1, 6)
    ]

    print(f"Running batch analysis on {len(sample_restaurants)} restaurants...")

    try:
        results = analyzer.analyze_multiple_locations(sample_restaurants)

        # Get summary
        summary = analyzer.get_population_summary(results)

        print("\n📊 Batch Analysis Summary:")
        print(f"  Total restaurants: {summary['total_restaurants_analyzed']}")
        print(f"  Successful analyses: {summary['successful_analyses']}")
        print(f"  Success rate: {summary['success_rate']:.1%}")
        print(f"  Average confidence: {summary['average_confidence']:.2f}")

        print("\n📈 Average Populations:")
        for key, value in summary['average_populations'].items():
            if 'drinking' in key:
                radius = key.split('_')[-2]  # Extract radius from key like 'drinking_age_1_mile'
                print(f"  {radius}-mile drinking age: {value:,}")
            elif 'population' in key:
                radius = key.split('_')[-2]
                print(f"  {radius}-mile total: {value:,}")

        print("\n✅ Batch analysis completed successfully!")
    except Exception as e:
        print(f"❌ Batch analysis failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main test function"""
    try:
        test_geocoding()
        test_population_analysis()
        test_batch_analysis()

        print("\n🎉 All population analysis tests completed!")
        print("\n💡 Key Features Demonstrated:")
        print("  • Address geocoding using OpenStreetMap")
        print("  • Population estimation within multiple radii")
        print("  • Drinking age population calculations")
        print("  • Demographic data integration")
        print("  • Batch processing capabilities")
        print("  • Error handling and recovery")
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
    except Exception as e:
        print(f"\n\nTests failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()