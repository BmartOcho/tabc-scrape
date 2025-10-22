#!/usr/bin/env python3
"""
Script to run the data enrichment pipeline on the main database
"""

import asyncio
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from tabc_scrape.storage.database import DatabaseManager
from tabc_scrape.storage.enrichment_pipeline import DataEnrichmentPipeline
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_enrichment(limit=None):
    """Run the enrichment pipeline"""
    print(f"Starting enrichment pipeline with limit: {limit}")

    # Initialize database and pipeline with main database
    db = DatabaseManager("sqlite:///src/dev_tabc_restaurants.db")
    pipeline = DataEnrichmentPipeline(db)

    # Run the pipeline
    stats = await pipeline.run_full_enrichment_pipeline(limit=limit)

    print("Enrichment completed!")
    print(f"Total restaurants processed: {stats.total_restaurants}")
    print(f"Successful enrichments: {stats.successful_enrichments}")
    print(f"Failed enrichments: {stats.failed_enrichments}")
    print(f"Total processing time: {stats.total_processing_time:.2f}s")
    print(f"Average time per restaurant: {stats.average_time_per_restaurant:.2f}s")

    if stats.data_sources_used:
        print("Data sources used:")
        for source, count in stats.data_sources_used.items():
            print(f"  - {source}: {count}")

    if stats.error_summary:
        print("Error summary:")
        for error_type, count in stats.error_summary.items():
            print(f"  - {error_type}: {count}")

    return stats

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='Run data enrichment pipeline')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of restaurants to process')
    parser.add_argument('--subset', type=int, default=10, help='Test on subset (default: 10)')

    args = parser.parse_args()

    if args.limit:
        limit = args.limit
    else:
        limit = args.subset

    try:
        stats = asyncio.run(run_enrichment(limit))
        print("\nEnrichment pipeline completed successfully!")
    except KeyboardInterrupt:
        print("\nEnrichment interrupted by user")
    except Exception as e:
        print(f"\nError running enrichment: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()