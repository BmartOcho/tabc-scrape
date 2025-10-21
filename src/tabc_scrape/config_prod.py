"""
Production Configuration for TABC Restaurant Data Scraper
"""

import os
from .config import APIConfig, ScrapingConfig, DatabaseConfig, EnrichmentConfig, Config

# Production-specific settings
prod_config = Config(
    api=APIConfig(
        base_url=os.getenv('TABC_API_URL', 'https://data.texas.gov/api/odata/v4/naix-2893'),
        timeout=60,  # Longer timeout for production
        max_retries=5,  # More retries for production
        backoff_factor=0.5
    ),
    scraping=ScrapingConfig(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (Production)',
        timeout=60,
        max_retries=5,
        delay_between_requests=2.0,  # Longer delay to be respectful
        headless_browser=True
    ),
    database=DatabaseConfig(
        url=os.getenv('TABC_DB_URL', 'sqlite:///prod_tabc_restaurants.db'),
        echo=False  # Disable SQL echo for production
    ),
    enrichment=EnrichmentConfig(
        population_radii=[1, 3, 5, 10]
    )
)