"""
Staging Configuration for TABC Restaurant Data Scraper
"""

import os
from .config import APIConfig, ScrapingConfig, DatabaseConfig, EnrichmentConfig, Config

# Staging-specific settings
staging_config = Config(
    api=APIConfig(
        base_url=os.getenv('TABC_API_URL', 'https://data.texas.gov/api/odata/v4/naix-2893'),
        timeout=30,
        max_retries=3,
        backoff_factor=0.3
    ),
    scraping=ScrapingConfig(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (Staging)',
        timeout=30,
        max_retries=3,
        delay_between_requests=1.0,
        headless_browser=True
    ),
    database=DatabaseConfig(
        url=os.getenv('TABC_DB_URL', 'sqlite:///staging_tabc_restaurants.db'),
        echo=False  # Disable SQL echo for staging
    ),
    enrichment=EnrichmentConfig(
        population_radii=[1, 3, 5, 10]
    )
)