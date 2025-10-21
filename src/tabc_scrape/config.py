"""
Configuration settings for TABC Restaurant Data Scraper
"""

import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

@dataclass
class APIConfig:
    """Configuration for Texas Comptroller API"""
    base_url: str = "https://data.texas.gov/api/odata/v4/naix-2893"
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.3

@dataclass
class ScrapingConfig:
    """Configuration for web scraping"""
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    timeout: int = 30
    max_retries: int = 3
    delay_between_requests: float = 1.0
    headless_browser: bool = True

@dataclass
class DatabaseConfig:
    """Configuration for database storage"""
    url: str = "sqlite:///tabc_restaurants.db"
    echo: bool = False

@dataclass
class EnrichmentConfig:
    """Configuration for data enrichment"""
    population_radii: Optional[List[int]] = None  # [1, 3, 5, 10] miles

    def __post_init__(self):
        if self.population_radii is None:
            self.population_radii = [1, 3, 5, 10]

class Config:
    """Main configuration class"""

    def __init__(self):
        self.api = APIConfig()
        self.scraping = ScrapingConfig()
        self.database = DatabaseConfig()
        self.enrichment = EnrichmentConfig()

        # Override with environment variables if available
        self._load_from_env()

    def _load_from_env(self):
        """Load configuration from environment variables"""
        api_url = os.getenv('TABC_API_URL')
        if api_url:
            self.api.base_url = api_url

        db_url = os.getenv('TABC_DB_URL')
        if db_url:
            self.database.url = db_url

        scraping_delay = os.getenv('TABC_SCRAPING_DELAY')
        if scraping_delay:
            try:
                self.scraping.delay_between_requests = float(scraping_delay)
            except ValueError:
                pass  # Keep default value if conversion fails

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'api': {
                'base_url': self.api.base_url,
                'timeout': self.api.timeout,
                'max_retries': self.api.max_retries
            },
            'scraping': {
                'timeout': self.scraping.timeout,
                'delay_between_requests': self.scraping.delay_between_requests,
                'headless_browser': self.scraping.headless_browser
            },
            'database': {
                'url': self.database.url,
                'echo': self.database.echo
            },
            'enrichment': {
                'population_radii': self.enrichment.population_radii
            }
        }

# Global configuration instance
config = Config()