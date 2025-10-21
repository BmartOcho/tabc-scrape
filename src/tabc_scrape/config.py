"""
Configuration settings for TABC Restaurant Data Scraper
"""

import os
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, validator
from urllib.parse import urlparse

class APIConfig(BaseModel):
    """Configuration for Texas Comptroller API"""
    base_url: str = Field(default="https://data.texas.gov/api/odata/v4/naix-2893", description="API base URL")
    timeout: int = Field(default=30, ge=1, le=300, description="Request timeout in seconds")
    max_retries: int = Field(default=3, ge=1, le=10, description="Maximum retry attempts")
    backoff_factor: float = Field(default=0.3, ge=0.1, le=5.0, description="Backoff factor for retries")

    @validator('base_url')
    def validate_base_url(cls, v):
        parsed = urlparse(v)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError('Invalid URL format')
        if parsed.scheme not in ['http', 'https']:
            raise ValueError('URL must use HTTP or HTTPS')
        return v

class ScrapingConfig(BaseModel):
    """Configuration for web scraping"""
    user_agent: str = Field(default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", description="User agent string")
    timeout: int = Field(default=30, ge=1, le=300, description="Request timeout in seconds")
    max_retries: int = Field(default=3, ge=1, le=10, description="Maximum retry attempts")
    delay_between_requests: float = Field(default=1.0, ge=0.1, le=60.0, description="Delay between requests in seconds")
    headless_browser: bool = Field(default=True, description="Use headless browser")

class DatabaseConfig(BaseModel):
    """Configuration for database storage"""
    url: str = Field(default="sqlite:///tabc_restaurants.db", description="Database URL")
    echo: bool = Field(default=False, description="Enable SQL echo for debugging")

    @validator('url')
    def validate_database_url(cls, v):
        if not v:
            raise ValueError('Database URL cannot be empty')
        parsed = urlparse(v)
        if parsed.scheme == 'sqlite' and not parsed.path:
            raise ValueError('SQLite URL must include a path')
        # For other DBs, ensure credentials are not logged (though not validated here)
        return v

class EnrichmentConfig(BaseModel):
    """Configuration for data enrichment"""
    population_radii: List[int] = Field(default_factory=lambda: [1, 3, 5, 10], description="Population radii in miles")

    @validator('population_radii')
    def validate_population_radii(cls, v):
        if not v:
            raise ValueError('Population radii cannot be empty')
        for radius in v:
            if not (0 < radius <= 100):
                raise ValueError('Each radius must be between 1 and 100 miles')
        return v

class Config(BaseModel):
    """Main configuration class"""
    api: APIConfig = Field(default_factory=APIConfig)
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    enrichment: EnrichmentConfig = Field(default_factory=EnrichmentConfig)

    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables with validation"""
        env_vars = {
            'api': {
                'base_url': os.getenv('TABC_API_URL', 'https://data.texas.gov/api/odata/v4/naix-2893'),
                'timeout': int(os.getenv('TABC_API_TIMEOUT', '30')),
                'max_retries': int(os.getenv('TABC_API_MAX_RETRIES', '3')),
                'backoff_factor': float(os.getenv('TABC_API_BACKOFF_FACTOR', '0.3'))
            },
            'scraping': {
                'user_agent': os.getenv('TABC_SCRAPING_USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
                'timeout': int(os.getenv('TABC_SCRAPING_TIMEOUT', '30')),
                'max_retries': int(os.getenv('TABC_SCRAPING_MAX_RETRIES', '3')),
                'delay_between_requests': float(os.getenv('TABC_SCRAPING_DELAY', '1.0')),
                'headless_browser': os.getenv('TABC_SCRAPING_HEADLESS', 'true').lower() == 'true'
            },
            'database': {
                'url': os.getenv('TABC_DB_URL', 'sqlite:///tabc_restaurants.db'),
                'echo': os.getenv('TABC_DB_ECHO', 'false').lower() == 'true'
            },
            'enrichment': {
                'population_radii': [int(r) for r in os.getenv('TABC_ENRICHMENT_RADII', '1,3,5,10').split(',')]
            }
        }
        return cls(**env_vars)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary (excluding sensitive data)"""
        return {
            'api': {
                'base_url': self.api.base_url,
                'timeout': self.api.timeout,
                'max_retries': self.api.max_retries,
                'backoff_factor': self.api.backoff_factor
            },
            'scraping': {
                'user_agent': self.scraping.user_agent,
                'timeout': self.scraping.timeout,
                'max_retries': self.scraping.max_retries,
                'delay_between_requests': self.scraping.delay_between_requests,
                'headless_browser': self.scraping.headless_browser
            },
            'database': {
                'url': self.database.url.replace('://', '://***:***@') if '://' in self.database.url and '@' in self.database.url else self.database.url,  # Mask credentials
                'echo': self.database.echo
            },
            'enrichment': {
                'population_radii': self.enrichment.population_radii
            }
        }

# Global configuration instance
def load_config():
    """Load configuration based on environment"""
    environment = os.getenv('ENVIRONMENT', 'dev').lower()
    if environment == 'dev':
        from .config_dev import dev_config
        return dev_config
    elif environment == 'staging':
        from .config_staging import staging_config
        return staging_config
    elif environment == 'prod':
        from .config_prod import prod_config
        return prod_config
    else:
        # Fallback to default from env
        return Config.from_env()

config = load_config()