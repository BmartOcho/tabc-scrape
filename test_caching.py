#!/usr/bin/env python3
"""
Test script for Redis caching functionality
"""

import sys
import os
import logging
from unittest.mock import Mock, patch

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from tabc_scrape.config import config
from tabc_scrape.storage.cache import CacheService, get_api_cache, set_api_cache, get_geocode_cache, set_geocode_cache

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_cache_service():
    """Test the cache service functionality"""
    print("=== Testing Cache Service ===")

    # Test cache service initialization
    cache = CacheService()
    print(f"Cache enabled: {cache.enabled}")

    if not cache.enabled:
        print("⚠️  Redis not available - testing with mocked cache")
        return test_mocked_cache()

    # Test basic cache operations
    test_key = "test_key"
    test_value = {"test": "data", "number": 42}

    # Test set operation
    success = cache.set("test", test_key, test_value, ttl=60)
    print(f"Set operation success: {success}")

    # Test exists operation
    exists = cache.exists("test", test_key)
    print(f"Key exists: {exists}")

    # Test get operation
    retrieved_value = cache.get("test", test_key)
    print(f"Retrieved value: {retrieved_value}")

    # Test delete operation
    delete_success = cache.delete("test", test_key)
    print(f"Delete operation success: {delete_success}")

    # Verify deletion
    exists_after_delete = cache.exists("test", test_key)
    print(f"Key exists after delete: {exists_after_delete}")

    print("✅ Cache service tests completed")
    return True

def test_mocked_cache():
    """Test cache functionality with mocked Redis"""
    print("\n=== Testing with Mocked Redis ===")

    # Mock the Redis client
    mock_redis = Mock()
    mock_redis.ping.return_value = True
    mock_redis.get.return_value = None
    mock_redis.setex.return_value = True
    mock_redis.delete.return_value = 1
    mock_redis.exists.return_value = False
    mock_redis.info.return_value = {
        'keyspace_hits': 10,
        'keyspace_misses': 5,
        'used_memory_human': '1M',
        'uptime_in_days': 1
    }

    with patch('redis.Redis', return_value=mock_redis):
        cache = CacheService()

        # Test basic operations
        test_value = {"test": "data"}

        # Test set
        success = cache.set("test", "key1", test_value)
        print(f"Mock set success: {success}")

        # Test get (should return None for new key)
        value = cache.get("test", "key1")
        print(f"Mock get (cache miss): {value}")

        # Simulate cache hit
        mock_redis.get.return_value = '{"test": "data"}'
        mock_redis.exists.return_value = True
        value = cache.get("test", "key1")
        print(f"Mock get (cache hit): {value}")

        # Test delete
        delete_success = cache.delete("test", "key1")
        print(f"Mock delete success: {delete_success}")

    print("✅ Mocked cache tests completed")
    return True

def test_api_caching():
    """Test API response caching"""
    print("\n=== Testing API Caching ===")

    # Test data
    test_url = "https://api.example.com/test"
    test_response = {"data": "test response", "status": "success"}

    # Test cache miss
    cached_response = get_api_cache(test_url)
    print(f"Cache miss (no cached data): {cached_response}")

    # Test cache set
    success = set_api_cache(test_url, test_response)
    print(f"Cache set success: {success}")

    # Test cache hit (if Redis is available)
    if success:
        cached_response = get_api_cache(test_url)
        print(f"Cache hit: {cached_response}")

    print("✅ API caching tests completed")
    return True

def test_geocoding_caching():
    """Test geocoding result caching"""
    print("\n=== Testing Geocoding Caching ===")

    # Test address
    test_address = "1600 Pennsylvania Avenue NW, Washington, DC 20500"

    # Test cache miss
    cached_result = get_geocode_cache(test_address)
    print(f"Geocode cache miss: {cached_result}")

    # Test cache set
    test_lat, test_lon = 38.8977, -77.0365
    success = set_geocode_cache(test_address, test_lat, test_lon)
    print(f"Geocode cache set success: {success}")

    # Test cache hit (if Redis is available)
    if success:
        cached_result = get_geocode_cache(test_address)
        print(f"Geocode cache hit: {cached_result}")

    print("✅ Geocoding caching tests completed")
    return True

def test_configuration():
    """Test cache configuration"""
    print("\n=== Testing Cache Configuration ===")

    print(f"Cache host: {config.cache.host}")
    print(f"Cache port: {config.cache.port}")
    print(f"Cache database: {config.cache.db}")
    print(f"Cache enabled: {config.cache.enabled}")
    print(f"Default TTL: {config.cache.default_ttl}s")
    print(f"API cache TTL: {config.cache.api_cache_ttl}s")
    print(f"Geocode cache TTL: {config.cache.geocode_cache_ttl}s")

    print("✅ Configuration tests completed")
    return True

def main():
    """Run all cache tests"""
    print("🧪 Starting Redis Caching Tests")
    print("=" * 50)

    try:
        # Test configuration
        test_configuration()

        # Test cache service
        test_cache_service()

        # Test API caching
        test_api_caching()

        # Test geocoding caching
        test_geocoding_caching()

        print("\n" + "=" * 50)
        print("🎉 All caching tests completed successfully!")
        print("\n📋 Next Steps:")
        print("1. Install and start Redis server:")
        print("   - Ubuntu/Debian: sudo apt install redis-server")
        print("   - macOS: brew install redis")
        print("   - Windows: Download from https://redis.io/download")
        print("   - Docker: docker run -d -p 6379:6379 redis:alpine")
        print("\n2. Install Redis Python client:")
        print("   pip install redis>=4.5.0,<5.0.0")
        print("\n3. Set environment variables (optional):")
        print("   TABC_CACHE_HOST=localhost")
        print("   TABC_CACHE_PORT=6379")
        print("   TABC_CACHE_DB=0")
        print("   TABC_CACHE_PASSWORD=your_password")
        print("   TABC_CACHE_ENABLED=true")

        return True

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)