"""
Simple in-memory caching service (Redis fallback)
"""

import json
import logging
import hashlib
import time
from typing import Any, Optional, Dict, Union
from collections import defaultdict
from datetime import datetime, timedelta

from ..config import config

logger = logging.getLogger(__name__)

class CacheService:
    """Simple in-memory caching service (Redis fallback)"""

    def __init__(self):
        self.enabled = True  # Enable simple caching for now
        self._cache = defaultdict(dict)
        self._expiry = defaultdict(dict)

        logger.info("Using simple in-memory cache")

    def _make_key(self, prefix: str, identifier: str) -> str:
        """Generate a cache key with prefix and identifier"""
        # Create a hash of the identifier to keep keys manageable
        key_hash = hashlib.md5(identifier.encode()).hexdigest()
        return f"{prefix}:{key_hash}"

    def _serialize_value(self, value: Any) -> str:
        """Serialize value for storage in Redis"""
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to serialize value: {e}")
            return str(value)

    def _deserialize_value(self, value: str) -> Any:
        """Deserialize value from Redis"""
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            # If JSON deserialization fails, return as string
            return value

    async def get(self, prefix: str, identifier: str) -> Optional[Any]:
        """
        Get a value from cache

        Args:
            prefix: Cache key prefix (e.g., 'api', 'geocode')
            identifier: Unique identifier for the cached item

        Returns:
            Cached value or None if not found/expired
        """
        if not self.enabled:
            return None

        key = self._make_key(prefix, identifier)
        current_time = time.time()

        # Check if key exists and hasn't expired
        if key in self._cache[prefix]:
            expiry_time = self._expiry[prefix].get(key, 0)
            if expiry_time == 0 or current_time < expiry_time:  # 0 means no expiry
                logger.debug(f"Cache hit for key: {key}")
                return self._deserialize_value(self._cache[prefix][key])
            else:
                # Expired, remove it
                del self._cache[prefix][key]
                del self._expiry[prefix][key]

        logger.debug(f"Cache miss for key: {key}")
        return None

    async def set(self, prefix: str, identifier: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a value in cache

        Args:
            prefix: Cache key prefix (e.g., 'api', 'geocode')
            identifier: Unique identifier for the cached item
            value: Value to cache
            ttl: Time to live in seconds (uses default if None)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return False

        try:
            key = self._make_key(prefix, identifier)
            serialized_value = self._serialize_value(value)

            if ttl is None:
                ttl = config.cache.default_ttl

            # Store with expiry time (0 means no expiry)
            self._cache[prefix][key] = serialized_value
            if ttl > 0:
                self._expiry[prefix][key] = time.time() + ttl
            else:
                self._expiry[prefix][key] = 0  # No expiry

            logger.debug(f"Cached value for key: {key} (TTL: {ttl}s)")
            return True

        except Exception as e:
            logger.error(f"Cache error during set operation: {e}")
            return False

    async def delete(self, prefix: str, identifier: str) -> bool:
        """
        Delete a value from cache

        Args:
            prefix: Cache key prefix
            identifier: Unique identifier for the cached item

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return False

        key = self._make_key(prefix, identifier)

        if key in self._cache[prefix]:
            del self._cache[prefix][key]
            if key in self._expiry[prefix]:
                del self._expiry[prefix][key]
            logger.debug(f"Deleted cache key: {key}")
            return True
        else:
            logger.debug(f"Cache key not found for deletion: {key}")
            return False

    async def exists(self, prefix: str, identifier: str) -> bool:
        """
        Check if a key exists in cache

        Args:
            prefix: Cache key prefix
            identifier: Unique identifier for the cached item

        Returns:
            True if key exists, False otherwise
        """
        if not self.enabled:
            return False

        key = self._make_key(prefix, identifier)
        current_time = time.time()

        # Check if key exists and hasn't expired
        if key in self._cache[prefix]:
            expiry_time = self._expiry[prefix].get(key, 0)
            if expiry_time == 0 or current_time < expiry_time:
                return True
            else:
                # Expired, clean it up
                del self._cache[prefix][key]
                del self._expiry[prefix][key]

        return False

    async def clear_pattern(self, pattern: str) -> bool:
        """
        Clear all keys matching a pattern

        Args:
            pattern: Key pattern to match (e.g., 'api:*')

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return False

        # Simple pattern matching (just clear all for now)
        cleared_count = 0
        for prefix in list(self._cache.keys()):
            for key in list(self._cache[prefix].keys()):
                self._cache[prefix].pop(key, None)
                self._expiry[prefix].pop(key, None)
                cleared_count += 1

        logger.info(f"Cleared {cleared_count} cache entries")
        return True

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_entries = sum(len(keys) for keys in self._cache.values())
        return {
            'enabled': True,
            'connected': True,
            'total_entries': total_entries,
            'cache_size_mb': 'N/A (in-memory)',
            'type': 'memory'
        }

# Global cache service instance
cache_service = CacheService()

# Convenience functions for common operations
async def get_api_cache(url: str) -> Optional[Dict[str, Any]]:
    """Get API response from cache"""
    return await cache_service.get('api', url)

async def set_api_cache(url: str, response: Dict[str, Any]) -> bool:
    """Cache API response"""
    return await cache_service.set('api', url, response, config.cache.api_cache_ttl)

async def get_geocode_cache(address: str) -> Optional[Dict[str, float]]:
    """Get geocoding result from cache"""
    return await cache_service.get('geocode', address)

async def set_geocode_cache(address: str, lat: float, lon: float) -> bool:
    """Cache geocoding result"""
    return await cache_service.set('geocode', address, {'lat': lat, 'lon': lon}, config.cache.geocode_cache_ttl)