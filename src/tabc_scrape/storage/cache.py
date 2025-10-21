"""
Redis caching service for API responses and geocoding results
"""

import json
import logging
import hashlib
from typing import Any, Optional, Dict, Union
import aioredis
from datetime import timedelta

from ..config import config

logger = logging.getLogger(__name__)

class CacheService:
    """Redis-based caching service using aioredis"""

    def __init__(self):
        self.enabled = config.cache.enabled
        self.client = None

        if self.enabled:
            try:
                # Note: aioredis connection should be established asynchronously
                # For now, we'll set up the connection parameters
                self.redis_url = f"redis://:{config.cache.password}@{config.cache.host}:{config.cache.port}/{config.cache.db}" if config.cache.password else f"redis://{config.cache.host}:{config.cache.port}/{config.cache.db}"
                logger.info(f"Redis cache configured for {config.cache.host}:{config.cache.port}")

            except Exception as e:
                logger.error(f"Error configuring Redis: {e}")
                logger.warning("Caching disabled due to configuration error")
                self.enabled = False

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

        try:
            redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
            key = self._make_key(prefix, identifier)
            cached_value = await redis_client.get(key)

            if cached_value is not None:
                logger.debug(f"Cache hit for key: {key}")
                return self._deserialize_value(cached_value)
            else:
                logger.debug(f"Cache miss for key: {key}")
                return None

        except Exception as e:
            logger.error(f"Redis error during get operation: {e}")
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
            redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
            key = self._make_key(prefix, identifier)
            serialized_value = self._serialize_value(value)

            if ttl is None:
                ttl = config.cache.default_ttl

            success = await redis_client.setex(key, ttl, serialized_value)

            if success:
                logger.debug(f"Cached value for key: {key} (TTL: {ttl}s)")
            else:
                logger.warning(f"Failed to cache value for key: {key}")

            return success

        except Exception as e:
            logger.error(f"Redis error during set operation: {e}")
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

        try:
            redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
            key = self._make_key(prefix, identifier)
            deleted_count = await redis_client.delete(key)

            if deleted_count > 0:
                logger.debug(f"Deleted cache key: {key}")
                return True
            else:
                logger.debug(f"Cache key not found for deletion: {key}")
                return False

        except Exception as e:
            logger.error(f"Redis error during delete operation: {e}")
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

        try:
            redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
            key = self._make_key(prefix, identifier)
            return bool(await redis_client.exists(key))
        except Exception as e:
            logger.error(f"Redis error during exists check: {e}")
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

        try:
            redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
            keys = await redis_client.keys(pattern)
            if keys:
                deleted_count = await redis_client.delete(*keys)
                logger.info(f"Cleared {deleted_count} cache keys matching pattern: {pattern}")
                return True
            return True
        except Exception as e:
            logger.error(f"Redis error during pattern clear: {e}")
            return False

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.enabled:
            return {'enabled': False}

        try:
            redis_client = aioredis.from_url(self.redis_url, decode_responses=True)
            info = await redis_client.info()
            return {
                'enabled': True,
                'connected': True,
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'memory_used': info.get('used_memory_human', '0B'),
                'uptime_days': info.get('uptime_in_days', 0)
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {'enabled': True, 'connected': False, 'error': str(e)}

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