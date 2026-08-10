"""Redis cache: replaces the file-based weather cache.

The file cache works but has three production problems:
  1. Doesn't survive container restarts (pods lose local disk)
  2. Can't be shared across parallel pipeline instances
  3. No real TTL enforcement (relies on date-partitioned paths)

Redis fixes all three. Same key structure, same logic, real TTL.

Setup:
  pip install redis
  Add to .env: REDIS_URL=redis://localhost:6379/0
  (or Redis Cloud / Memorystore URL for production)

Local Redis for development:
  brew install redis && brew services start redis   # macOS
  OR
  docker run -d -p 6379:6379 redis                  # Docker
"""
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

_client = None


def _get_redis():
    global _client
    if _client is None:
        import redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _client = redis.from_url(url, decode_responses=True)
        # verify connection
        _client.ping()
    return _client


def _cache_key(hazard, provider, lat, lng, day):
    """Same structure as the file cache, but as a Redis key.

    Format: eraas:weather:{day}:{hazard}:{provider}:{lat}_{lng}
    The provider is in the key (lesson C5: prevents shape mismatches).
    """
    return f"eraas:weather:{day}:{hazard}:{provider}:{lat}_{lng}"


def get_cached(hazard, provider, lat, lng, day):
    """Try to read from Redis. Returns (data, True) or (None, False)."""
    try:
        r = _get_redis()
        key = _cache_key(hazard, provider, lat, lng, day)
        val = r.get(key)
        if val:
            return json.loads(val), True
        return None, False
    except Exception:
        # Redis down? Fall through to API call — cache is non-blocking
        return None, False


def set_cached(hazard, provider, lat, lng, day, data, ttl_hours=24):
    """Write to Redis with TTL. Non-blocking: if Redis is down, we skip."""
    try:
        r = _get_redis()
        key = _cache_key(hazard, provider, lat, lng, day)
        r.setex(key, int(ttl_hours * 3600), json.dumps(data))
        return True
    except Exception:
        return False


def get_cache_stats(day=None):
    """Count cached entries. Useful for monitoring."""
    try:
        r = _get_redis()
        if day is None:
            day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        pattern = f"eraas:weather:{day}:*"
        keys = list(r.scan_iter(match=pattern, count=1000))
        return {"day": day, "cached_entries": len(keys)}
    except Exception as e:
        return {"error": str(e)}


def flush_day(day):
    """Clear all cache entries for a specific day."""
    try:
        r = _get_redis()
        pattern = f"eraas:weather:{day}:*"
        keys = list(r.scan_iter(match=pattern, count=1000))
        if keys:
            r.delete(*keys)
        return len(keys)
    except Exception:
        return 0


def close():
    global _client
    if _client:
        _client.close()
        _client = None