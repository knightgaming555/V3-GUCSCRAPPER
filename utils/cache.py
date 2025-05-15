# utils/cache.py
import redis
import json
import logging
import base64  # For simple binary caching
import time
import pickle

from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

redis_client = None
try:
    # Initialize Redis client - decode_responses=False for binary/pickle/base64
    redis_client = redis.from_url(
        config.REDIS_URL, decode_responses=False, socket_timeout=10
    )  # Keep a reasonable timeout
    redis_client.ping()
    logger.info(f"Utils/Cache: Successfully connected to Redis at {config.REDIS_URL}")
except redis.exceptions.ConnectionError as e:
    logger.critical(
        f"Utils/Cache: Failed to connect to Redis: {e}. Caching will be disabled.",
        exc_info=True,
    )
    redis_client = None
except Exception as e:
    logger.critical(f"Utils/Cache: Error initializing Redis client: {e}", exc_info=True)
    redis_client = None


def get_from_cache(key: str):
    """Retrieves and decodes JSON data from Redis cache."""
    if not redis_client:
        logger.warning("Redis client not available, cannot get from cache.")
        return None
    try:
        cached_bytes = redis_client.get(key.encode("utf-8"))  # GET expects bytes key
        if cached_bytes:
            try:
                # Assume cached data is JSON encoded UTF-8 string
                return json.loads(cached_bytes.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(
                    f"[Cache] Error decoding JSON for key '{key}': {e}. Data: {cached_bytes[:100]!r}"
                )
                # Optionally delete corrupted key
                # delete_from_cache(key)
                return None
        return None
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Cache] Redis connection error on get '{key}': {e}")
    except Exception as e:
        logger.error(f"[Cache] Error getting key '{key}': {e}", exc_info=True)
    return None


def set_in_cache(key: str, value, timeout: int = config.CACHE_DEFAULT_TIMEOUT):
    """Encodes value to JSON and stores it in Redis cache with expiry."""
    if not redis_client:
        logger.warning("Redis client not available, cannot set cache.")
        return False
    try:
        value_json = json.dumps(value, ensure_ascii=False)
        value_bytes = value_json.encode("utf-8")
        # SETEX expects bytes key and value
        redis_client.setex(key.encode("utf-8"), timeout, value_bytes)
        logger.debug(f"Set cache for key '{key}' with timeout {timeout}s")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Cache] Redis connection error on set '{key}': {e}")
    except TypeError as e:
        logger.error(
            f"[Cache] Failed to serialize value to JSON for key '{key}': {e}",
            exc_info=True,
        )
    except Exception as e:
        logger.error(f"[Cache] Error setting key '{key}': {e}", exc_info=True)
    return False


def delete_from_cache(key: str) -> int:
    """Deletes a key from the Redis cache. Returns number of keys deleted."""
    if not redis_client:
        logger.warning("Redis client not available, cannot delete from cache.")
        return 0
    try:
        # Use byte key directly since client has decode_responses=False
        deleted_count = redis_client.delete(key.encode("utf-8"))
        if deleted_count > 0:
            logger.info(f"Deleted cache key: {key}")
        return deleted_count
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Cache] Redis connection error on delete '{key}': {e}")
    except Exception as e:
        logger.error(f"[Cache] Error deleting key '{key}': {e}", exc_info=True)
    return 0


def generate_cache_key(prefix: str, username: str, identifier: str = None) -> str:
    """Generates a consistent cache key."""
    if identifier:
        import hashlib

        # Ensure identifier is string before encoding
        id_str = str(identifier)
        hash_part = hashlib.md5(id_str.encode("utf-8")).hexdigest()[:16]
        key = f"{prefix}:{username}:{hash_part}"
    else:
        key = f"{prefix}:{username}"
    # Basic cleanup - replace common URL chars if needed, though Redis keys are quite flexible
    # key = key.replace('://', '_').replace('/', '_').replace('?', '_').replace('&', '_').replace('=', '_')
    return key


# --- Simple Binary Caching (Base64 encoded string stored with SETEX) ---


def save_binary_simple(
    cache_key: str, content: bytes, expiry: int = config.PROXY_CACHE_EXPIRY
):
    """Saves binary content to Redis as a Base64 encoded string using SETEX."""
    if not redis_client:
        logger.warning("Redis client not available, cannot save binary cache (simple).")
        return False
    try:
        start_time = time.perf_counter()
        encoded_content_bytes = base64.b64encode(content)  # Get bytes
        # SETEX expects bytes for the value when decode_responses=False
        redis_client.setex(cache_key.encode("utf-8"), expiry, encoded_content_bytes)
        duration = time.perf_counter() - start_time
        logger.info(
            f"Saved binary content (simple Base64 SETEX) for {cache_key} ({len(content)} bytes -> {len(encoded_content_bytes)} encoded bytes) in {duration:.3f}s"
        )
        return True
    except redis.exceptions.TimeoutError:
        logger.error(
            f"Redis TIMEOUT error saving binary cache (simple) for {cache_key}."
        )
    except redis.exceptions.ConnectionError as e:
        logger.error(
            f"Redis connection error saving binary cache (simple) for {cache_key}: {e}"
        )
    except Exception as e:
        logger.error(
            f"Error saving binary cache (simple) for {cache_key}: {e}", exc_info=True
        )
    return False


def get_binary_simple(cache_key: str) -> bytes | None:
    """Gets binary content stored as a Base64 string from Redis."""
    if not redis_client:
        logger.warning("Redis client not available, cannot get binary cache (simple).")
        return None
    try:
        # GET returns bytes since decode_responses=False
        encoded_content_bytes = redis_client.get(cache_key.encode("utf-8"))
        if encoded_content_bytes:
            try:
                decoded_bytes = base64.b64decode(encoded_content_bytes)
                logger.info(
                    f"Retrieved binary content (simple Base64) for {cache_key} ({len(decoded_bytes)} bytes)"
                )
                return decoded_bytes
            except Exception as decode_err:
                logger.error(
                    f"Error decoding base64 cache (simple) for {cache_key}: {decode_err}"
                )
                # Optionally delete corrupted key
                # delete_from_cache(cache_key)
                return None
        return None
    except redis.exceptions.ConnectionError as e:
        logger.error(
            f"Redis connection error reading binary cache (simple) for {cache_key}: {e}"
        )
    except Exception as e:
        logger.error(
            f"Error reading binary cache (simple) for {cache_key}: {e}", exc_info=True
        )
    return None


def get_pickle_cache(key: str):
    """Retrieves and unpickles data from Redis cache."""
    if not redis_client:
        logger.warning("Redis client not available, cannot get from pickle cache.")
        return None
    try:
        cached_bytes = redis_client.get(key.encode("utf-8"))
        if cached_bytes:
            try:
                return pickle.loads(cached_bytes)
            except pickle.UnpicklingError as e:
                logger.error(
                    f"[Cache] Error unpickling data for key '{key}': {e}. Data: {cached_bytes[:100]!r}"
                )
                # Optionally delete corrupted key
                # delete_from_cache(key)
                return None
            except Exception as e_unpickle: # Catch other potential unpickling issues
                logger.error(f"[Cache] General error unpickling key '{key}': {e_unpickle}", exc_info=True)
                return None
        return None
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Cache] Redis connection error on get (pickle) '{key}': {e}")
    except Exception as e:
        logger.error(f"[Cache] Error getting key (pickle) '{key}': {e}", exc_info=True)
    return None
