# utils/cache.py
import redis
import orjson # Use orjson for faster JSON operations
import logging
import base64  # For simple binary caching
import time
import pickle
import hashlib # Added for hashing

from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

redis_client = None
try:
    # Initialize Redis client - decode_responses=False for binary/pickle/base64
    redis_client = redis.from_url(
        config.REDIS_URL, decode_responses=False, socket_timeout=10, health_check_interval=30
    )
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
        cached_bytes = redis_client.get(key.encode("utf-8"))
        if cached_bytes:
            try:
                return orjson.loads(cached_bytes)
            except orjson.JSONDecodeError as e:
                logger.error(
                    f"[Cache] Error decoding JSON for key '{key}': {e}. Data: {cached_bytes[:100]!r}"
                )
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
        # Use orjson.dumps with OPT_SORT_KEYS for consistency if value is a dict/contains dicts
        value_bytes = orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
        redis_client.setex(key.encode("utf-8"), timeout, value_bytes)
        logger.debug(f"Set cache for key '{key}' (direct) with timeout {timeout}s")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Cache] Redis connection error on set '{key}' (direct): {e}")
    except TypeError as e:
        logger.error(
            f"[Cache] Failed to serialize value to JSON for key '{key}' (direct): {e}",
            exc_info=True,
        )
    except Exception as e:
        logger.error(f"[Cache] Error setting key '{key}' (direct): {e}", exc_info=True)
    return False


def delete_from_cache(key: str) -> int:
    """Deletes a key from the Redis cache. Returns number of keys deleted."""
    if not redis_client:
        logger.warning("Redis client not available, cannot delete from cache.")
        return 0
    try:
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
    """Generates a consistent cache key. Used by refresh_cache for key_base."""
    if identifier:
        id_str = str(identifier)
        # Using hashlib directly here as it's for key name generation, not data hashing
        import hashlib as local_hash_for_key_gen
        hash_part = local_hash_for_key_gen.md5(id_str.encode("utf-8")).hexdigest()[:16]
        key = f"{prefix}:{username}:{hash_part}"
    else:
        key = f"{prefix}:{username}"
    return key


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
                return None
            except Exception as e_unpickle:
                logger.error(f"[Cache] General error unpickling key '{key}': {e_unpickle}", exc_info=True)
                return None
        return None
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Cache] Redis connection error on get (pickle) '{key}': {e}")
    except Exception as e:
        logger.error(f"[Cache] Error getting key (pickle) '{key}': {e}", exc_info=True)
    return None


def set_pickle_cache(key: str, value, timeout: int = config.CACHE_DEFAULT_TIMEOUT):
    """Pickles the value and stores it in Redis cache with expiry."""
    if not redis_client:
        logger.warning("Redis client not available, cannot set pickle cache.")
        return False
    try:
        pickled_value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        redis_client.setex(key.encode("utf-8"), timeout, pickled_value)
        logger.info(f"Set PICKLE cache for key {key} with expiry {timeout} seconds")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Pickle Cache: Redis connection error on set '{key}': {e}")
    except (pickle.PicklingError, TypeError) as e:
        logger.error(
            f"Pickle Cache: Error pickling value for key '{key}': {e}",
            exc_info=True,
        )
    except Exception as e:
        logger.error(f"Pickle Cache: Error setting key {key}: {e}", exc_info=True)
    return False


def save_binary_simple(
    cache_key: str, content: bytes, expiry: int = config.PROXY_CACHE_EXPIRY
):
    if not redis_client: return False
    try:
        encoded_content_bytes = base64.b64encode(content)
        redis_client.setex(cache_key.encode("utf-8"), expiry, encoded_content_bytes)
        return True
    except Exception as e:
        logger.error(f"Error saving binary cache (simple) for {cache_key}: {e}", exc_info=True)
    return False

def get_binary_simple(cache_key: str) -> bytes | None:
    if not redis_client: return None
    try:
        encoded_content_bytes = redis_client.get(cache_key.encode("utf-8"))
        if encoded_content_bytes:
            return base64.b64decode(encoded_content_bytes)
        return None
    except Exception as e:
        logger.error(f"Error reading binary cache (simple) for {cache_key}: {e}", exc_info=True)
    return None


# --- NEW HASH-BASED CACHING FUNCTIONS ---

def _get_canonical_bytes_for_hashing(data_obj, is_pickle: bool):
    """
    Converts data to canonical bytes for consistent hashing.
    For JSON, uses orjson. For Pickle, uses pickle.dumps.
    """
    if is_pickle:
        return pickle.dumps(data_obj, protocol=pickle.HIGHEST_PROTOCOL)
    else: # JSON
        return orjson.dumps(data_obj, option=orjson.OPT_SORT_KEYS)

def _generate_data_hash(data_bytes: bytes) -> str:
    """Generates a SHA256 hash string from bytes."""
    return hashlib.sha256(data_bytes).hexdigest()

def get_data_and_stored_hash(key_base: str, is_pickle: bool):
    """
    Retrieves data object and its stored hash from Redis.
    '<key_base>:data' stores the actual data (JSON string bytes or pickled bytes).
    '<key_base>:hash' stores the SHA256 hash string of the data (as bytes).
    Returns (data_object, stored_hash_string) or (None, None) or (None, stored_hash_string)
    """
    if not redis_client:
        logger.warning("Redis client not available for get_data_and_stored_hash.")
        return None, None

    data_key_bytes = f"{key_base}:data".encode("utf-8")
    hash_key_bytes = f"{key_base}:hash".encode("utf-8")

    try:
        cached_data_bytes = redis_client.get(data_key_bytes)
        stored_hash_bytes = redis_client.get(hash_key_bytes)

        data_object = None
        if cached_data_bytes:
            try:
                if is_pickle:
                    data_object = pickle.loads(cached_data_bytes)
                else: # JSON
                    data_object = orjson.loads(cached_data_bytes)
            except (orjson.JSONDecodeError, pickle.UnpicklingError, TypeError) as e:
                logger.error(f"Error deserializing data for key_base '{key_base}': {e}. Data bytes: {cached_data_bytes[:100]!r}")
                return None, (stored_hash_bytes.decode("utf-8") if stored_hash_bytes else None)

        stored_hash_string = stored_hash_bytes.decode("utf-8") if stored_hash_bytes else None
        return data_object, stored_hash_string

    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error on get_data_and_stored_hash for '{key_base}': {e}")
    except Exception as e:
        logger.error(f"Error in get_data_and_stored_hash for '{key_base}': {e}", exc_info=True)
    return None, None


def set_data_and_hash_pipelined(pipe, key_base: str, data_obj, new_hash_string: str, canonical_data_bytes: bytes, timeout: int, is_pickle: bool):
    """
    Adds commands to set data and its hash to a Redis pipeline.
    `canonical_data_bytes` are the bytes generated by _get_canonical_bytes_for_hashing.
    `new_hash_string` is the hash generated from `canonical_data_bytes`.
    """
    if not redis_client:
        logger.error("Redis client not available for pipelined set.")
        return

    data_key_bytes = f"{key_base}:data".encode("utf-8")
    hash_key_bytes = f"{key_base}:hash".encode("utf-8")

    try:
        pipe.setex(data_key_bytes, timeout, canonical_data_bytes)
        pipe.setex(hash_key_bytes, timeout, new_hash_string.encode("utf-8"))
        logger.debug(f"Pipelined SETEX for {key_base}:data and {key_base}:hash with timeout {timeout}s")
    except Exception as e:
        logger.error(f"Error adding set_data_and_hash to pipeline for '{key_base}': {e}", exc_info=True)
        raise


def expire_keys_pipelined(pipe, key_base: str, timeout: int):
    """Adds commands to EXPIRE data and hash keys to a Redis pipeline."""
    if not redis_client:
        logger.error("Redis client not available for pipelined expire.")
        return

    data_key_bytes = f"{key_base}:data".encode("utf-8")
    hash_key_bytes = f"{key_base}:hash".encode("utf-8")
    try:
        pipe.expire(data_key_bytes, timeout)
        pipe.expire(hash_key_bytes, timeout)
        logger.debug(f"Pipelined EXPIRE for {key_base}:data and {key_base}:hash with timeout {timeout}s")
    except Exception as e:
        logger.error(f"Error adding expire_keys to pipeline for '{key_base}': {e}", exc_info=True)
        raise