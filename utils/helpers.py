# utils/helpers.py
import hashlib
import json
import logging
import re
import requests
from urllib.parse import urlparse, urlunparse, unquote
import time  # Added for memory cache
import threading  # Added for memory cache lock

# Import config singleton
from config import config

# Import redis client for cached redis getters
try:
    from .cache import redis_client
except ImportError:
    logging.critical("Could not import redis_client from utils.cache for helpers.")
    redis_client = None


logger = logging.getLogger(__name__)


def calculate_dict_hash(data: dict) -> str:
    """Return an MD5 hash of the given dictionary (used for change detection)."""
    if not isinstance(data, dict):
        logger.warning(
            "calculate_dict_hash called with non-dict, returning empty hash."
        )
        return ""
    try:
        # Use separators=(',', ':') for compact, deterministic JSON encoding
        return hashlib.md5(
            json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash: {e}", exc_info=True)
        return ""  # Return empty string on error


def extract_v_param(text: str) -> str | None:
    """Extract the dynamic 'v' parameter from JavaScript in HTML text."""
    if not isinstance(text, str) or not text:
        logger.warning("extract_v_param received empty or non-string input.")
        return None

    # Refined regex: Allow optional whitespace, ignore case, ensure captured value is reasonable (alphanumeric/hyphen)
    regex = r"sTo\s*\(\s*'([a-zA-Z0-9-]+)'\s*\)"

    # Use re.search with re.IGNORECASE
    match = re.search(regex, text, re.IGNORECASE)

    if match:
        v_param_value = match.group(1)
        logger.info(f"Successfully extracted 'v' parameter: {v_param_value}")
        return v_param_value
    else:
        # Log a snippet if match fails, to help debug
        snippet_start = text.find("sTo(")  # Find occurrence even if regex fails
        if snippet_start != -1:
            snippet = text[
                max(0, snippet_start - 20) : snippet_start + 40
            ]  # Get context around sTo(
            logger.warning(
                f"Could not extract 'v' parameter using regex. Found 'sTo(' but pattern mismatch near: ...{snippet}..."
            )
        else:
            logger.warning(
                "Could not find 'sTo(' call in the provided text to extract 'v' parameter."
            )
        # logger.debug(f"Full text length checked: {len(text)}") # Optional
        return None


def normalize_course_url(course_url: str) -> str:
    """Normalizes the course URL for consistent caching and requests."""
    if not course_url or not isinstance(course_url, str):
        return ""
    try:
        decoded = unquote(course_url).strip().lower()
        parsed = urlparse(decoded)

        if not parsed.scheme:
            parsed = parsed._replace(scheme="https")
        if not parsed.netloc:
            if "cms.guc.edu.eg" in parsed.path:
                domain_start_index = parsed.path.find("cms.guc.edu.eg")
                path_parts = parsed.path[domain_start_index:].split("/")
                new_netloc = path_parts[0]
                new_path = "/" + "/".join(path_parts[1:])
                if new_path == "/":
                    new_path = ""
                parsed = parsed._replace(netloc=new_netloc, path=new_path)
            else:
                logger.warning(
                    f"Could not reliably normalize URL missing domain: {course_url}"
                )
                return course_url

        if "courseviewstn" in parsed.path and not parsed.path.endswith(".aspx"):
            parsed = parsed._replace(path=parsed.path + ".aspx")

        if parsed.path != "/" and parsed.path.endswith("/"):
            parsed = parsed._replace(path=parsed.path[:-1])

        return urlunparse(parsed)
    except Exception as e:
        logger.error(f"Error normalizing URL '{course_url}': {e}", exc_info=True)
        return course_url


def get_country_from_ip(ip_address: str) -> str:
    """Looks up the country for a given IP address using ipapi.co."""
    if not ip_address or ip_address in ("127.0.0.1", "::1", "localhost"):
        logger.debug("Localhost or missing IP; using fallback country 'Localhost'.")
        return "Localhost"
    if (
        not re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip_address)
        and ":" not in ip_address
    ):
        logger.warning(f"Invalid IP format provided: {ip_address}")
        return "Invalid IP Format"

    api_url = f"https://ipapi.co/{ip_address}/json/"
    headers = {"User-Agent": "UnisightBackend/1.0"}

    try:
        response = requests.get(api_url, timeout=5, headers=headers)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            reason = data.get("reason", "Unknown reason")
            logger.warning(f"IP API error for {ip_address}: {reason}")
            if "Rate limit exceeded" in reason:
                return "API Rate Limited"
            return "API Error"

        country = data.get("country_name")
        if not country:
            if data.get("reserved"):
                return "Reserved Range"
            else:
                return "Unknown (API)"
        logger.info(f"Determined country '{country}' for IP {ip_address}")
        return country
    except requests.exceptions.Timeout:
        return "Lookup Timeout"
    except requests.exceptions.HTTPError as e:
        return f"Lookup Failed (HTTP {e.response.status_code})"
    except requests.exceptions.RequestException as e:
        return "Lookup Failed (Network)"
    except json.JSONDecodeError as e:
        return "Lookup Failed (JSON)"
    except Exception as e:
        return "Lookup Failed (Unknown)"


def guess_content_type(filename: str) -> str:
    """Guesses the MIME type based on the file extension."""
    if not filename or not isinstance(filename, str):
        return "application/octet-stream"
    parts = filename.split("?")[0].split(".")
    ext = parts[-1].lower() if len(parts) > 1 else ""
    content_types = {
        "pdf": "application/pdf",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "doc": "application/msword",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "ppt": "application/vnd.ms-powerpoint",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xls": "application/vnd.ms-excel",
        "txt": "text/plain",
        "csv": "text/csv",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "svg": "image/svg+xml",
        "webp": "image/webp",
        "zip": "application/zip",
        "rar": "application/vnd.rar",
        "7z": "application/x-7z-compressed",
        "html": "text/html",
        "htm": "text/html",
        "xml": "application/xml",
        "json": "application/json",
        "js": "application/javascript",
        "css": "text/css",
        "mp4": "video/mp4",
        "mp3": "audio/mpeg",
        "wav": "audio/wav",
    }
    return content_types.get(ext, "application/octet-stream")


# --- In-Memory Cache ---
_memory_cache = {}
_memory_cache_ttl = {}
_memory_cache_lock = threading.Lock()
MEMORY_CACHE_SHORT_TTL = 5  # seconds (e.g., 1 minute for version/announcement)


def get_from_memory_cache(key: str):
    """Gets a value from the simple in-memory cache if not expired."""
    with _memory_cache_lock:
        if key in _memory_cache and time.time() < _memory_cache_ttl.get(key, 0):
            logger.debug(f"Memory cache hit for key '{key}'")
            return _memory_cache[key]
    logger.debug(f"Memory cache miss for key '{key}'")
    return None


def set_in_memory_cache(key: str, value, ttl: int = MEMORY_CACHE_SHORT_TTL):
    """Sets a value in the simple in-memory cache."""
    with _memory_cache_lock:
        _memory_cache[key] = value
        _memory_cache_ttl[key] = time.time() + ttl
        logger.debug(f"Set memory cache for key '{key}' with TTL {ttl}s")


# --- Cached Getters for Version and Dev Announcement ---
_VERSION_CACHE_KEY = "memory:version_number"
_DEV_ANNOUNCE_CACHE_KEY = "memory:dev_announcement"


def get_version_number_cached() -> str:
    """Gets version number, using memory cache first, then Redis."""
    cached_version = get_from_memory_cache(_VERSION_CACHE_KEY)
    if cached_version:
        return cached_version

    logger.debug("Fetching version number from Redis (memory cache miss).")
    version_number = "1.0"  # Default
    try:
        if redis_client:  # Use the client imported earlier
            # Assuming redis_client uses decode_responses=False, need to decode bytes
            version_raw_bytes = redis_client.get("VERSION_NUMBER".encode("utf-8"))
            if version_raw_bytes:
                version_number = version_raw_bytes.decode("utf-8")
                set_in_memory_cache(_VERSION_CACHE_KEY, version_number)
            else:
                logger.warning("VERSION_NUMBER not set in Redis.")
                set_in_memory_cache(_VERSION_CACHE_KEY, version_number)  # Cache default
        else:
            logger.warning("Redis client unavailable for version check.")
            version_number = "Redis Unavailable"
    except Exception as e:
        logger.error(f"Error getting VERSION_NUMBER from Redis: {e}", exc_info=True)
        version_number = "Error Fetching"
    return version_number


def get_dev_announcement_cached() -> dict:
    """Gets dev announcement, using memory cache first, then Redis."""
    cached_announce = get_from_memory_cache(_DEV_ANNOUNCE_CACHE_KEY)
    if cached_announce:
        return cached_announce

    logger.debug("Fetching dev announcement from Redis (memory cache miss).")
    announcement = config.DEFAULT_DEV_ANNOUNCEMENT  # Start with default
    try:
        if redis_client:
            # Use bytes key, decode response bytes
            announce_key_bytes = config.REDIS_DEV_ANNOUNCEMENT_KEY.encode("utf-8")
            announcement_bytes = redis_client.get(announce_key_bytes)
            if announcement_bytes:
                try:
                    announcement = json.loads(announcement_bytes.decode("utf-8"))
                    logger.info("Successfully loaded dev announcement from Redis.")
                except (json.JSONDecodeError, UnicodeDecodeError) as json_err:
                    logger.error(
                        f"Error parsing announcement JSON from Redis: {json_err}. Using default."
                    )
                    # Optionally try to fix it in Redis
                    # set_dev_announcement(config.DEFAULT_DEV_ANNOUNCEMENT) # Needs import/relocation
            else:
                logger.info(
                    f"No announcement in Redis key '{config.REDIS_DEV_ANNOUNCEMENT_KEY}'. Using default."
                )
                # Optionally store default in Redis now
                # set_dev_announcement(config.DEFAULT_DEV_ANNOUNCEMENT) # Needs import/relocation

            set_in_memory_cache(_DEV_ANNOUNCE_CACHE_KEY, announcement)  # Cache result
        else:
            logger.warning("Redis client unavailable for dev announcement check.")

    except Exception as e:
        logger.error(f"Error getting dev announcement from Redis: {e}", exc_info=True)

    return announcement


# This function might need to be defined here if api/guc cannot be imported easily
# Or move it entirely here from api/guc.py
def set_dev_announcement(announcement: dict) -> bool:
    """Stores the developer announcement in Redis."""
    if not redis_client:
        logger.error("Redis client unavailable for setting dev announcement.")
        return False
    try:
        value_json = json.dumps(announcement, ensure_ascii=False)
        value_bytes = value_json.encode("utf-8")
        key_bytes = config.REDIS_DEV_ANNOUNCEMENT_KEY.encode("utf-8")
        redis_client.set(
            key_bytes, value_bytes
        )  # Use SET, no expiry needed here usually
        logger.info(
            f"Stored dev announcement to Redis key '{config.REDIS_DEV_ANNOUNCEMENT_KEY}'."
        )
        return True
    except Exception as e:
        logger.error(f"Error setting dev announcement: {e}", exc_info=True)
        return False
