# api/cms.py
import logging
import json
from flask import Blueprint, request, jsonify, g
import concurrent.futures  # To run scraping tasks
import hashlib  # Added for hash generation
import time # Added for timing logs

from config import config
from scraping.core import create_session, make_request
from utils.auth import (
    AuthError, 
    get_password_for_readonly_session
)
from utils.cache import (
    get_from_cache, 
    set_in_cache, 
    generate_cache_key, 
    get_pickle_cache,
    set_pickle_cache
)
from utils.helpers import normalize_course_url, get_from_memory_cache, set_in_memory_cache # Import in-memory cache functions

# Import specific scraping functions needed
from scraping.cms import (
    scrape_cms_courses,
    scrape_course_content,
    scrape_course_announcements,
)
from scraping.guc_data import (
    parse_notifications,
)  # Assuming this parses the CMS Home notifications correctly
from utils.mock_data import cmsdata_mockData, mock_content_map

logger = logging.getLogger(__name__)
cms_bp = Blueprint("cms_bp", __name__)

# --- Constants ---
CMS_COURSES_CACHE_PREFIX = "cms_courses"
CMS_COURSE_DATA_CACHE_PREFIX = "cms_content"
CMS_NOTIFICATIONS_CACHE_PREFIX = "cms_notifications"

# Define a short TTL for in-memory cache for hot CMS content data
CMS_CONTENT_MEMORY_CACHE_TTL = 30 # seconds


def _is_cms_content_substantial(content_data: list) -> bool:
    """
    Evaluates if CMS content data is substantial (has real content weeks with materials).

    Args:
        content_data: List containing announcements, mock weeks, and actual content weeks

    Returns:
        bool: True if content has substantial material beyond just announcements/mock data
    """
    if not content_data or not isinstance(content_data, list):
        return False

    # Count actual content weeks (not announcements or mock weeks)
    actual_content_weeks = 0
    total_materials = 0

    for item in content_data:
        if isinstance(item, dict):
            # Skip announcements
            if "course_announcement" in item:
                continue

            # Check if this is a week with actual content
            # The actual CMS data structure uses "week_name" and "contents" keys
            if "week_name" in item and "contents" in item:
                week_contents = item.get("contents", [])
                if isinstance(week_contents, list) and len(week_contents) > 0:
                    # Check if it's not just a mock week
                    week_name = item.get("week_name", "").lower()
                    if "mock" not in week_name and "placeholder" not in week_name:
                        actual_content_weeks += 1
                        total_materials += len(week_contents)

    # Consider content substantial if it has at least 1 real week with materials
    # or at least 3 total materials across weeks
    return actual_content_weeks >= 1 or total_materials >= 3


# --- Helper to get combined course data (content + announcement) ---
def get_combined_course_data(
    username: str, password: str, course_url: str, force_refresh: bool = False
) -> dict | None:
    """
    Orchestrates fetching both content and announcements for a single course.
    Handles caching internally using the 'cms_content' prefix.
    Returns the final combined list structure expected by the frontend/refresh script.

    Args:
        username (str): The username.
        password (str): The password.
        course_url (str): The normalized course URL.
        force_refresh (bool): If True, bypasses the cache.
    """
    normalized_url = normalize_course_url(
        course_url
    )  # Already normalized before calling
    if not normalized_url:
        logger.error(f"Invalid course URL for combined data: {course_url}")
        return {"error": "Invalid course URL provided."}

    # Generate cache key consistent with refresh_cache.py for cms_content
    # Key format: "cms_content:{hash_of_username_and_normalized_url}"
    # key_string_for_hash = f"{username}:{normalized_url}" # Original, now global
    # hash_value = hashlib.md5(key_string_for_hash.encode("utf-8")).hexdigest()
    # cache_key = f"{CMS_COURSE_DATA_CACHE_PREFIX}:{hash_value}"

    # NEW global cache key generation (matching refresh_cache.py's modified generate_cms_content_cache_key logic):
    if not normalized_url:
        # This case should ideally be caught before, but as a safeguard:
        logger.error(f"Cannot generate cache key from invalid normalized_url: {course_url}")
        return {"error": "Invalid course URL for cache key generation."}
    
    global_hash_value = hashlib.md5(normalized_url.encode("utf-8")).hexdigest()
    cache_key = f"{CMS_COURSE_DATA_CACHE_PREFIX}:{global_hash_value}"
    logger.debug(f"Using global cache key for {normalized_url}: {cache_key}")

    # --- Cache Check (In-Memory first, then Redis) ---
    if not force_refresh:
        # 1. Check in-memory cache
        in_memory_cache_check_start_time = time.perf_counter()
        cached_data = get_from_memory_cache(cache_key)
        in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
        logger.info(f"TIMING: In-memory Cache check for CMS content took {in_memory_cache_check_duration:.2f} ms")

        if cached_data:
            if isinstance(cached_data, list) and len(cached_data) > 0:
                logger.info(
                    f"Serving combined CMS data from IN-MEMORY cache for {username} - {normalized_url}"
                )
                return cached_data
            else:
                logger.warning(
                    f"Invalid combined CMS data format found in IN-MEMORY cache for {cache_key}. Fetching fresh."
                )

        # 2. If not in-memory, check Redis cache
        redis_cache_check_start_time = time.perf_counter()
        cached_data = get_pickle_cache(cache_key)
        redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
        logger.info(f"TIMING: Redis Cache check for CMS content took {redis_cache_check_duration:.2f} ms")

        if cached_data:
            # Basic validation of cached structure
            if isinstance(cached_data, list) and len(cached_data) > 0:
                logger.info(
                    f"Serving combined CMS data from REDIS cache for {username} - {normalized_url}"
                )
                # Set in in-memory cache for future rapid access
                set_in_memory_cache(cache_key, cached_data, ttl=CMS_CONTENT_MEMORY_CACHE_TTL)
                logger.info(f"Set CMS content in IN-MEMORY cache for {username}")
                return cached_data
            else:
                logger.warning(
                    f"Invalid combined CMS data format found in REDIS cache for {cache_key}. Fetching fresh."
                )
    else:
        logger.info(
            f"Force refresh requested for combined CMS data for {username} - {normalized_url}. Bypassing all caches."
        )

    # 3. Cache Miss or Forced Refresh -> Fetch Fresh Data Concurrently
    log_prefix = "Cache miss (both in-memory and Redis)" if not force_refresh else "Forced refresh"
    logger.info(
        f"{log_prefix} for combined CMS data. Fetching fresh for {username} - {normalized_url}"
    )
    scrape_call_start_time = time.perf_counter()

    content_list = None
    announcement_result = (
        None  # Expected: {'announcements_html': '...'} or {'error': '...'} or None
    )
    fetch_success = False

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=2, thread_name_prefix="CourseDataFetch"
    ) as executor:
        content_future = executor.submit(
            scrape_course_content, username, password, normalized_url
        )
        announcement_future = executor.submit(
            scrape_course_announcements, username, password, normalized_url
        )
        try:
            content_list = content_future.result()  # Returns list or None
            if content_list is not None:
                fetch_success = True
        except Exception as e:
            logger.error(f"Course content future error: {e}")
        try:
            announcement_result = announcement_future.result()  # Returns dict or None
            if announcement_result is not None:
                fetch_success = True
        except Exception as e:
            logger.error(f"Course announcement future error: {e}")

    scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
    logger.info(f"TIMING: CMS content scrape took {scrape_call_duration:.2f} ms")

    if not fetch_success:
        logger.error(
            f"Both content and announcement fetch failed for course: {normalized_url}"
        )
        return {"error": "Failed to fetch data for the specified course."}

    # 4. Assemble final list for caching and returning

    combined_data_for_cache = []
    course_announcement_dict_to_add = None

    if announcement_result and isinstance(announcement_result, dict):
        html_content = announcement_result.get("announcements_html")
        if html_content:
            # Check if the content is just an empty div with whitespace
            if html_content.strip() == '<div id="ContentPlaceHolderright_ContentPlaceHoldercontent_desc" style="overflow-x:auto;" class="p-xl-2">\n\n\n                                     </div>':
                course_announcement_dict_to_add = {"course_announcement": ""}
            else:
                course_announcement_dict_to_add = {"course_announcement": html_content}
            combined_data_for_cache.append(course_announcement_dict_to_add)
        else:
            # If no announcements or empty content, add empty string
            course_announcement_dict_to_add = {"course_announcement": ""}
            combined_data_for_cache.append(course_announcement_dict_to_add)
    elif announcement_result and isinstance(announcement_result, dict) and "error" in announcement_result:
        logger.warning(
            f"Announcement scraping failed for {normalized_url}: {announcement_result['error']}"
        )
        # Add empty string for announcements on error
        course_announcement_dict_to_add = {"course_announcement": ""}
        combined_data_for_cache.append(course_announcement_dict_to_add)

    if content_list is not None and isinstance(content_list, list):
        combined_data_for_cache.extend(content_list)
    elif content_list is not None:
        logger.warning(
            f"scrape_course_content returned unexpected type: {type(content_list)}"
        )

    # 5. Cache the result with fallback logic
    new_data_is_substantial = _is_cms_content_substantial(combined_data_for_cache)

    if course_announcement_dict_to_add or (
        content_list is not None and isinstance(content_list, list)
    ):
        # If new data is not substantial, check if we should preserve existing cache
        if not new_data_is_substantial:
            logger.warning(
                f"Newly fetched CMS content for {normalized_url} is not substantial (empty/minimal content)"
            )

            # Try to get existing cached data from Redis
            existing_cached_data = get_pickle_cache(cache_key)
            existing_data_is_substantial = _is_cms_content_substantial(existing_cached_data)

            if existing_cached_data and existing_data_is_substantial:
                logger.info(
                    f"Preserving existing substantial CMS content cache (REDIS) for {normalized_url} "
                    f"instead of overwriting with insufficient new data (Key: {cache_key})"
                )
                # Extend the timeout of existing cache to keep it fresh in Redis
                if set_pickle_cache(cache_key, existing_cached_data, timeout=config.CACHE_LONG_CMS_CONTENT_TIMEOUT):
                    logger.info(f"Successfully preserved and refreshed existing CMS content cache (REDIS) for {cache_key}")
                    # Also set in in-memory cache if preserving
                    set_in_memory_cache(cache_key, existing_cached_data, ttl=CMS_CONTENT_MEMORY_CACHE_TTL)
                    logger.info(f"Set preserved CMS content in IN-MEMORY cache for {cache_key}")
                    return existing_cached_data
                else:
                    logger.error(f"Failed to refresh existing CMS content cache (REDIS) timeout for {cache_key}")
                    # Fall through to cache new data anyway
            else:
                logger.info(f"No existing substantial cache (REDIS) found for {cache_key}, will cache new data even if minimal")

        # Cache the new data in Redis (either it's substantial, or no good existing cache exists)
        set_pickle_cache(
            cache_key, combined_data_for_cache, timeout=config.CACHE_LONG_CMS_CONTENT_TIMEOUT
        )
        if new_data_is_substantial:
            logger.info(f"Cached fresh substantial combined CMS data (pickled) in REDIS for {cache_key}")
        else:
            logger.info(f"Cached new minimal combined CMS data (pickled) in REDIS for {cache_key}")

        # Also cache the new data in in-memory
        set_in_memory_cache(cache_key, combined_data_for_cache, ttl=CMS_CONTENT_MEMORY_CACHE_TTL)
        logger.info(f"Cached fresh combined CMS data in IN-MEMORY for {cache_key}")

    return combined_data_for_cache


@cms_bp.route("/cms_data", methods=["GET"])
def api_cms_courses():
    """
    Endpoint to fetch the user's CMS courses.
    Uses cache first, then scrapes if needed.
    Requires query params: username, password.
    """
    req_start_time = time.perf_counter() # Overall request start

    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for CMS Courses API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "CMS Courses API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock cms_courses data for user {username}")
        g.log_outcome = "mock_data_served"
        return jsonify(cmsdata_mockData["courses"]), 200

    try:
        password_to_use = get_password_for_readonly_session(username, password)

        # --- Cache Check (In-Memory first, then Redis) ---
        cache_key = generate_cache_key(CMS_COURSES_CACHE_PREFIX, username)
        if not force_refresh:
            # 1. Check in-memory cache
            in_memory_cache_check_start_time = time.perf_counter()
            cached_data = get_from_memory_cache(cache_key)
            in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
            logger.info(f"TIMING: In-memory Cache check for CMS courses took {in_memory_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty list [] from cache
                logger.info(f"Serving CMS courses from IN-MEMORY cache for {username}")
                g.log_outcome = "memory_cache_hit"
                return jsonify(cached_data), 200

            # 2. If not in-memory, check Redis cache
            redis_cache_check_start_time = time.perf_counter()
            cached_data = get_from_cache(cache_key)
            redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
            logger.info(f"TIMING: Redis Cache check for CMS courses took {redis_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty list [] from cache
                logger.info(f"Serving CMS courses from REDIS cache for {username}")
                g.log_outcome = "redis_cache_hit"
                # Set in in-memory cache for future rapid access
                set_in_memory_cache(cache_key, cached_data, ttl=CMS_CONTENT_MEMORY_CACHE_TTL)
                logger.info(f"Set CMS courses in IN-MEMORY cache for {username}")
                return jsonify(cached_data), 200

        # --- Cache Miss -> Scrape ---
        logger.info(f"Cache miss or forced refresh for CMS courses (both in-memory and Redis). Scraping for {username}")
        g.log_outcome = "scrape_attempt"
        scrape_call_start_time = time.perf_counter()

        courses = scrape_cms_courses(username, password_to_use)
        scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(f"TIMING: CMS courses scrape took {scrape_call_duration:.2f} ms")

        if courses is None:
            g.log_outcome = "scrape_error"
            g.log_error_message = "CMS courses scraper returned None"
            logger.error(f"CMS courses scraping returned None for {username}.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to fetch CMS courses due to a server error",
                    }
                ),
                500,
            )
        elif isinstance(courses, dict) and "error" in courses:
            error_msg = courses["error"]
            logger.error(
                f"CMS courses scraping returned specific error for {username}: {error_msg}"
            )
            g.log_error_message = error_msg
            if "Authentication failed" in error_msg:
                g.log_outcome = "scrape_auth_error"
                status_code = 401
            else:
                g.log_outcome = "scrape_returned_error"
                status_code = 502
            return jsonify({"status": "error", "message": error_msg}), status_code
        else:
            # --- Success ---
            g.log_outcome = "scrape_success"
            logger.info(f"Successfully scraped CMS courses for {username}")

            # Cache the successful result in Redis
            set_in_cache(cache_key, courses, timeout=config.CACHE_LONG_TIMEOUT)
            logger.info(f"Cached fresh CMS courses in REDIS for {username}")

            # Cache the successful result in in-memory
            set_in_memory_cache(cache_key, courses, ttl=CMS_CONTENT_MEMORY_CACHE_TTL) # Using CMS_CONTENT_MEMORY_CACHE_TTL as a generic default for CMS related items
            logger.info(f"Cached fresh CMS courses in IN-MEMORY for {username}")

            return jsonify(courses), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during CMS courses request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/cms_data for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )


@cms_bp.route("/cms_content", methods=["GET"])
def api_cms_content():
    """
    Endpoint to fetch detailed CMS content for a specific course.
    Requires query params: username, password, course_url.
    """
    req_start_time = time.perf_counter() # Overall request start

    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for CMS Content API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify({"status": "Success", "message": "CMS Content API route is up!"}),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    course_url = request.args.get("course_url")
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info("Serving mock cms_content data for google.user")
        g.log_outcome = "mock_data_served"
        # Mock content map key needs to be normalized just like real URLs
        mock_course_key = normalize_course_url(course_url)
        if mock_course_key and mock_course_key in mock_content_map:
            return jsonify(mock_content_map[mock_course_key]), 200
        else:
            logger.warning(f"Mock content not found for course_url: {course_url}")
            return jsonify({"status": "error", "message": "Mock content not found"}), 404

    try:
        if not username or not password or not course_url:
            g.log_outcome = "validation_error"
            g.log_error_message = (
                "Missing required parameters (username, password, course_url)"
            )
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Missing required parameters: username, password, course_url",
                    }
                ),
                400,
            )

        password_to_use = get_password_for_readonly_session(username, password)

        # Use the helper function that now includes 2-tier caching logic
        content_data = get_combined_course_data(
            username, password_to_use, course_url, force_refresh
        )

        if isinstance(content_data, dict) and "error" in content_data:
            error_message = content_data["error"]
            g.log_outcome = "scrape_error"
            g.log_error_message = error_message
            logger.error(f"Error fetching combined CMS content: {error_message}")
            return jsonify({"status": "error", "message": error_message}), 500
        elif content_data is None:
            g.log_outcome = "scrape_error"
            g.log_error_message = "Combined CMS content function returned None"
            logger.error(f"get_combined_course_data returned None for {username} - {course_url}.")
            return (
                jsonify({"status": "error", "message": "Failed to fetch CMS content"}),
                500,
            )
        else:
            g.log_outcome = "success"
            logger.info(f"Successfully retrieved combined CMS content for {username} - {course_url}")
            return jsonify(content_data), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during CMS content request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/cms_content for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )


@cms_bp.route("/cms_notifications", methods=["GET"])
def api_cms_notifications():
    """
    Endpoint to fetch CMS home page notifications.
    Uses cache first, then scrapes if needed.
    Requires query params: username, password
    """
    req_start_time = time.perf_counter() # Overall request start

    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for CMS Notifications API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify({"status": "Success", "message": "CMS Notifications API route is up!"}),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock cms_notifications data for user {username}")
        g.log_outcome = "mock_data_served"
        return jsonify(cmsdata_mockData["notifications"]), 200

    try:
        password_to_use = get_password_for_readonly_session(username, password)

        # --- Cache Check (In-Memory first, then Redis) ---
        cache_key = generate_cache_key(CMS_NOTIFICATIONS_CACHE_PREFIX, username)
        if not force_refresh:
            # 1. Check in-memory cache
            in_memory_cache_check_start_time = time.perf_counter()
            cached_data = get_from_memory_cache(cache_key)
            in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
            logger.info(f"TIMING: In-memory Cache check for CMS notifications took {in_memory_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty list [] from cache
                logger.info(f"Serving CMS notifications from IN-MEMORY cache for {username}")
                g.log_outcome = "memory_cache_hit"
                return jsonify(cached_data), 200

            # 2. If not in-memory, check Redis cache
            redis_cache_check_start_time = time.perf_counter()
            cached_data = get_from_cache(cache_key)
            redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
            logger.info(f"TIMING: Redis Cache check for CMS notifications took {redis_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty list [] from cache
                logger.info(f"Serving CMS notifications from REDIS cache for {username}")
                g.log_outcome = "redis_cache_hit"
                # Set in in-memory cache for future rapid access
                set_in_memory_cache(cache_key, cached_data, ttl=CMS_CONTENT_MEMORY_CACHE_TTL) # Using CMS_CONTENT_MEMORY_CACHE_TTL as a generic default for CMS related items
                logger.info(f"Set CMS notifications in IN-MEMORY cache for {username}")
                return jsonify(cached_data), 200

        # --- Cache Miss -> Scrape ---
        logger.info(f"Cache miss or forced refresh for CMS notifications (both in-memory and Redis). Scraping for {username}")
        g.log_outcome = "scrape_attempt"
        scrape_call_start_time = time.perf_counter()

        # Scrape notifications from the CMS home page
        # The scraper returns a list of notification dicts or an error dict
        notifications_raw = parse_notifications(username, password_to_use)
        scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(f"TIMING: CMS notifications scrape took {scrape_call_duration:.2f} ms")

        if notifications_raw is None:
            g.log_outcome = "scrape_error"
            g.log_error_message = "CMS notifications scraper returned None"
            logger.error(f"CMS notifications scraping returned None for {username}.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to fetch CMS notifications due to a server error",
                    }
                ),
                500,
            )
        elif isinstance(notifications_raw, dict) and "error" in notifications_raw:
            error_msg = notifications_raw["error"]
            g.log_outcome = "scrape_error"
            g.log_error_message = error_msg
            logger.error(f"CMS notifications scraping failed for {username}: {error_msg}")
            return jsonify({"status": "error", "message": error_msg}), 500
        else:
            # --- Success ---
            g.log_outcome = "scrape_success"
            logger.info(f"Successfully scraped CMS notifications for {username}")

            # Cache the successful result in Redis
            set_in_cache(cache_key, notifications_raw, timeout=config.CACHE_DEFAULT_TIMEOUT)
            logger.info(f"Cached fresh CMS notifications in REDIS for {username}")

            # Cache the successful result in in-memory
            set_in_memory_cache(cache_key, notifications_raw, ttl=CMS_CONTENT_MEMORY_CACHE_TTL) # Using CMS_CONTENT_MEMORY_CACHE_TTL as a generic default for CMS related items
            logger.info(f"Cached fresh CMS notifications in IN-MEMORY for {username}")

            return jsonify(notifications_raw), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during CMS notifications request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/cms_notifications for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )


@cms_bp.route("/announcements", methods=["GET"])
def api_announcements():
    """
    Endpoint to retrieve the current API version number and developer announcement.
    This uses memory-cached getters from utils.helpers.
    """
    req_start_time = time.perf_counter() # Overall request start

    if request.args.get("bot", "").lower() == "true":  # Bot check
        logger.info("Received bot health check request for Announcements API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify({"status": "Success", "message": "Announcements API route is up!"}),
            200,
        )

    # These functions already use in-memory caching internally
    version_number = get_version_number_cached()
    dev_announcement = get_dev_announcement_cached()

    if version_number in ["Error Fetching", "Redis Unavailable"]:
        g.log_outcome = "internal_error_version"
        g.log_error_message = f"Failed to retrieve API version: {version_number}"
        return (
            jsonify({"status": "error", "message": "Could not retrieve API version."}),
            503,
        )

    response_data = {
        "version_number": version_number,
        "dev_announcement": dev_announcement,
    }

    g.log_outcome = "success"
    logger.info("Served announcements and version number.")

    return jsonify(response_data), 200
