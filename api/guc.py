import logging
import orjson  # Changed from json to orjson for consistency
import re  # Added for parsing notification strings
from datetime import datetime  # Added for timestamp parsing
from collections import defaultdict  # Added for grouping grades

# No asyncio needed here anymore
from flask import Blueprint, request, jsonify, g
import time  # For perf_counter timing logs

from config import config
from utils.auth import validate_credentials_flow, AuthError, user_has_stored_credentials, delete_user_credentials
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from utils.helpers import get_from_memory_cache, set_in_memory_cache  # Import in-memory cache functions

# Import the *synchronous* scraper function (or its alias)
from scraping.guc_data import scrape_guc_data_fast, scrape_guc_data

# Import cached getters from helpers
from utils.helpers import (
    get_version_number_cached,
    get_dev_announcement_cached,
    get_dev_announcement_enabled_cached,
)
from utils.mock_data import guc_mockData

logger = logging.getLogger(__name__)
guc_bp = Blueprint("guc_bp", __name__)

# Define a short TTL for in-memory cache for hot data
GUC_DATA_MEMORY_CACHE_TTL = 1800  # 30 Minutes

# We might need set_dev_announcement if the cached getter should store the default
try:
    from utils.helpers import (
        set_dev_announcement,
    )
except ImportError:
    try:
        from .guc import (
            set_dev_announcement,
        )
    except ImportError:

        def set_dev_announcement(a):
            logger.error("set_dev_announcement function not found!")


CACHE_PREFIX = "guc_data"  # Use consistent prefix
TARGET_NOTIFICATION_USERS = ["mohamed.elsaadi", "seif.elkady"]  # For user-specific notifications


def _beautify_grade_updates_body(messages_list: list[str]) -> str:
    if not messages_list:
        return "No specific updates available."

    courses = defaultdict(list)
    for message in messages_list:
        parts = message.split(" - ", 2)
        if len(parts) == 3:
            course_identifier = parts[1].strip()
            grade_detail = parts[2].strip()
            cleaned_grade_detail = grade_detail.split("(was")[0].strip()
            courses[course_identifier].append(cleaned_grade_detail)
        else:
            courses["Miscellaneous Updates"].append(message)

    output_lines = []
    for course_identifier, items in courses.items():
        output_lines.append(f"{course_identifier}:")
        for item in items:
            output_lines.append(f"  - {item}")
        output_lines.append("")

    return "\n".join(output_lines).strip()


@guc_bp.route("/guc_data", methods=["GET"])
def api_guc_data():
    """
    Endpoint to fetch GUC student info and notifications. Sync version using PycURL.
    Uses cache first (in-memory -> redis) then scrapes. Adds dev announcement to response only.
    """
    req_start_time = time.perf_counter()
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for GUC Data API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "GUC Data API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    req_version = request.args.get("version_number")
    first_time = request.args.get("first_time", "false").lower() == "true"
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock guc_data data for user {username}")
        g.log_outcome = "mock_data_served"
        return jsonify(guc_mockData), 200

    if not username or not password or not req_version:
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing required parameters (username, password, version_number)"
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Missing required parameters: username, password, version_number",
                }
            ),
            400,
        )

    try:
        # Version check (cached getter)
        version_check_start = time.perf_counter()
        current_version = get_version_number_cached()
        version_check_duration = (time.perf_counter() - version_check_start) * 1000
        logger.info(f"TIMING: Version check took {version_check_duration:.2f} ms")

        if current_version in ["Error Fetching", "Redis Unavailable"]:
            g.log_outcome = "internal_error_version"
            g.log_error_message = f"Failed to retrieve current API version ({current_version})"
            return (
                jsonify({"status": "error", "message": "Could not verify API version. Please try again later."}),
                503,
            )

        if req_version != current_version:
            logger.warning(f"Incorrect version for {username}. Required: {current_version}, Got: {req_version}")
            g.log_outcome = "version_error"
            g.log_error_message = f"Incorrect version. Required: {current_version}, Got: {req_version}"

            if first_time:
                logger.warning(f"First-time login with incorrect version for {username}. Credentials will not be saved.")
                if user_has_stored_credentials(username):
                    logger.warning(f"Found existing credentials for first-time user {username} with incorrect version. Deleting them.")
                    delete_user_credentials(username)
                    logger.info(f"Deleted credentials for first-time user {username} with incorrect version.")

            return (
                jsonify({"status": "error", "message": f"Incorrect version number. Please update the app to version {current_version}."}),
                403,
            )

        # Authentication
        auth_start_time = time.perf_counter()
        password_to_use = validate_credentials_flow(username, password, first_time)
        auth_duration = (time.perf_counter() - auth_start_time) * 1000
        logger.info(f"TIMING: Auth flow took {auth_duration:.2f} ms")

        # Cache checks: in-memory first, then Redis
        cache_key = generate_cache_key(CACHE_PREFIX, username)

        # In-memory cache check
        in_memory_cache_check_start_time = time.perf_counter()
        cached_data = get_from_memory_cache(cache_key)
        in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
        logger.info(f"TIMING: In-memory Cache check took {in_memory_cache_check_duration:.2f} ms")

        if cached_data is not None:
            logger.info(f"Serving guc_data from IN-MEMORY cache for {username}")
            g.log_outcome = "memory_cache_hit"
            # Use a shallow copy so we don't mutate the cached object
            resp = dict(cached_data) if isinstance(cached_data, dict) else cached_data
            if get_dev_announcement_enabled_cached():
                try:
                    dev_announcement = get_dev_announcement_cached()
                    original_guc_notifications = resp.get("notifications", [])
                    if not isinstance(original_guc_notifications, list):
                        original_guc_notifications = []

                    final_notifications_list = []
                    if dev_announcement:
                        if not any(n.get("id") == dev_announcement.get("id") for n in original_guc_notifications):
                            final_notifications_list.append(dev_announcement)
                    final_notifications_list.extend(original_guc_notifications)
                    resp["notifications"] = final_notifications_list
                except Exception as e:
                    logger.error(f"Failed to add dev announcement or user-specific notifications to cached guc_data (in-memory): {e}")

            dev_announce_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
            logger.info(f"TIMING: Get/Add Dev Announce (In-memory Cache Hit) took {dev_announce_duration:.2f} ms")
            return jsonify(resp), 200

        # Redis cache check
        redis_cache_check_start_time = time.perf_counter()
        cached_data = get_from_cache(cache_key)
        redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
        logger.info(f"TIMING: Redis Cache check took {redis_cache_check_duration:.2f} ms")

        if cached_data is not None:
            logger.info(f"Serving guc_data from REDIS cache for {username}")
            g.log_outcome = "redis_cache_hit"
            # set in in-memory cache for future quick access
            set_in_memory_cache(cache_key, cached_data, ttl=GUC_DATA_MEMORY_CACHE_TTL)
            logger.info(f"Set guc_data in IN-MEMORY cache for {username}")

            resp = dict(cached_data) if isinstance(cached_data, dict) else cached_data
            if get_dev_announcement_enabled_cached():
                try:
                    dev_announcement = get_dev_announcement_cached()
                    original_guc_notifications = resp.get("notifications", [])
                    if not isinstance(original_guc_notifications, list):
                        original_guc_notifications = []

                    final_notifications_list = []
                    if dev_announcement:
                        if not any(n.get("id") == dev_announcement.get("id") for n in original_guc_notifications):
                            final_notifications_list.append(dev_announcement)
                    final_notifications_list.extend(original_guc_notifications)
                    resp["notifications"] = final_notifications_list
                except Exception as e:
                    logger.error(f"Failed to add dev announcement or user-specific notifications to cached guc_data: {e}")

            dev_announce_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
            logger.info(f"TIMING: Get/Add Dev Announce (Redis Cache Hit) took {dev_announce_duration:.2f} ms")
            return jsonify(resp), 200

        # Cache miss -> scrape
        logger.info(f"Cache miss for guc_data (both in-memory and Redis). Starting sync scrape for {username}")
        g.log_outcome = "scrape_attempt"
        scrape_call_start_time = time.perf_counter()

        scrape_result = scrape_guc_data_fast(username, password_to_use)
        scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(f"TIMING: Guc data scrape took {scrape_call_duration:.2f} ms")

        if scrape_result is None:
            logger.error(f"GUC data scraping returned None for {username}")
            g.log_outcome = "scrape_critical_fail"
            g.log_error_message = "Scraping function returned None (critical failure)"
            return jsonify({"status": "error", "message": "Critical error during data scraping."}), 500

        if isinstance(scrape_result, AuthError):
            logger.warning(f"AuthError during guc_data scrape for {username}: {scrape_result.message}")
            g.log_outcome = "scrape_auth_error"
            g.log_error_message = f"Auth error during scrape: {scrape_result.message}"
            return jsonify({"status": "error", "message": scrape_result.message}), 401

        if isinstance(scrape_result, dict) and scrape_result.get("error"):
            error_message = scrape_result.get("message", "Unknown scraping error")
            logger.error(f"GUC data scraping failed for {username}: {error_message}")
            g.log_outcome = "scrape_fail"
            g.log_error_message = error_message
            return jsonify({"status": "error", "message": error_message}), 500

        # Success path: cache canonical scrape_result, but add dev announcement only in response copy
        g.log_outcome = "scrape_success"
        logger.info(f"Successfully scraped guc_data for {username}")

        cache_set_start_time = time.perf_counter()
        set_in_cache(cache_key, scrape_result, timeout=config.CACHE_DEFAULT_TIMEOUT)
        cache_set_duration = (time.perf_counter() - cache_set_start_time) * 1000
        logger.info(f"TIMING: Redis Cache set took {cache_set_duration:.2f} ms")
        logger.info(f"Cached fresh guc_data in REDIS for {username}")

        set_in_memory_cache(cache_key, scrape_result, ttl=GUC_DATA_MEMORY_CACHE_TTL)
        logger.info(f"Set fresh guc_data in IN-MEMORY cache for {username}")

        # Build response copy and attach dev announcement
        resp = dict(scrape_result) if isinstance(scrape_result, dict) else scrape_result
        if get_dev_announcement_enabled_cached():
            try:
                dev_announcement = get_dev_announcement_cached()
                original_guc_notifications = resp.get("notifications", [])
                if not isinstance(original_guc_notifications, list):
                    original_guc_notifications = []

                final_notifications_list = []
                if dev_announcement:
                    if not any(n.get("id") == dev_announcement.get("id") for n in original_guc_notifications):
                        final_notifications_list.append(dev_announcement)
                final_notifications_list.extend(original_guc_notifications)
                resp["notifications"] = final_notifications_list
            except Exception as e:
                logger.error(f"Failed to add dev announcement or user-specific notifications to scraped guc_data: {e}")

        dev_announce_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(f"TIMING: Get/Add Dev Announce (Scrape Success) took {dev_announce_duration:.2f} ms")
        return jsonify(resp), 200

    except AuthError as e:
        logger.warning(f"AuthError during GUC data request for {username}: {e.log_message}")
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(f"Unhandled exception during /api/guc_data request for {username}: {e}")
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"

        error_msg = str(e).lower()
        if "auth" in error_msg or "login failed" in error_msg or "credentials" in error_msg or "password" in error_msg:
            logger.warning(f"Authentication error detected in exception: {e}")
            return jsonify({"status": "error", "message": "Invalid credentials"}), 401

        return jsonify({"status": "error", "message": "An internal server error occurred"}), 500
    finally:
        total_duration_final = (time.perf_counter() - req_start_time) * 1000
        logger.info(f"TIMING: Request processing finished in {total_duration_final:.2f} ms (Outcome: {getattr(g, 'log_outcome', 'unknown')})")
