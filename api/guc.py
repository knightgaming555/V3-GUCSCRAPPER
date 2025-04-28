# api/guc.py
import logging
import json  # Keep for JSON handling

# No asyncio needed here anymore
from flask import Blueprint, request, jsonify, g
import time  # For perf_counter timing logs

from config import config
from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key

# Import the *synchronous* scraper function (or its alias)
from scraping.guc_data import scrape_guc_data_fast, scrape_guc_data

# Import cached getters from helpers
from utils.helpers import get_version_number_cached, get_dev_announcement_cached
from utils.mock_data import guc_mockData

logger = logging.getLogger(__name__)
guc_bp = Blueprint("guc_bp", __name__)

# We might need set_dev_announcement if the cached getter should store the default
# Let's import it just in case, or move the logic entirely to helpers.py
try:
    from utils.helpers import (
        set_dev_announcement,
    )  # Assume it might be moved there later
except ImportError:
    # Fallback if still in api.guc (though it should be in helpers or utils)
    try:
        from .guc import (
            set_dev_announcement,
        )  # Relative import might work if called from app.py context
    except ImportError:

        def set_dev_announcement(a):
            logger.error("set_dev_announcement function not found!")


CACHE_PREFIX = "guc_data"  # Use consistent prefix


# Change from async def to def
@guc_bp.route("/guc_data", methods=["GET"])
def api_guc_data():
    """
    Endpoint to fetch GUC student info and notifications. Sync version using PycURL.
    Uses cache first, then scrapes. Adds dev announcement.
    """
    req_start_time = time.perf_counter()  # Overall request start
    # --- Bot Health Check ---
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

    # --- Parameter Extraction & Initial Validation ---
    username = request.args.get("username")
    password = request.args.get("password")
    req_version = request.args.get("version_number")
    first_time = request.args.get("first_time", "false").lower() == "true"
    g.username = username  # Set for logging

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock guc_data data for user {username}")
        g.log_outcome = "mock_data_served"
        # Use the imported mock data and jsonify it
        return jsonify(guc_mockData), 200

    if not username or not password or not req_version:
        g.log_outcome = "validation_error"
        g.log_error_message = (
            "Missing required parameters (username, password, version_number)"
        )
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Missing required parameters: username, password, version_number",
                }
            ),
            400,
        )

    try:  # Wrap main logic in try-finally for consistent timing log
        # --- Version Check (using memory-cached getter) ---
        version_check_start = time.perf_counter()
        current_version = get_version_number_cached()
        version_check_duration = (time.perf_counter() - version_check_start) * 1000
        logger.info(f"TIMING: Version check took {version_check_duration:.2f} ms")

        if current_version in ["Error Fetching", "Redis Unavailable"]:
            # Handle case where version check failed critically
            g.log_outcome = "internal_error_version"
            g.log_error_message = (
                f"Failed to retrieve current API version ({current_version})"
            )
            # Maybe allow request but log warning? Or return error? Let's return error.
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Could not verify API version. Please try again later.",
                    }
                ),
                503,
            )

        if req_version != current_version:
            logger.warning(
                f"Incorrect version for {username}. Required: {current_version}, Got: {req_version}"
            )
            g.log_outcome = "version_error"
            g.log_error_message = (
                f"Incorrect version. Required: {current_version}, Got: {req_version}"
            )
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Incorrect version number. Please update the app to version {current_version}.",
                    }
                ),
                403,
            )

        # --- Authentication (Remains the same) ---
        auth_start_time = time.perf_counter()
        password_to_use = validate_credentials_flow(username, password, first_time)
        auth_duration = (time.perf_counter() - auth_start_time) * 1000
        logger.info(f"TIMING: Auth flow took {auth_duration:.2f} ms")

        # --- Cache Check ---
        cache_check_start_time = time.perf_counter()
        cache_key = generate_cache_key(CACHE_PREFIX, username)
        cached_data = get_from_cache(cache_key)  # Hits Redis
        cache_check_duration = (time.perf_counter() - cache_check_start_time) * 1000
        logger.info(f"TIMING: Redis Cache check took {cache_check_duration:.2f} ms")

        if cached_data:
            logger.info(f"Serving guc_data from cache for {username}")
            g.log_outcome = "cache_hit"
            dev_announce_start_time = time.perf_counter()
            try:
                # Use memory-cached getter for announcement
                dev_announcement = get_dev_announcement_cached()
                if isinstance(cached_data.get("notifications"), list):
                    if not any(
                        n.get("id") == dev_announcement.get("id")
                        for n in cached_data["notifications"]
                    ):
                        cached_data["notifications"].insert(0, dev_announcement)
                else:
                    cached_data["notifications"] = [dev_announcement]
            except Exception as e:
                logger.error(f"Failed to add dev announcement to cached guc_data: {e}")
            dev_announce_duration = (
                time.perf_counter() - dev_announce_start_time
            ) * 1000
            logger.info(
                f"TIMING: Get/Add Dev Announce (Cache Hit) took {dev_announce_duration:.2f} ms"
            )
            return jsonify(cached_data), 200

        # --- Cache Miss -> Scrape (Use Sync Pycurl version) ---
        logger.info(f"Cache miss for guc_data. Starting sync scrape for {username}")
        g.log_outcome = "scrape_attempt"
        scrape_call_start_time = time.perf_counter()

        # Call the synchronous scrape function directly
        scrape_result = scrape_guc_data_fast(username, password_to_use)

        scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(
            f"TIMING: Sync scrape call (incl. network/parse) took {scrape_call_duration:.2f} ms"
        )

        # --- Handle Scraping Result (Mostly same as before) ---
        if scrape_result and "error" in scrape_result:
            error_msg = scrape_result["error"]
            logger.error(f"GUC data scraping error for {username}: {error_msg}")
            g.log_error_message = error_msg
            if "Authentication failed" in error_msg:
                g.log_outcome = "scrape_auth_error"
                status_code = 401
            elif any(
                e in error_msg.lower()
                for e in ["network", "fetch", "timeout", "connection", "pycurl"]
            ):
                g.log_outcome = "scrape_connection_error"
                status_code = 504
            elif any(e in error_msg.lower() for e in ["parsing", "extract"]):
                g.log_outcome = "scrape_parsing_error"
                status_code = 502
            else:
                g.log_outcome = "scrape_unknown_error"
                status_code = 500
            return (
                jsonify({"status": "error", "message": error_msg, "data": None}),
                status_code,
            )
        elif not scrape_result:
            logger.error(f"GUC data scraping returned None unexpectedly for {username}")
            g.log_outcome = "scrape_no_result"
            g.log_error_message = "Scraping function returned None"
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to fetch GUC data (scraper returned None)",
                    }
                ),
                500,
            )
        else:
            # --- Success ---
            g.log_outcome = "scrape_success"
            logger.info(f"Successfully scraped guc_data for {username}")

            cache_set_start_time = time.perf_counter()
            set_in_cache(cache_key, scrape_result, timeout=config.CACHE_DEFAULT_TIMEOUT)
            cache_set_duration = (time.perf_counter() - cache_set_start_time) * 1000
            logger.info(f"TIMING: Cache set took {cache_set_duration:.2f} ms")
            logger.info(f"Cached fresh guc_data for {username}")

            dev_announce_start_time = time.perf_counter()
            try:
                dev_announcement = get_dev_announcement_cached()
                if isinstance(scrape_result.get("notifications"), list):
                    scrape_result["notifications"].insert(0, dev_announcement)
                else:
                    scrape_result["notifications"] = [dev_announcement]
            except Exception as e:
                logger.error(f"Failed to add dev announcement to scraped guc_data: {e}")
            dev_announce_duration = (
                time.perf_counter() - dev_announce_start_time
            ) * 1000
            logger.info(
                f"TIMING: Get/Add Dev Announce (Scrape Success) took {dev_announce_duration:.2f} ms"
            )

            return jsonify(scrape_result), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during GUC data request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/guc_data request for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )
    finally:
        total_duration_final = (time.perf_counter() - req_start_time) * 1000
        logger.info(
            f"TIMING: Request processing finished in {total_duration_final:.2f} ms (Outcome: {g.log_outcome})"
        )
