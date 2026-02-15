# api/attendance.py
import logging
from flask import Blueprint, request, jsonify, g
import time # Added for timing logs

from config import config
from utils.auth import validate_credentials_flow, AuthError

# delete_from_cache might be useful if force_refresh should also clear existing cache first
from utils.cache import (
    get_from_cache,
    set_in_cache,
    generate_cache_key,
    delete_from_cache,
)
from utils.helpers import get_from_memory_cache, set_in_memory_cache # Import in-memory cache functions
from scraping.attendance import scrape_attendance  # Import the updated scraper
from utils.mock_data import attendance_mockData

logger = logging.getLogger(__name__)
attendance_bp = Blueprint("attendance_bp", __name__)

CACHE_PREFIX = "attendance"
# Define a short TTL for in-memory cache for hot attendance data
ATTENDANCE_MEMORY_CACHE_TTL = 1800 #30 Minutes


@attendance_bp.route("/attendance", methods=["GET"])
def api_attendance():
    """
    Endpoint to fetch attendance data (summary level + sessions) for all courses.
    Uses cache first, then scrapes if needed.
    Requires query params: username, password
    Optional query param: force_fetch=true (to bypass cache)
    """
    req_start_time = time.perf_counter() # Overall request start

    if request.args.get("bot", "").lower() == "true":  # Bot check
        logger.info("Received bot health check request for Attendance API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "Attendance API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    # Read the force_fetch parameter, default to False if not present or invalid value

    force_fetch = request.args.get("force_fetch", "false", type=str).lower() == "true"
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock attendance data for user {username}")
        g.log_outcome = "mock_data_served"
        # Use the imported mock data and jsonify it
        return jsonify(attendance_mockData), 200
    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Cache Check (In-Memory first, then Redis) ---
        cache_key = generate_cache_key(CACHE_PREFIX, username)

        # ---> Add force_fetch condition here <---
        if not force_fetch:
            # 1. Check in-memory cache
            in_memory_cache_check_start_time = time.perf_counter()
            cached_data = get_from_memory_cache(cache_key)
            in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
            logger.info(f"TIMING: In-memory Cache check for attendance took {in_memory_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty dict {} from cache
                logger.info(f"Serving attendance from IN-MEMORY cache for {username}")
                g.log_outcome = "memory_cache_hit"
                return jsonify(cached_data), 200

            # 2. If not in-memory, check Redis cache
            redis_cache_check_start_time = time.perf_counter()
            cached_data = get_from_cache(cache_key)
            redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
            logger.info(f"TIMING: Redis Cache check for attendance took {redis_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty dict {} from cache
                logger.info(f"Serving attendance from REDIS cache for {username}")
                g.log_outcome = "redis_cache_hit"
                # Set in in-memory cache for future rapid access
                set_in_memory_cache(cache_key, cached_data, ttl=ATTENDANCE_MEMORY_CACHE_TTL)
                logger.info(f"Set attendance in IN-MEMORY cache for {username}")
                return jsonify(cached_data), 200
            else:
                logger.info(
                    f"Cache miss for attendance (both in-memory and Redis). Will proceed to scrape for {username}"
                )
        else:
            logger.info(
                f"Force fetch requested for attendance. Bypassing cache check for {username}."
            )
            # Optional: Delete existing cache entry when forcing refresh
            # delete_from_cache(cache_key)

        # --- Cache Miss or Force Fetch -> Scrape ---
        # This block now runs if it's a cache miss OR if force_fetch is true
        logger.info(
            f"Starting attendance scrape for {username} (Force Fetch: {force_fetch})"
        )
        g.log_outcome = "scrape_attempt"
        scrape_call_start_time = time.perf_counter()

        attendance_data = scrape_attendance(username, password_to_use)
        scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(f"TIMING: Attendance scrape took {scrape_call_duration:.2f} ms")

        # --- Handle Scraping Result ---
        if attendance_data is None:
            g.log_outcome = "scrape_error"
            g.log_error_message = "Attendance scraper returned None (critical failure)"
            logger.error(f"Critical failure during attendance scraping for {username}.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to fetch attendance data due to a server error",
                    }
                ),
                500,
            )
        else:
            # --- Success ---
            g.log_outcome = (
                "scrape_success" if attendance_data else "scrape_success_nodata"
            )
            logger.info(
                f"Successfully scraped attendance for {username}. Found data for {len(attendance_data)} courses."
            )

            # Cache in Redis
            set_in_cache(
                cache_key, attendance_data, timeout=config.CACHE_DEFAULT_TIMEOUT
            )
            logger.info(
                f"Cached fresh attendance data in REDIS for {username} (after {'force fetch' if force_fetch else 'cache miss'})"
            )

            # Cache in in-memory
            set_in_memory_cache(cache_key, attendance_data, ttl=ATTENDANCE_MEMORY_CACHE_TTL)
            logger.info(
                f"Cached fresh attendance data in IN-MEMORY for {username}"
            )

            return jsonify(attendance_data), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during attendance request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/attendance for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )
