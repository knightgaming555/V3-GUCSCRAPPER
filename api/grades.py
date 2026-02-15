# api/grades.py
import logging
from flask import Blueprint, request, jsonify, g
import time # Added for timing logs

from config import config
from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from utils.helpers import get_from_memory_cache, set_in_memory_cache # Import in-memory cache functions
from scraping.grades import scrape_grades  # Import the main grades scraper
from utils.mock_data import grades_mockData

logger = logging.getLogger(__name__)
grades_bp = Blueprint("grades_bp", __name__)

CACHE_PREFIX = "grades"
# Define a short TTL for in-memory cache for hot grades data
GRADES_MEMORY_CACHE_TTL = 1800 #30 Minutes


@grades_bp.route("/grades", methods=["GET"])
def api_grades():
    """
    Endpoint to fetch midterm and detailed grades for the user.
    Uses cache first, then scrapes if needed.
    Requires query params: username, password
    """
    req_start_time = time.perf_counter() # Overall request start

    # --- Bot Health Check ---
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for Grades API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "Grades API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    # --- Parameter Extraction & Validation ---
    username = request.args.get("username")
    password = request.args.get("password")
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock grades data for user {username}")
        g.log_outcome = "mock_data_served"
        # Use the imported mock data and jsonify it
        return jsonify(grades_mockData), 200
    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Cache Check (In-Memory first, then Redis) ---
        cache_key = generate_cache_key(CACHE_PREFIX, username)
        if not force_refresh:
            # 1. Check in-memory cache
            in_memory_cache_check_start_time = time.perf_counter()
            cached_data = get_from_memory_cache(cache_key)
            in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
            logger.info(f"TIMING: In-memory Cache check for grades took {in_memory_cache_check_duration:.2f} ms")

            if cached_data is not None: # Allow empty dict/list from cache if that's valid
                logger.info(f"Serving grades from IN-MEMORY cache for {username}")
                g.log_outcome = "memory_cache_hit"
                return jsonify(cached_data), 200

            # 2. If not in-memory, check Redis cache
            redis_cache_check_start_time = time.perf_counter()
            cached_data = get_from_cache(cache_key)
            redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
            logger.info(f"TIMING: Redis Cache check for grades took {redis_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty dict/list from cache if that's valid
                logger.info(f"Serving grades from REDIS cache for {username}")
                g.log_outcome = "redis_cache_hit"
                # Set in in-memory cache for future rapid access
                set_in_memory_cache(cache_key, cached_data, ttl=GRADES_MEMORY_CACHE_TTL)
                logger.info(f"Set grades in IN-MEMORY cache for {username}")
                return jsonify(cached_data), 200

        # --- Cache Miss -> Scrape ---
        logger.info(f"Cache miss or forced refresh for grades (both in-memory and Redis). Scraping for {username}")
        g.log_outcome = "scrape_attempt"
        scrape_call_start_time = time.perf_counter()

        # Call the scraping function (scrape_grades handles its own errors/retries)
        # It returns the grades dict or None on critical failure
        grades_data = scrape_grades(username, password_to_use)
        scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(f"TIMING: Grades scrape took {scrape_call_duration:.2f} ms")

        # --- Handle Scraping Result ---
        if grades_data is None:
            # Scraper failed critically (e.g., initial page fetch failed)
            g.log_outcome = "scrape_error"
            g.log_error_message = "Grades scraper returned None (critical failure)"
            logger.error(f"Critical failure during grades scraping for {username}.")
            # Return 500 or 502 depending on assumption
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to fetch grades data due to a server error",
                    }
                ),
                500,
            )
        elif isinstance(grades_data, dict) and "error" in grades_data:
            # Handle specific errors returned by the scraper (e.g., auth failed)
            error_msg = grades_data["error"]
            logger.error(
                f"Grades scraping returned specific error for {username}: {error_msg}"
            )
            g.log_error_message = error_msg
            if "Authentication failed" in error_msg:
                g.log_outcome = "scrape_auth_error"
                status_code = 401
            # Add checks for other specific errors if scrape_grades provides them
            else:
                g.log_outcome = "scrape_returned_error"
                status_code = 502  # Assume upstream error if not auth
            return jsonify({"status": "error", "message": error_msg}), status_code
        else:
            # --- Success ---
            # grades_data could be {} or {'midterm_results': {}, 'subject_codes': {}, 'detailed_grades': {}} if scraping succeeded but found nothing.
            g.log_outcome = "scrape_success"
            logger.info(f"Successfully scraped grades for {username}")

            # Cache the successful result in Redis
            set_in_cache(
                cache_key, grades_data, timeout=config.CACHE_DEFAULT_TIMEOUT
            )  # Use default cache timeout
            logger.info(f"Cached fresh grades in REDIS for {username}")

            # Cache the successful result in in-memory
            set_in_memory_cache(cache_key, grades_data, ttl=GRADES_MEMORY_CACHE_TTL)
            logger.info(f"Cached fresh grades in IN-MEMORY for {username}")

            # Return the scraped data (can be an empty dict if no grades found)
            return jsonify(grades_data), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during grades request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(f"Unhandled exception during /api/grades for {username}: {e}")
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )
