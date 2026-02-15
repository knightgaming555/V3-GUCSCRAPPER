# api/exams.py
import logging
from flask import Blueprint, request, jsonify, g
import time # Added for timing logs

from config import config
from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from utils.helpers import get_from_memory_cache, set_in_memory_cache # Import in-memory cache functions
from scraping.exams import scrape_exam_seats  # Import the main exam seats scraper
from utils.mock_data import exam_mockData

logger = logging.getLogger(__name__)
exams_bp = Blueprint("exams_bp", __name__)

CACHE_PREFIX = "exam_seats"
# Define a short TTL for in-memory cache for hot exam seats data
EXAM_SEATS_MEMORY_CACHE_TTL = 1800 #30 Minutes


@exams_bp.route("/exam_seats", methods=["GET"])
def api_exam_seats():
    """
    Endpoint to fetch exam seat assignments for the user.
    Uses cache first, then scrapes if needed.
    Requires query params: username, password
    """
    req_start_time = time.perf_counter() # Overall request start

    # --- Bot Health Check ---
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for Exam Seats API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "Exam Seats API route is up!",
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
        logger.info(f"Serving mock exam_seats data for user {username}")
        g.log_outcome = "mock_data_served"
        # Use the imported mock data and jsonify it
        return jsonify(exam_mockData), 200

    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Cache Check (In-Memory first, then Redis) ---
        cache_key = generate_cache_key(CACHE_PREFIX, username)
        if not force_refresh:
            # 1. Check in-memory cache
            in_memory_cache_check_start_time = time.perf_counter()
            cached_data = get_from_memory_cache(cache_key)
            in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
            logger.info(f"TIMING: In-memory Cache check for exam seats took {in_memory_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty list [] from cache
                logger.info(f"Serving exam seats from IN-MEMORY cache for {username}")
                g.log_outcome = "memory_cache_hit"
                return jsonify(cached_data), 200

            # 2. If not in-memory, check Redis cache
            redis_cache_check_start_time = time.perf_counter()
            cached_data = get_from_cache(cache_key)
            redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
            logger.info(f"TIMING: Redis Cache check for exam seats took {redis_cache_check_duration:.2f} ms")

            if cached_data is not None:  # Allow empty list [] from cache
                logger.info(f"Serving exam seats from REDIS cache for {username}")
                g.log_outcome = "redis_cache_hit"
                # Set in in-memory cache for future rapid access
                set_in_memory_cache(cache_key, cached_data, ttl=EXAM_SEATS_MEMORY_CACHE_TTL)
                logger.info(f"Set exam seats in IN-MEMORY cache for {username}")
                return jsonify(cached_data), 200

        # --- Cache Miss -> Scrape ---
        logger.info(
            f"Cache miss or forced refresh for exam seats (both in-memory and Redis). Scraping for {username}"
        )
        g.log_outcome = "scrape_attempt"
        scrape_call_start_time = time.perf_counter()

        # Call the scraping function
        # It returns list on success (can be empty), None on critical failure
        seats_data = scrape_exam_seats(username, password_to_use)
        scrape_call_duration = (time.perf_counter() - scrape_call_start_time) * 1000
        logger.info(f"TIMING: Exam seats scrape took {scrape_call_duration:.2f} ms")

        # --- Handle Scraping Result ---
        if seats_data is None:
            # Scraper failed critically (Auth or Network or Critical Parse Error)
            g.log_outcome = (
                "scrape_error"  # More specific outcome might be logged by scraper
            )
            g.log_error_message = "Exam seats scraper returned None (critical failure)"
            logger.error(f"Critical failure during exam seats scraping for {username}.")
            # Distinguish between auth failure (if possible) and other errors
            # Assuming scraper returns None mainly on network/auth, return 502/401
            # Need scraper to potentially return {"error": "Authentication failed"} for clarity
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to fetch exam seats data due to a server or authentication error",
                    }
                ),
                502,
            )  # Or 401 if confirmed auth error
        else:
            # --- Success ---
            # seats_data is a list (can be empty [])
            g.log_outcome = "scrape_success" if seats_data else "scrape_success_nodata"
            logger.info(
                f"Successfully scraped exam seats for {username}. Found {len(seats_data)} seats."
            )

            # Cache the successful result (can be empty list) in Redis
            set_in_cache(
                cache_key, seats_data, timeout=config.CACHE_DEFAULT_TIMEOUT
            )  # Cache for default period
            logger.info(f"Cached fresh exam seats in REDIS for {username}")

            # Cache the successful result in in-memory
            set_in_memory_cache(cache_key, seats_data, ttl=EXAM_SEATS_MEMORY_CACHE_TTL)
            logger.info(f"Cached fresh exam seats in IN-MEMORY for {username}")

            # Return the scraped data list
            return jsonify(seats_data), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during exam seats request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/exam_seats for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )
