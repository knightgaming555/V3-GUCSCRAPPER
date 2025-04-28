# api/grades.py
import logging
from flask import Blueprint, request, jsonify, g

from config import config
from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from scraping.grades import scrape_grades  # Import the main grades scraper
from utils.mock_data import grades_mockData

logger = logging.getLogger(__name__)
grades_bp = Blueprint("grades_bp", __name__)

CACHE_PREFIX = "grades"


@grades_bp.route("/grades", methods=["GET"])
def api_grades():
    """
    Endpoint to fetch midterm and detailed grades for the user.
    Uses cache first, then scrapes if needed.
    Requires query params: username, password
    """
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

        # --- Cache Check ---
        cache_key = generate_cache_key(CACHE_PREFIX, username)
        if not force_refresh:
            cached_data = get_from_cache(cache_key)
            if (
                cached_data is not None
            ):  # Allow empty dict/list from cache if that's valid
                logger.info(f"Serving grades from cache for {username}")
                g.log_outcome = "cache_hit"
                return jsonify(cached_data), 200

        # --- Cache Miss -> Scrape ---
        logger.info(f"Cache miss or forced refresh for grades. Scraping for {username}")
        g.log_outcome = "scrape_attempt"

        # Call the scraping function (scrape_grades handles its own errors/retries)
        # It returns the grades dict or None on critical failure
        grades_data = scrape_grades(username, password_to_use)

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

            # Cache the successful result
            set_in_cache(
                cache_key, grades_data, timeout=config.CACHE_DEFAULT_TIMEOUT
            )  # Use default cache timeout
            logger.info(f"Cached fresh grades for {username}")

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
