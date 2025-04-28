# api/schedule.py
import logging
from flask import Blueprint, request, jsonify, g

from config import config
from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from scraping.schedule import (
    scrape_schedule,
    filter_schedule_details,
)  # Import necessary functions
from utils.mock_data import schedule_mockData

logger = logging.getLogger(__name__)
schedule_bp = Blueprint("schedule_bp", __name__)

# Define timings here or import from a central place if used elsewhere
TIMINGS = {
    "0": "8:15AM-9:45AM",
    "1": "10:00AM-11:30AM",
    "2": "11:45AM-1:15PM",
    "3": "1:45PM-3:15PM",
    "4": "3:45PM-5:15PM",
}


@schedule_bp.route("/schedule", methods=["GET"])
def api_schedule():
    """
    Endpoint to fetch the user's schedule.
    Uses cache first, then scrapes if needed.
    Returns filtered schedule details along with timings.
    Requires query params: username, password
    """
    # --- Bot Health Check ---
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for Schedule API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "Schedule API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    # --- Parameter Extraction & Validation ---
    username = request.args.get("username")
    password = request.args.get("password")
    g.username = username  # Set for logging

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock guc_data data for user {username}")
        g.log_outcome = "mock_data_served"
        # Use the imported mock data and jsonify it
        return jsonify(schedule_mockData), 200

    # Use centralized auth flow (raises AuthError on failure)
    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Cache Check ---
        # Cache stores the tuple: (filtered_schedule, timings)
        cache_key = generate_cache_key("schedule", username)
        cached_data = get_from_cache(cache_key)

        if cached_data:
            # Validate cached data structure (should be a list/tuple of length 2)
            if isinstance(cached_data, (list, tuple)) and len(cached_data) == 2:
                logger.info(f"Serving schedule from cache for {username}")
                g.log_outcome = "cache_hit"
                # Return the cached tuple directly
                return jsonify(cached_data), 200
            else:
                logger.warning(
                    f"Invalid schedule data format found in cache for {username}. Fetching fresh data."
                )
                # Optionally delete invalid cache item here
                # delete_from_cache(cache_key)

        # --- Cache Miss -> Scrape ---
        logger.info(f"Cache miss for schedule. Starting scrape for {username}")
        g.log_outcome = "scrape_attempt"

        # Call the scraping function
        # scrape_schedule now returns the raw parsed schedule dict or dict with 'error'
        raw_schedule_data = scrape_schedule(username, password_to_use)

        # --- Handle Scraping Result ---
        if isinstance(raw_schedule_data, dict) and "error" in raw_schedule_data:
            error_msg = raw_schedule_data["error"]
            logger.error(f"Schedule scraping error for {username}: {error_msg}")
            g.log_error_message = error_msg
            # Map error message to status code and log outcome
            if "Authentication failed" in error_msg:
                g.log_outcome = "scrape_auth_error"
                status_code = 401
            elif any(
                e in error_msg
                for e in [
                    "timeout",
                    "Connection error",
                    "HTTP error",
                    "Failed to fetch",
                ]
            ):
                g.log_outcome = "scrape_connection_error"
                status_code = 504  # Gateway timeout / upstream error
            elif any(e in error_msg for e in ["parse", "Failed to find", "content"]):
                g.log_outcome = "scrape_parsing_error"
                status_code = 502  # Bad gateway - parsing issue
            else:
                g.log_outcome = "scrape_unknown_error"
                status_code = 500
            return (
                jsonify({"status": "error", "message": error_msg, "data": None}),
                status_code,
            )

        elif not raw_schedule_data or not isinstance(raw_schedule_data, dict):
            # Handle case where scraper returns None or unexpected type
            logger.error(
                f"Schedule scraping returned invalid data type for {username}: {type(raw_schedule_data)}"
            )
            g.log_outcome = "scrape_no_result"
            g.log_error_message = "Scraping function returned invalid data"
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to fetch schedule data (invalid result)",
                    }
                ),
                500,
            )
        else:
            # --- Success ---
            g.log_outcome = "scrape_success"
            logger.info(f"Successfully scraped schedule for {username}")

            # Filter the raw data
            filtered_data = filter_schedule_details(raw_schedule_data)

            # Prepare the response tuple (filtered_schedule, timings)
            response_data = (filtered_data, TIMINGS)

            # Cache the successful result (the tuple)
            set_in_cache(
                cache_key, response_data, timeout=config.CACHE_LONG_TIMEOUT
            )  # Use long timeout
            logger.info(f"Cached fresh schedule for {username}")

            return jsonify(response_data), 200

    except AuthError as e:
        # Handle authentication errors from validate_credentials_flow
        logger.warning(
            f"AuthError during schedule request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        # Catch unexpected errors in the endpoint logic
        logger.exception(
            f"Unhandled exception during /api/schedule request for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )
