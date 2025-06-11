# api/schedule.py
import logging
from flask import Blueprint, request, jsonify, g

from config import config
from utils.auth import (
    AuthError, 
    get_password_for_readonly_session
)
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from scraping.schedule import (
    scrape_schedule,
    filter_schedule_details,
)  # Import necessary functions
from scraping.staff_schedule_scraper import scrape_staff_schedule # New import
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


def is_schedule_empty(schedule_data: dict) -> bool:
    """
    Check if a schedule contains any meaningful course data.

    Returns True if all Course_Name fields are empty, "Unknown", "Free", "N/A",
    or if the schedule is completely empty.

    Args:
        schedule_data: The filtered schedule dictionary

    Returns:
        bool: True if schedule is considered empty, False otherwise
    """
    if not schedule_data or not isinstance(schedule_data, dict):
        return True

    # Values that indicate no meaningful course data
    empty_values = {"", "Unknown", "Free", "N/A", "Error", "Parsing Failed"}

    for day, periods in schedule_data.items():
        if not isinstance(periods, dict):
            continue

        for period_name, period_details in periods.items():
            if not isinstance(period_details, dict):
                continue

            course_name = period_details.get("Course_Name", "")
            # If we find any course name that's not in the empty values set,
            # the schedule has meaningful data
            if course_name not in empty_values:
                return False

    # All course names were empty/unknown/free, so schedule is considered empty
    return True


@schedule_bp.route("/schedule", methods=["GET"])
def api_schedule():
    """
    Endpoint to fetch the user's schedule.
    Requires query params: username, password.
    """
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for Schedule API.")
        g.log_outcome = "bot_check_success"
        return jsonify({
            "status": "Success",
            "message": "Schedule API route is up!",
            "data": None,
        }), 200

    username = request.args.get("username")
    password = request.args.get("password")
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock schedule data for user {username}")
        g.log_outcome = "mock_data_served"
        return jsonify(schedule_mockData), 200

    try:
        if not username or not password:
            g.log_outcome = "validation_error_schedule"
            g.log_error_message = "Missing required parameters (username, password) for Schedule"
            return jsonify({
                "status": "error",
                "message": "Missing required parameters: username, password"
            }), 400

        password_to_use = get_password_for_readonly_session(username, password)

        cache_key = generate_cache_key("schedule", username)
        cached_data = get_from_cache(cache_key)

        if cached_data:
            # Validate cached data structure (should be a list/tuple of length 2)
            if isinstance(cached_data, (list, tuple)) and len(cached_data) == 2:
                # Check if it's a schedule update message (Monday with "Schedule is being updated")
                schedule_data = cached_data[0]
                if (isinstance(schedule_data, dict) and
                    "Monday" in schedule_data and
                    isinstance(schedule_data["Monday"], dict) and
                    "First Period" in schedule_data["Monday"] and
                    isinstance(schedule_data["Monday"]["First Period"], dict) and
                    schedule_data["Monday"]["First Period"].get("Course_Name") == "Schedule is being updated"):
                    logger.info(f"Serving schedule update message from cache for {username}")
                    g.log_outcome = "cache_hit_empty_schedule"
                else:
                    logger.info(f"Serving schedule from cache for {username}")
                    g.log_outcome = "cache_hit"
                # Return the cached tuple directly (whether update message or full schedule)
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
            status_code = 500  # Default
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

            # Check if the schedule is empty (no meaningful course data)
            if is_schedule_empty(filtered_data):
                logger.info(f"Schedule for {username} contains no meaningful course data, returning schedule update message")
                g.log_outcome = "scrape_success_empty_schedule"

                # Prepare schedule update message response
                schedule_update_message = {
                    "Monday": {
                        "First Period": {
                            "Course_Name": "Schedule is being updated",
                            "Location": "No location",
                            "Type": "No Type"
                        }
                    }
                }
                empty_schedule_response = (schedule_update_message, TIMINGS)

                # Cache the empty result
                set_in_cache(
                    cache_key, empty_schedule_response, timeout=config.CACHE_LONG_TIMEOUT
                )  # Cache schedule update message
                logger.info(f"Cached schedule update message for {username}")

                return jsonify(empty_schedule_response), 200

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

@schedule_bp.route("/staff_schedule", methods=["POST"])
def api_staff_schedule():
    """
    Endpoint to fetch a specific staff member's schedule using their name.
    Requires a JSON body with: username, password, staff_name
    """
    data = request.get_json()
    if not data:
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing JSON request body for staff schedule"
        return jsonify({"status": "error", "message": "Missing JSON request body"}), 400

    username = data.get("username")
    password = data.get("password") # This is the temporary password/token from client
    staff_name = data.get("staff_name")
    g.username = username # For logging context

    required_params = [
        ("username", username),
        ("password", password),
        ("staff_name", staff_name),
    ]

    missing = [name for name, val in required_params if not val or not str(val).strip()]
    if missing:
        msg = f"Missing or empty required JSON parameters for staff schedule: {', '.join(missing)}"
        logger.warning(f"{msg} (User: {username or 'N/A'})")
        g.log_outcome = "validation_error_staff_schedule"
        g.log_error_message = msg
        return jsonify({"status": "error", "message": msg}), 400

    # Normalize staff_name for consistent caching and logging
    normalized_staff_name = staff_name.strip() # Used for user-specific cache key and logging

    try:
        # --- Pre-warmed Cache Check ---
        # Key format matches 'scripts/refresh_staff_schedules.py'
        # _normalize_staff_name -> " ".join(name.lower().split())
        # refresh script key part -> "_".join(_normalize_staff_name(original_staff_name).split())
        prewarm_key_staff_part = "_".join(normalized_staff_name.lower().split())
        prewarm_cache_key = f"staff_schedule_PREWARM_{prewarm_key_staff_part}"
        
        logger.debug(f"Checking pre-warmed cache for staff '{normalized_staff_name}' with key: {prewarm_cache_key}")
        prewarmed_cached_data = get_from_cache(prewarm_cache_key)

        if prewarmed_cached_data:
            if isinstance(prewarmed_cached_data, dict) and "error" not in prewarmed_cached_data:
                logger.info(f"Serving staff schedule for '{normalized_staff_name}' from PREWARMED cache (User: {username})")
                g.log_outcome = "staff_prewarm_cache_hit"
                return jsonify({"status": "success", "data": prewarmed_cached_data}), 200
            else:
                logger.warning(f"Invalid or error data found in PREWARMED staff schedule cache for '{normalized_staff_name}' (User: {username}). Key: {prewarm_cache_key}. Proceeding to user-specific cache.")

        # --- User-Specific Cache Check (existing logic) ---
        user_cache_key = generate_cache_key("staff_schedule", username, normalized_staff_name) # `normalized_staff_name` is already stripped
        logger.debug(f"Checking user-specific cache for staff '{normalized_staff_name}' (User: {username}) with key: {user_cache_key}")
        user_cached_data = get_from_cache(user_cache_key)
        
        if user_cached_data:
            if isinstance(user_cached_data, dict) and "error" not in user_cached_data:
                logger.info(f"Serving staff schedule for '{normalized_staff_name}' from USER-SPECIFIC cache for user {username}")
                g.log_outcome = "staff_user_cache_hit" # Differentiated log outcome
                return jsonify({"status": "success", "data": user_cached_data}), 200
            else:
                logger.warning(f"Invalid or error data found in USER-SPECIFIC staff schedule cache for '{normalized_staff_name}' (User: {username}). Key: {user_cache_key}. Fetching fresh.")

        logger.info(f"Cache miss for staff schedule: '{normalized_staff_name}' (User: {username}) in both pre-warmed and user-specific caches. Proceeding to scrape.")
        actual_password = get_password_for_readonly_session(username, password)
        # AuthError from get_password_for_readonly_session will be caught by the AuthError handler below

        from scraping.authenticate import authenticate_user_session 
        session = authenticate_user_session(username, actual_password)
        if not session:
            logger.error(f"Failed to get authenticated session for {username} for staff schedule (staff: {normalized_staff_name}).")
            g.log_outcome = "session_error_staff_schedule"
            g.log_error_message = "Failed to establish authenticated session with GUC portal."
            return jsonify({"status": "error", "message": "Failed to authenticate with GUC portal"}), 502

        logger.info(f"Fetching staff schedule for staff_name: '{normalized_staff_name}', requested by {username}")
        
        schedule_result = scrape_staff_schedule(session, normalized_staff_name)

        if isinstance(schedule_result, dict) and "error" in schedule_result:
            error_msg = schedule_result["error"]
            logger.warning(f"Staff schedule scraping error for staff_name '{normalized_staff_name}' (User: {username}): {error_msg}")
            g.log_error_message = error_msg
            status_code = 500 # Default internal error
            
            if "Could not find staff ID" in error_msg:
                g.log_outcome = "staff_name_not_found"
                status_code = 404
            elif "timed out" in error_msg.lower():
                g.log_outcome = "staff_scrape_timeout"
                status_code = 504
            elif "HTTP error" in error_msg or "Network or HTTP error" in error_msg or "Network or request error" in error_msg:
                g.log_outcome = "staff_scrape_http_error"
                status_code = 502
            elif "parsing failed" in error_msg.lower() or "ASP.NET tokens" in error_msg:
                g.log_outcome = "staff_scrape_parsing_or_token_error"
                status_code = 502
            else:
                g.log_outcome = "staff_scrape_unknown_error"
            # Do not cache errors
            return jsonify({"status": "error", "message": f"Failed to fetch staff schedule for '{normalized_staff_name}': {error_msg}"}), status_code
        
        # Ensure schedule_result is a dict and doesn't have an error before caching
        if isinstance(schedule_result, dict) and "error" not in schedule_result:
            # Cache the successful result in the USER-SPECIFIC cache
            set_in_cache(
                user_cache_key, # Use the user_cache_key defined earlier
                schedule_result,
                timeout=config.CACHE_STAFF_SCHEDULE_TIMEOUT # Existing timeout for user-specific staff schedule
            )
            logger.info(f"Cached fresh staff schedule for '{normalized_staff_name}' in USER-SPECIFIC cache for user {username} (Key: {user_cache_key})")
            g.log_outcome = "staff_scrape_success_cached_user"
            return jsonify({"status": "success", "data": schedule_result}), 200
        elif isinstance(schedule_result, dict) and "error" in schedule_result: # Already handled above, but as a safeguard
             logger.error(f"Scraping returned an error for '{normalized_staff_name}', not caching. Error: {schedule_result.get('error')}")
             # The error response is already constructed and returned above this block.
             # This path should ideally not be hit if error handling above is complete.
             # For safety, ensure an error is returned if somehow reached:
             return jsonify({"status": "error", "message": schedule_result.get('error', 'Scraping failed'), "data": None}), 500
        else: # Scraper returned something unexpected (not a dict or dict without error but also not success)
            logger.error(f"Scraping for '{normalized_staff_name}' returned unexpected data type or format. Data: {str(schedule_result)[:200]}. Not caching.")
            g.log_outcome = "staff_scrape_invalid_data"
            return jsonify({"status": "error", "message": "Scraping returned invalid data for staff schedule.", "data": None}), 500

    except AuthError as e:
        logger.warning(f"AuthError for {username} during staff schedule request (staff: {normalized_staff_name}): {e.log_message}")
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(f"Unhandled exception for {username} during /api/staff_schedule (staff: {normalized_staff_name}): {e}")
        g.log_outcome = "internal_error_unhandled_staff"
        g.log_error_message = f"Unhandled staff schedule exception: {e}"
        return jsonify({"status": "error", "message": f"An internal server error occurred processing staff schedule for '{normalized_staff_name}'."}), 500
