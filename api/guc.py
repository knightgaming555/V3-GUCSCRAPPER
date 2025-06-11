# api/guc.py
import logging
import json  # Keep for JSON handling
import re # Added for parsing notification strings
from datetime import datetime # Added for timestamp parsing
from collections import defaultdict # Added for grouping grades

# No asyncio needed here anymore
from flask import Blueprint, request, jsonify, g
import time  # For perf_counter timing logs

from config import config
from utils.auth import validate_credentials_flow, AuthError, user_has_stored_credentials, delete_user_credentials
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
TARGET_NOTIFICATION_USERS = ["mohamed.elsaadi", "seif.elkady"] # For user-specific notifications


def _beautify_grade_updates_body(messages_list: list[str]) -> str:
    """
    Formats a list of grade update strings into a structured, readable format.
    Groups updates by course and lists items under each course.
    """
    if not messages_list:
        return "No specific updates available."

    courses = defaultdict(list)
    # Regex to capture:
    # 1. Category (e.g., "[Grades]") - ignored for now but part of the pattern
    # 2. Semester/Context (e.g., "General", "Engineering 2nd Semester")
    # 3. Course Name and Code (e.g., "SM101 Scientific Methods (A1)")
    # 4. Grade Item detail (e.g., "discussion 1: 3.5/5 (was 3.5 / 5)")
    # Basic regex: r"^\\[Grades\\] (?:.*?) - (.*?) - (.*)$" - simpler approach is splitting
    
    for message in messages_list:
        parts = message.split(" - ", 2) # Split into 3 parts max based on " - "
        if len(parts) == 3:
            # parts[0] is like "[Grades] General"
            # parts[1] is the course identifier, e.g., "SM101 Scientific Methods (A1)"
            # parts[2] is the grade detail, e.g., "discussion 1: 3.5/5 (was 3.5 / 5)"
            course_identifier = parts[1].strip()
            grade_detail = parts[2].strip()
            # Remove the "(was ...)" part from the grade detail
            cleaned_grade_detail = grade_detail.split("(was")[0].strip()
            courses[course_identifier].append(cleaned_grade_detail)
        else:
            # Fallback for lines not matching the expected format
            courses["Miscellaneous Updates"].append(message)

    output_lines = []
    for course_identifier, items in courses.items():
        output_lines.append(f"{course_identifier}:")
        for item in items:
            output_lines.append(f"  - {item}")
        output_lines.append("")  # Add a blank line between courses

    return "\n".join(output_lines).strip()


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

            # If this is a first-time login attempt with incorrect version
            if first_time:
                logger.warning(
                    f"First-time login with incorrect version for {username}. Credentials will not be saved."
                )

                # Check if credentials were already saved for this user (from a previous attempt)
                # and delete them if they exist
                if user_has_stored_credentials(username):
                    logger.warning(
                        f"Found existing credentials for first-time user {username} with incorrect version. Deleting them."
                    )
                    delete_user_credentials(username)
                    logger.info(f"Deleted credentials for first-time user {username} with incorrect version.")

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
                dev_announcement = get_dev_announcement_cached()
                original_guc_notifications = cached_data.pop("notifications", []) # Get and remove original GUC notifications
                if not isinstance(original_guc_notifications, list):
                    original_guc_notifications = []
                
                # Construct final notifications list in desired order
                final_notifications_list = []
                if dev_announcement: # Ensure dev_announcement is not None
                    # Avoid adding duplicate dev_announcement if it was already in original_guc_notifications
                    if not any(n.get("id") == dev_announcement.get("id") for n in original_guc_notifications):
                        final_notifications_list.append(dev_announcement)
                
                final_notifications_list.extend(original_guc_notifications) # Add original GUC notifications
                
                cached_data["notifications"] = final_notifications_list

            except Exception as e:
                logger.error(f"Failed to add dev announcement or user-specific notifications to cached guc_data: {e}")
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
            if "Authentication failed" in error_msg or "auth" in error_msg.lower() or "login failed" in error_msg.lower() or "credentials" in error_msg.lower() or "password" in error_msg.lower():
                g.log_outcome = "scrape_auth_error"
                status_code = 401
                # Standardize the error message for authentication failures
                error_msg = "Invalid credentials"
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
                original_guc_notifications = scrape_result.pop("notifications", []) # Get and remove original GUC notifications
                if not isinstance(original_guc_notifications, list):
                    original_guc_notifications = []

                # Prepare user-specific notifications (or placeholder)
                # user_specific_notifications_structured = [] # This will hold the single card, or be empty if no card
                # if username in TARGET_NOTIFICATION_USERS:
                #     user_notif_cache_key = f"user_notifications_{username}"
                #     fetched_user_updates_batches = get_from_cache(user_notif_cache_key) or []
                #     logger.info(f"Attempting to fetch user-specific notification batches for {username} from key: {user_notif_cache_key}")
                #     if fetched_user_updates_batches:
                #         logger.info(f"Found {len(fetched_user_updates_batches)} batch(es) of user-specific notifications for {username}.")
                #     else:
                #         logger.info(f"No user-specific notification batches found in cache for {username}.")

                #     if isinstance(fetched_user_updates_batches, list) and fetched_user_updates_batches:
                #         latest_batch = fetched_user_updates_batches[0] # Get the most recent batch
                #         if isinstance(latest_batch, dict) and latest_batch.get("messages") and latest_batch.get("timestamp"):
                #             messages_list = latest_batch["messages"]
                #             # Beautify the messages list
                #             beautified_content = _beautify_grade_updates_body(messages_list)
                #             messages_body = beautified_content
                #             # Append department info if there's actual content
                #             if beautified_content != "No specific updates available.":
                #                 messages_body += "\n\nDepartment: Unisight System"
                #             timestamp_str = latest_batch["timestamp"]
                #             try:
                #                 dt_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
                #                 formatted_date = dt_obj.strftime("%m/%d/%Y")
                #                 formatted_email_time = dt_obj.strftime("%Y-%m-%dT%H:%M:%S")
                #             except ValueError:
                #                 formatted_date = timestamp_str # fallback
                #                 formatted_email_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") # fallback to now if timestamp unparseable

                #             user_specific_notifications_structured.append({
                #                 "id": str(800000), # Dedicated ID for the consolidated user updates card
                #                 "title": "Your Updates",
                #                 "subject": "Your latest update", # Changed subject line
                #                 "body": messages_body,
                #                 "date": formatted_date,
                #                 "email_time": formatted_email_time,
                #                 "staff": "Unisight System",
                #                 "importance": "Normal"
                #             })
                #         else:
                #             logger.warning(f"Latest batch for {username} has unexpected format: {latest_batch}")
                #             # Potentially add placeholder if latest batch is corrupt but others exist?
                #             # For now, if latest is bad, we'll fall through to placeholder if this list remains empty.
                    
                #     # If after trying to process batches, list is still empty, add placeholder
                #     if not user_specific_notifications_structured:
                #         now = datetime.now()
                #         user_specific_notifications_structured.append({
                #             "id": str(777777),
                #             "title": "No New Updates", 
                #             "subject": "No new notifications for you at this time.", 
                #             "body": "Nothing new to see here!\n\nDepartment: Unisight System", # Append department info to placeholder too
                #             "date": now.strftime("%m/%d/%Y"),
                #             "email_time": now.strftime("%Y-%m-%dT%H:%M:%S"),
                #             "staff": "Unisight System",
                #             "importance": "Normal"
                #         })
                
                # Construct final notifications list in desired order
                final_notifications_list = []
                if dev_announcement: # Ensure dev_announcement is not None
                    # Avoid adding duplicate dev_announcement if it was already in original_guc_notifications
                    if not any(n.get("id") == dev_announcement.get("id") for n in original_guc_notifications):
                        final_notifications_list.append(dev_announcement)
                
                # final_notifications_list.extend(user_specific_notifications_structured) # Add user-specific (or placeholder)
                final_notifications_list.extend(original_guc_notifications) # Add original GUC notifications
                
                scrape_result["notifications"] = final_notifications_list

            except Exception as e:
                logger.error(f"Failed to add dev announcement or user-specific notifications to scraped guc_data: {e}")
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

        # Check if the exception message contains authentication failure indicators
        error_msg = str(e).lower()
        if "auth" in error_msg or "login failed" in error_msg or "credentials" in error_msg or "password" in error_msg:
            logger.warning(f"Authentication error detected in exception: {e}")
            return (
                jsonify(
                    {"status": "error", "message": "Invalid credentials"}
                ),
                401,
            )

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
