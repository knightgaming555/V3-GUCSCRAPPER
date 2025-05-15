# api/cms.py
import logging
import json
from flask import Blueprint, request, jsonify, g
import concurrent.futures  # To run scraping tasks
import hashlib  # Added for hash generation

from config import config
from scraping.core import create_session, make_request
from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from utils.helpers import normalize_course_url

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
CMS_COURSES_CACHE_PREFIX = "cms"
CMS_COURSE_DATA_CACHE_PREFIX = "cms_content"
CMS_NOTIFICATIONS_CACHE_PREFIX = "cms_notifications"


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
    key_string_for_hash = f"{username}:{normalized_url}"
    hash_value = hashlib.md5(key_string_for_hash.encode("utf-8")).hexdigest()
    cache_key = f"{CMS_COURSE_DATA_CACHE_PREFIX}:{hash_value}"

    # 1. Check Cache (only if force_refresh is False)
    if not force_refresh:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            # Basic validation of cached structure
            if isinstance(cached_data, list) and len(cached_data) > 0:
                logger.info(
                    f"Serving combined CMS data from cache for {username} - {normalized_url}"
                )
                return cached_data
            else:
                logger.warning(
                    f"Invalid combined CMS data found in cache for {cache_key}. Fetching fresh."
                )
    else:
        logger.info(
            f"Force refresh requested for combined CMS data for {username} - {normalized_url}"
        )

    # 2. Cache Miss or Forced Refresh -> Fetch Fresh Data Concurrently
    log_prefix = "Cache miss" if not force_refresh else "Forced refresh"
    logger.info(
        f"{log_prefix} for combined CMS data. Fetching fresh for {username} - {normalized_url}"
    )

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

    if not fetch_success:
        logger.error(
            f"Both content and announcement fetch failed for course: {normalized_url}"
        )
        return {"error": "Failed to fetch data for the specified course."}

    # 3. Assemble final list for caching and returning

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

    # 4. Cache the result
    if course_announcement_dict_to_add or (
        content_list is not None and isinstance(content_list, list)
    ):
        set_in_cache(
            cache_key, combined_data_for_cache, timeout=config.CACHE_DEFAULT_TIMEOUT
        )
        logger.info(f"Cached fresh combined CMS data for {cache_key}")
    else:
        logger.warning(f"Skipping cache set for {cache_key} - only Mock Week resulted.")

    return combined_data_for_cache


# --- API Endpoints ---


# /cms_data endpoint remains the same, only fetches course list
@cms_bp.route("/cms_data", methods=["GET"])
def api_cms_courses():
    """Endpoint to fetch the list of courses from CMS homepage."""
    # --- Bot Health Check ---
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
        logger.info(f"Serving mock cms data data for user {username}")
        g.log_outcome = "mock_data_served"
        # Use the imported mock data and jsonify it
        return jsonify(cmsdata_mockData), 200

    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Cache Check ---
        cache_key = generate_cache_key(CMS_COURSES_CACHE_PREFIX, username)
        if not force_refresh:
            cached_data = get_from_cache(cache_key)
            if (
                cached_data is not None
            ):  # Check explicitly for None, allow empty list from cache
                logger.info(f"Serving CMS courses from cache for {username}")
                g.log_outcome = "cache_hit"
                return jsonify(cached_data), 200

        # --- Scrape ---
        logger.info(
            f"Cache miss or forced refresh for CMS courses. Scraping for {username}"
        )
        g.log_outcome = "scrape_attempt"
        courses = scrape_cms_courses(username, password_to_use)

        if courses is None:  # Indicates scraping failure
            g.log_outcome = (
                "scrape_error"  # More specific outcome set by scraper usually
            )
            g.log_error_message = "Failed to scrape CMS course list"  # Generic message
            return (
                jsonify(
                    {"status": "error", "message": "Failed to fetch CMS course list"}
                ),
                502,  # Bad Gateway / Upstream error
            )
        else:
            # Success (courses can be an empty list [])
            g.log_outcome = "scrape_success" if courses else "scrape_success_nodata"
            set_in_cache(cache_key, courses, timeout=config.CACHE_LONG_TIMEOUT)
            logger.info(
                f"Successfully scraped {len(courses)} CMS courses for {username}"
            )
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
            f"Unhandled exception during /api/cms_courses for {username}: {e}"
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
    Endpoint to fetch content AND announcement for a SPECIFIC course.
    Requires 'course_url' query parameter.
    Accepts 'force_refresh=true' to bypass cache.
    Returns the combined list structure: [AnnouncementDict?, MockWeekDict, WeekDict1...]
    """
    # --- Bot Health Check ---
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for CMS Content API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "CMS Content API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    course_url = request.args.get("course_url")
    # --- Add force_refresh parameter ---
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    g.username = username

    if username == "google.user" and password == "google@3569":
        logger.info(f"Handling mock cms content request for user {username}")

        if not course_url:
            # Handle missing course_url for mock user too
            logger.warning("Mock user request missing course_url")
            g.log_outcome = "mock_validation_error"
            g.log_error_message = "Missing course_url parameter for mock user"
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Missing required parameter: course_url",
                    }
                ),
                400,
            )

        # Normalize the requested URL using the *actual* function used elsewhere
        normalized_requested_url = normalize_course_url(course_url)
        if not normalized_requested_url:
            logger.warning(f"Mock user request invalid course_url: {course_url}")
            g.log_outcome = "mock_validation_error"
            g.log_error_message = (
                f"Invalid course_url format for mock user: {course_url}"
            )
            return (
                jsonify({"status": "error", "message": "Invalid course_url format"}),
                400,
            )

        # --- Use the map to find the content ---
        specific_mock_content = mock_content_map.get(normalized_requested_url)

        if specific_mock_content is not None:
            logger.info(
                f"Serving specific mock content for matching course URL: {course_url}"
            )
            g.log_outcome = "mock_data_served_specific"
            # Use the mapped mock data and jsonify it
            return jsonify(specific_mock_content), 200
        else:
            logger.info(
                f"No specific mock content defined for course URL: {course_url}. Returning empty list for mock user."
            )
            g.log_outcome = "mock_data_served_empty"
            # Return an empty list as no specific mock data exists for this URL in the map
            return jsonify([]), 200

    if not course_url:
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing course_url parameter"
        return (
            jsonify(
                {"status": "error", "message": "Missing required parameter: course_url"}
            ),
            400,
        )

    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        g.log_outcome = "validation_error"
        g.log_error_message = f"Invalid course_url format: {course_url}"
        return jsonify({"status": "error", "message": "Invalid course_url format"}), 400

    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Call the helper with force_refresh ---
        result_data = get_combined_course_data(
            username, password_to_use, normalized_url, force_refresh=force_refresh
        )

        # --- Process the result ---
        if isinstance(result_data, dict) and "error" in result_data:
            error_msg = result_data["error"]
            logger.error(
                f"Failed to get combined CMS data for {username} - {normalized_url}: {error_msg}"
            )
            g.log_outcome = "scrape_error"
            g.log_error_message = error_msg
            status_code = 500
            if "Authentication" in error_msg:
                status_code = 401
            elif "fetch" in error_msg:
                status_code = 504
            elif "parse" in error_msg:
                status_code = 502
            elif "not found" in error_msg:
                status_code = 404
            return jsonify({"status": "error", "message": error_msg}), status_code
        elif isinstance(result_data, list):
            g.log_outcome = (
                "success_force_refresh" if force_refresh else "success"
            )  # Differentiate log outcome
            logger.info(
                f"Successfully served combined CMS data for {username} - {normalized_url}{' (forced refresh)' if force_refresh else ''}"
            )
            return jsonify(result_data), 200
        else:
            logger.error(
                f"Unexpected return type from get_combined_course_data: {type(result_data)}"
            )
            g.log_outcome = "internal_error_logic"
            g.log_error_message = "Unexpected data format from CMS helper"
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Internal server error processing CMS data",
                    }
                ),
                500,
            )

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


# /cms_notifications endpoint remains the same
@cms_bp.route("/cms_notifications", methods=["GET"])
def api_cms_notifications():
    """Endpoint to fetch notifications from the CMS homepage."""
    # --- Bot Health Check ---
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for CMS Notifications API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "CMS Notifications API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    g.username = username
    force_refresh = (
        request.args.get("force_refresh", "false").lower() == "true"
    )  # Added force_refresh

    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Cache Check ---
        cache_key = generate_cache_key(CMS_NOTIFICATIONS_CACHE_PREFIX, username)
        if not force_refresh:  # Check flag
            cached_data = get_from_cache(cache_key)
            if cached_data is not None:
                logger.info(f"Serving CMS notifications from cache for {username}")
                g.log_outcome = "cache_hit"
                return jsonify(cached_data), 200

        # --- Scrape ---
        log_prefix = "Cache miss" if not force_refresh else "Forced refresh"
        logger.info(f"{log_prefix} for CMS notifications. Scraping for {username}")
        g.log_outcome = "scrape_attempt"

        session = create_session(username, password_to_use)
        response = make_request(session, config.CMS_HOME_URL)

        notifications = []
        if response:
            notifications = parse_notifications(response.text)
            if notifications is None:
                g.log_outcome = "scrape_parsing_error"
                g.log_error_message = "Failed to parse notifications from CMS homepage"
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Failed to parse CMS notifications",
                        }
                    ),
                    502,
                )
            else:
                g.log_outcome = (
                    "scrape_success" if notifications else "scrape_success_nodata"
                )
        else:
            g.log_outcome = "scrape_error"
            g.log_error_message = "Failed to fetch CMS homepage for notifications"
            return (
                jsonify({"status": "error", "message": "Failed to fetch CMS homepage"}),
                502,
            )

        # Cache result
        set_in_cache(cache_key, notifications, timeout=config.CACHE_DEFAULT_TIMEOUT)
        logger.info(
            f"Successfully scraped {len(notifications)} CMS notifications for {username}"
        )
        return jsonify(notifications), 200

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


# /announcements endpoint remains the same
@cms_bp.route("/announcements", methods=["GET"])
def api_announcements():
    """Fetches announcements for a specific course URL."""
    # --- Bot Health Check ---
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for Announcements API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "Announcements API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    course_url = request.args.get("course_url")
    g.username = username

    if not course_url:
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing course_url parameter"
        return (
            jsonify(
                {"status": "error", "message": "Missing required parameter: course_url"}
            ),
            400,
        )

    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        g.log_outcome = "validation_error"
        g.log_error_message = f"Invalid course_url format: {course_url}"
        return jsonify({"status": "error", "message": "Invalid course_url format"}), 400

    try:
        password_to_use = validate_credentials_flow(username, password)

        # NOTE: Consider if this specific endpoint also needs force_refresh.
        # For now, it doesn't have cache logic, so force_refresh is irrelevant here.
        # If caching were added, force_refresh would be needed here too.

        logger.info(
            f"Fetching announcements for specific course: {username} - {normalized_url}"
        )
        g.log_outcome = "scrape_attempt"

        announcement_result = scrape_course_announcements(
            username, password_to_use, normalized_url
        )

        if announcement_result is None:
            g.log_outcome = "scrape_error"
            g.log_error_message = "Failed to fetch course page for announcements"
            return (
                jsonify({"status": "error", "message": "Failed to fetch course data"}),
                502,
            )
        elif isinstance(announcement_result, dict) and "error" in announcement_result:
            error_msg = announcement_result["error"]
            logger.warning(
                f"Failed to scrape announcements for {username} - {normalized_url}: {error_msg}"
            )
            g.log_outcome = "scrape_fail_no_announce"
            g.log_error_message = error_msg
            status_code = 404 if "not found" in error_msg else 500
            return jsonify(announcement_result), status_code
        elif (
            isinstance(announcement_result, dict)
            and "announcements_html" in announcement_result
        ):
            g.log_outcome = "scrape_success"
            logger.info(
                f"Successfully scraped announcements for {username} - {normalized_url}"
            )
            return jsonify(announcement_result), 200
        else:
            logger.error(
                f"Unexpected result from scrape_course_announcements: {announcement_result}"
            )
            g.log_outcome = "internal_error_logic"
            g.log_error_message = "Unexpected result format from announcement scraper"
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Internal server error processing announcements",
                    }
                ),
                500,
            )

    except AuthError as e:
        logger.warning(
            f"AuthError during announcements request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/announcements for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )
