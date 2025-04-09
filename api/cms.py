# api/cms.py
import logging
import json
from flask import Blueprint, request, jsonify, g
import concurrent.futures  # To run scraping tasks

from config import config
from scraping.core import create_session
from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from utils.helpers import normalize_course_url

# Import specific scraping functions needed
from scraping.cms import cms_scraper, scrape_cms_courses
from scraping.guc_data import (
    parse_notifications,
)  # Assuming this parses the CMS Home notifications correctly

logger = logging.getLogger(__name__)
cms_bp = Blueprint("cms_bp", __name__)

# --- Constants ---
CMS_COURSES_CACHE_PREFIX = "cms"
CMS_COURSE_DATA_CACHE_PREFIX = (
    "cms_content"  # Matches old key for compatibility if needed
)
CMS_NOTIFICATIONS_CACHE_PREFIX = (
    "cms_notifications"  # Separate cache for homepage notifications
)


# --- Helper to get combined course data (content + announcement) ---
# This replaces the old cms_scraper call from the API endpoint perspective
def get_combined_course_data(
    username: str, password: str, course_url: str
) -> dict | None:
    """
    Orchestrates fetching both content and announcements for a single course.
    Handles caching internally using the 'cms_content' prefix.
    Returns the final combined list structure expected by the frontend/refresh script.
    """
    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        logger.error(f"Invalid course URL for combined data: {course_url}")
        return {"error": "Invalid course URL provided."}

    cache_key = generate_cache_key(
        CMS_COURSE_DATA_CACHE_PREFIX, username, normalized_url
    )

    # 1. Check Cache
    cached_data = get_from_cache(cache_key)
    if cached_data:
        # Basic validation of cached structure (list, first item is announcement dict or mock, second is mock or week)
        if isinstance(cached_data, list) and len(cached_data) > 0:
            # More robust checks can be added here if needed
            logger.info(
                f"Serving combined CMS data from cache for {username} - {normalized_url}"
            )
            return cached_data
        else:
            logger.warning(
                f"Invalid combined CMS data found in cache for {cache_key}. Fetching fresh."
            )

    # 2. Cache Miss -> Fetch Fresh Data Concurrently
    logger.info(
        f"Cache miss for combined CMS data. Fetching fresh for {username} - {normalized_url}"
    )
    content_list = None
    announcement_result = (
        None  # Expected: {'announcements_html': '...'} or {'error': '...'} or None
    )
    fetch_success = False

    # Use the core scraping functions imported from scraping.cms
    from scraping.cms import scrape_course_content, scrape_course_announcements

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
                fetch_success = True  # Success even if list is empty []
        except Exception as e:
            logger.error(f"Course content future error: {e}")
        try:
            announcement_result = announcement_future.result()  # Returns dict or None
            if announcement_result is not None:
                fetch_success = True  # Success even if it's an error dict
        except Exception as e:
            logger.error(f"Course announcement future error: {e}")

    if not fetch_success:
        logger.error(
            f"Both content and announcement fetch failed for course: {normalized_url}"
        )
        # Return error dict, as this is a failure for a specific course request
        return {"error": "Failed to fetch data for the specified course."}

    # 3. Assemble final list for caching and returning
    # Structure: [CourseAnnouncementDict?, MockWeekDict, WeekDict1, WeekDict2, ...]
    mock_week = {  # Define mock week here or import from config/utils
        "week_name": "Mock Week",
        "announcement": "",
        "description": "Placeholder",
        "contents": [],
    }
    combined_data_for_cache = []
    course_announcement_dict_to_add = None

    # Process announcement result
    if announcement_result and isinstance(announcement_result, dict):
        html_content = announcement_result.get("announcements_html")
        if html_content and html_content.strip():
            course_announcement_dict_to_add = {"course_announcement": html_content}
            combined_data_for_cache.append(course_announcement_dict_to_add)
        elif "error" in announcement_result:
            logger.warning(
                f"Announcement scraping failed for {normalized_url}: {announcement_result['error']}"
            )
            # Optionally include the error in the cached structure if needed by frontend
            # combined_data_for_cache.append({"course_announcement_error": announcement_result['error']})
        # else: log empty html or unexpected dict format

    # Add Mock Week
    combined_data_for_cache.append(mock_week)

    # Add Content Weeks (if fetch was successful, even if list is empty)
    if content_list is not None and isinstance(content_list, list):
        combined_data_for_cache.extend(content_list)
    elif content_list is not None:  # Log if content fetch succeeded but wasn't a list
        logger.warning(
            f"scrape_course_content returned unexpected type: {type(content_list)}"
        )

    # 4. Cache the result (only if something beyond mock week was found/fetched)
    if course_announcement_dict_to_add or (
        content_list is not None and isinstance(content_list, list)
    ):
        set_in_cache(
            cache_key, combined_data_for_cache, timeout=config.CACHE_DEFAULT_TIMEOUT
        )  # Use appropriate timeout
        logger.info(f"Cached fresh combined CMS data for {cache_key}")
    else:
        logger.warning(f"Skipping cache set for {cache_key} - only Mock Week resulted.")

    return combined_data_for_cache


# --- API Endpoints ---


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
            # Determine status code based on logged errors if possible, default 502/500
            return (
                jsonify(
                    {"status": "error", "message": "Failed to fetch CMS course list"}
                ),
                502,
            )
        else:
            # Success (courses can be an empty list [])
            g.log_outcome = "scrape_success" if courses else "scrape_success_nodata"
            set_in_cache(
                cache_key, courses, timeout=config.CACHE_LONG_TIMEOUT
            )  # Cache list for a while
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

    # Normalize URL before proceeding
    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        g.log_outcome = "validation_error"
        g.log_error_message = f"Invalid course_url format: {course_url}"
        return jsonify({"status": "error", "message": "Invalid course_url format"}), 400

    try:
        password_to_use = validate_credentials_flow(username, password)

        # Use the helper to get combined data (handles caching)
        # This function returns the final list structure OR an error dict
        result_data = get_combined_course_data(
            username, password_to_use, normalized_url
        )

        if isinstance(result_data, dict) and "error" in result_data:
            error_msg = result_data["error"]
            logger.error(
                f"Failed to get combined CMS data for {username} - {normalized_url}: {error_msg}"
            )
            g.log_outcome = "scrape_error"  # Or specific outcome if available
            g.log_error_message = error_msg
            # Determine status code based on error if possible
            status_code = 500  # Default internal error
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
            # Success - result_data is the list: [Announce?, Mock, Weeks...]
            g.log_outcome = (
                "success"  # Can be cache_hit or scrape_success, helper logs specifics
            )
            logger.info(
                f"Successfully served combined CMS data for {username} - {normalized_url}"
            )
            return jsonify(result_data), 200
        else:
            # Should not happen if helper is correct
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

    # (Consider adding force_refresh param if needed)

    try:
        password_to_use = validate_credentials_flow(username, password)

        # --- Cache Check ---
        cache_key = generate_cache_key(CMS_NOTIFICATIONS_CACHE_PREFIX, username)
        cached_data = get_from_cache(cache_key)
        if cached_data is not None:  # Allow empty list from cache
            logger.info(f"Serving CMS notifications from cache for {username}")
            g.log_outcome = "cache_hit"
            return jsonify(cached_data), 200

        # --- Scrape ---
        logger.info(f"Cache miss for CMS notifications. Scraping for {username}")
        g.log_outcome = "scrape_attempt"

        # Need a specific function for CMS homepage notifications if parse_notifications doesn't work
        # Assuming parse_notifications IS the correct one for CMS homepage based on old cms_data.py
        # If not, need to create/use scrape_cms_homepage_notifications()
        from scraping.core import make_request  # Use core request maker
        from scraping.cms import scrape_cms_courses  # Reuse session creation

        session = create_session(username, password_to_use)
        response = make_request(session, config.CMS_HOME_URL)  # Fetch homepage

        notifications = []
        if response:
            # Assuming parse_notifications from guc_data scraper works on CMS homepage HTML
            # Need to verify this assumption. If not, implement specific parser.
            # For now, let's assume it does for structure.
            notifications = parse_notifications(response.text)  # Reuse parser
            if notifications is None:  # Parser indicated failure
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
            g.log_outcome = "scrape_error"  # Fetch failed
            g.log_error_message = "Failed to fetch CMS homepage for notifications"
            return (
                jsonify({"status": "error", "message": "Failed to fetch CMS homepage"}),
                502,
            )

        # Cache result (even if empty list)
        set_in_cache(
            cache_key, notifications, timeout=config.CACHE_DEFAULT_TIMEOUT
        )  # Shorter timeout maybe?
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


# --- Announcements Endpoint (Legacy/Specific Course) ---
# This might be redundant if /cms_content returns announcements correctly.
# Kept for backward compatibility or specific use cases.
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
    course_url = request.args.get("course_url")  # Specific course URL needed
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

        # Use the specific announcement scraper
        from scraping.cms import scrape_course_announcements

        logger.info(
            f"Fetching announcements for specific course: {username} - {normalized_url}"
        )
        g.log_outcome = "scrape_attempt"

        # Scraper returns {'announcements_html': ...} or {'error': ...} or None
        announcement_result = scrape_course_announcements(
            username, password_to_use, normalized_url
        )

        if announcement_result is None:  # Network or Auth failure during scrape
            g.log_outcome = "scrape_error"  # Or scrape_auth_error if distinguishable
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
            # Return the error from the scraper
            return jsonify(announcement_result), 404  # Or 500 depending on error type
        elif (
            isinstance(announcement_result, dict)
            and "announcements_html" in announcement_result
        ):
            # Success
            g.log_outcome = "scrape_success"
            logger.info(
                f"Successfully scraped announcements for {username} - {normalized_url}"
            )
            # Return the dict containing the HTML
            return jsonify(announcement_result), 200
        else:
            # Unexpected result format
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
