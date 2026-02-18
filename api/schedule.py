import logging
import requests
from flask import Blueprint, request, jsonify, g
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import config
from scraping.authenticate import authenticate_user_session
from utils.auth import AuthError, get_password_for_readonly_session
from utils.cache import get_from_cache, set_in_cache, generate_cache_key
from utils.helpers import get_from_memory_cache, set_in_memory_cache
from scraping.schedule import scrape_schedule, filter_schedule_details
from utils.mock_data import schedule_mockData

from scraping.staff_schedule_scraper import (
    get_global_staff_list_and_tokens,
    _find_staff_id_from_list,
    scrape_staff_schedule_only,
    get_staff_profile_details,
)

logger = logging.getLogger(__name__)
schedule_bp = Blueprint("schedule_bp", __name__)

SCHEDULE_MEMORY_CACHE_TTL = 1800  # 30 Minutes
TIMINGS = {
    "0": "8:30AM-9:40AM",
    "1": "9:45AM-10:55AM",
    "2": "11:00AM-12:10PM",
    "3": "12:20PM-1:30PM",
    "4": "1:35PM-2:45PM",
}

SCHEDULE_SLOT_TIMINGS = {
    0: "8:30AM-9:40AM",
    1: "9:45AM-10:55AM",
    2: "11:00AM-12:10PM",
    3: "12:20PM-1:30PM",
    4: "1:35PM-2:45PM",
    5: "5:30PM-7:00PM",
    6: "7:15PM-8:45PM",
    7: "9:00PM-10:30PM",
}


def _parse_bool_like(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y")
    return bool(value)


def is_schedule_empty(schedule_data: dict) -> bool:
    if not schedule_data or not isinstance(schedule_data, dict):
        return True
    empty_values = {"", "Unknown", "Free", "N/A", "Error", "Parsing Failed"}
    for day, periods in schedule_data.items():
        if not isinstance(periods, dict):
            continue
        for period_name, period_details in periods.items():
            if not isinstance(period_details, dict):
                continue
            course_name = period_details.get("Course_Name", "")
            if course_name not in empty_values:
                return False
    return True


def _format_staff_schedule_for_client(parsed_staff_schedule: dict, timings: dict):
    PERIOD_NAMES = {
        0: "First Period",
        1: "Second Period",
        2: "Third Period",
        3: "Fourth Period",
        4: "Fifth Period",
    }
    DAYS_ORDER = ["Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

    if not parsed_staff_schedule or not isinstance(parsed_staff_schedule, dict):
        return {}

    staff_id = next(iter(parsed_staff_schedule.keys()))
    staff_schedule = parsed_staff_schedule.get(staff_id, {})

    formatted = {}
    for day in DAYS_ORDER:
        per_day = {}
        day_has_content = False
        for idx, period_name in PERIOD_NAMES.items():
            slot_entries = staff_schedule.get(day, {}).get(idx, [])
            if slot_entries:
                entry = slot_entries[0]
                course = (entry.get("group") or "Free").strip()
                location = (entry.get("location") or "Free").strip()
                typ = "Lecture"
                low_course = course.lower()
                if "tutorial" in low_course or "tut" in low_course:
                    typ = "Tut"
                elif "lab" in low_course:
                    typ = "Lab"
                elif course in ("Free", "N/A", "Unknown", ""):
                    typ = "Free"

                if typ != "Free":
                    day_has_content = True

                per_day[period_name] = {"Course_Name": course, "Location": location, "Type": typ}
            else:
                per_day[period_name] = {"Course_Name": "Free", "Location": "Free", "Type": "Free"}

        if day_has_content:
            formatted[day] = per_day

    return formatted


@schedule_bp.route("/schedule", methods=["GET"])
def api_schedule():
    username = request.args.get("username")
    password = request.args.get("password")
    force_refresh = _parse_bool_like(request.args.get("force_refresh"))
    g.username = username

    if not all([username, password]):
        return jsonify({"status": "error", "message": "Missing required parameters: username, password"}), 400

    # optional mock user
    if username == "google.user" and password == "google@3569":
        logger.info(f"Serving mock schedule data for user {username}")
        return jsonify(schedule_mockData), 200

    try:
        password_to_use = get_password_for_readonly_session(username, password)
        cache_key = generate_cache_key("schedule", username)

        # 1) in-memory cache
        in_memory_cache_check_start_time = time.perf_counter()
        cached_data = get_from_memory_cache(cache_key)
        in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
        logger.info(f"TIMING: In-memory Cache check for schedule took {in_memory_cache_check_duration:.2f} ms")

        if cached_data is not None:
            logger.info(f"Serving schedule from IN-MEMORY cache for {username}")
            g.log_outcome = "memory_cache_hit"
            return jsonify(cached_data), 200

        # 2) redis cache
        redis_cache_check_start_time = time.perf_counter()
        cached_data = get_from_cache(cache_key)
        redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
        logger.info(f"TIMING: Redis Cache check for schedule took {redis_cache_check_duration:.2f} ms")

        if cached_data is not None and not force_refresh:
            logger.info(f"Serving schedule from REDIS cache for {username}")
            g.log_outcome = "redis_cache_hit"
            # populate in-memory cache for faster subsequent hits
            set_in_memory_cache(cache_key, cached_data, ttl=SCHEDULE_MEMORY_CACHE_TTL)
            return jsonify(cached_data), 200

        # Cache miss or force refresh -> scrape
        logger.info(f"Cache miss or forced refresh for schedule. Scraping for {username}")
        g.log_outcome = "scrape_attempt"
        scrape_start = time.perf_counter()
        raw_schedule = scrape_schedule(username, password_to_use)
        scrape_duration = (time.perf_counter() - scrape_start) * 1000
        logger.info(f"TIMING: Schedule scrape took {scrape_duration:.2f} ms")

        if not raw_schedule or ("error" in raw_schedule and raw_schedule.get("error")):
            error_msg = raw_schedule.get("error", "Failed to scrape schedule.") if isinstance(raw_schedule, dict) else "Failed to scrape schedule."
            logger.error(f"Schedule scraping failed for {username}: {error_msg}")
            g.log_outcome = "scrape_fail"
            return jsonify({"status": "error", "message": error_msg}), 502

        filtered_data = filter_schedule_details(raw_schedule)
        response_data = [filtered_data, TIMINGS]

        # cache result (redis + in-memory)
        set_in_cache(cache_key, response_data, timeout=config.CACHE_LONG_TIMEOUT)
        set_in_memory_cache(cache_key, response_data, ttl=SCHEDULE_MEMORY_CACHE_TTL)
        logger.info(f"Cached schedule for {username}")

        g.log_outcome = "scrape_success"
        return jsonify(response_data), 200

    except AuthError as e:
        logger.warning(f"AuthError during /schedule for {username}: {e}")
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(f"Unhandled exception in /schedule for {username}: {e}")
        return jsonify({"status": "error", "message": "An internal server error occurred."}), 500


@schedule_bp.route("/staff_list", methods=["GET"])
def api_staff_list():
    username = request.args.get("username")
    password = request.args.get("password")
    force_refresh = _parse_bool_like(request.args.get("force_refresh", False))
    g.username = username

    if not all([username, password]):
        return jsonify({"status": "error", "message": "Missing required parameters: username, password"}), 400

    try:
        actual_pw = get_password_for_readonly_session(username, password)
        session = authenticate_user_session(username, actual_pw)
        if not session:
            return jsonify({"status": "error", "message": "Failed to authenticate with GUC portal"}), 502

        staff_list, _ = get_global_staff_list_and_tokens(session, force_refresh=force_refresh)

        if staff_list is None:
            return jsonify({"status": "error", "message": "Failed to fetch staff list from upstream"}), 502

        staff_dict = {str(item["id"]): item["name"] for item in staff_list if "id" in item and "name" in item}
        return jsonify({"status": "success", "data": staff_dict}), 200

    except AuthError as e:
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(f"Unhandled exception in /staff_list for {username}: {e}")
        return jsonify({"status": "error", "message": "An internal server error occurred."}), 500


@schedule_bp.route("/staff_schedule", methods=["POST"])
def api_staff_schedule():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Missing JSON request body"}), 400

    username = data.get("username")
    password = data.get("password")
    staff_name = data.get("staff_name")
    force_refresh = _parse_bool_like(data.get("force_refresh", False))
    g.username = username

    if not all([username, password, staff_name]):
        return jsonify({"status": "error", "message": "Missing required JSON parameters: username, password, staff_name"}), 400

    normalized_staff_name = staff_name.strip()
    user_cache_key = generate_cache_key("staff_schedule", username, normalized_staff_name)

    # 1) check in-memory cache first
    if not force_refresh:
        in_memory_cache_check_start_time = time.perf_counter()
        cached_data = get_from_memory_cache(user_cache_key)
        in_memory_cache_check_duration = (time.perf_counter() - in_memory_cache_check_start_time) * 1000
        logger.info(f"TIMING: In-memory Cache check for staff schedule took {in_memory_cache_check_duration:.2f} ms")

        if cached_data is not None:
            logger.info(f"Serving staff schedule from IN-MEMORY cache for {username}")
            return jsonify(cached_data), 200

    # 2) check redis cache
    if not force_refresh:
        redis_cache_check_start_time = time.perf_counter()
        cached_data = get_from_cache(user_cache_key)
        redis_cache_check_duration = (time.perf_counter() - redis_cache_check_start_time) * 1000
        logger.info(f"TIMING: Redis Cache check for staff schedule took {redis_cache_check_duration:.2f} ms")

        if cached_data is not None:
            logger.info(f"Serving staff schedule from REDIS cache for {username}")
            set_in_memory_cache(user_cache_key, cached_data, ttl=SCHEDULE_MEMORY_CACHE_TTL)
            return jsonify(cached_data), 200

    try:
        logger.info(f"Cache miss for '{normalized_staff_name}'. Proceeding to scrape.")
        actual_password = get_password_for_readonly_session(username, password)
        session = authenticate_user_session(username, actual_password)
        if not session:
            return jsonify({"status": "error", "message": "Failed to authenticate with GUC portal"}), 502

        session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GUC-API/1.0)",
            "Referer": "https://apps.guc.edu.eg",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

        logger.info("Fetching global staff list and tokens...")
        staff_list, schedule_tokens = get_global_staff_list_and_tokens(session, force_refresh=force_refresh)

        if not staff_list or not schedule_tokens:
            msg = "Failed to retrieve the global staff list or necessary security tokens from the GUC portal."
            logger.error(msg)
            return jsonify({"status": "error", "message": msg}), 502

        staff_id = _find_staff_id_from_list(staff_list, normalized_staff_name)
        if not staff_id:
            msg = f"Could not find a staff member matching the name '{normalized_staff_name}'."
            logger.warning(msg)
            return jsonify({"status": "error", "message": msg}), 404

        staff_details = next((s for s in staff_list if s['id'] == staff_id), None)
        exact_staff_name = staff_details['name'] if staff_details else normalized_staff_name
        logger.info(f"Found Staff ID: {staff_id} with exact name: '{exact_staff_name}'")

        profile_cache_key = generate_cache_key("staff_profile", staff_id)
        # check in-memory before redis for profile
        profile_part = get_from_memory_cache(profile_cache_key)
        if profile_part is None:
            profile_part = get_from_cache(profile_cache_key)

        schedule_part = None
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {}
            futures[ex.submit(scrape_staff_schedule_only, session, staff_id, schedule_tokens)] = "schedule"
            if profile_part is None:
                futures[ex.submit(get_staff_profile_details, session, exact_staff_name, staff_id)] = "profile"

            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    logger.exception("Worker failed for %s: %s", name, e)
                    res = None

                if name == "schedule":
                    schedule_part = res
                elif name == "profile":
                    profile_part = res
                    if isinstance(profile_part, dict) and "error" not in profile_part:
                        set_in_cache(profile_cache_key, profile_part, timeout=60 * 60 * 12)  # 12 hours
                        set_in_memory_cache(profile_cache_key, profile_part, ttl=60 * 60 * 12)

        if schedule_part is None:
            msg = f"Found staff '{exact_staff_name}', but failed to retrieve their schedule data due to a network or server error."
            logger.error(msg)
            return jsonify({"status": "error", "message": msg}), 502

        formatted_schedule = _format_staff_schedule_for_client(schedule_part, SCHEDULE_SLOT_TIMINGS)
        response_payload = [formatted_schedule, SCHEDULE_SLOT_TIMINGS, profile_part]

        # cache the response for this user+staff
        set_in_cache(user_cache_key, response_payload, timeout=config.CACHE_STAFF_SCHEDULE_TIMEOUT)
        set_in_memory_cache(user_cache_key, response_payload, ttl=SCHEDULE_MEMORY_CACHE_TTL)
        logger.info(f"Cached fresh staff schedule and profile for '{exact_staff_name}'.")

        return jsonify(response_payload), 200

    except AuthError as e:
        logger.warning(f"AuthError for {username} during staff schedule request (staff: {normalized_staff_name}): {e.log_message}")
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(f"Unhandled exception during staff schedule request for '{normalized_staff_name}': {e}")
        return jsonify({"status": "error", "message": "An internal server error occurred."}), 500
