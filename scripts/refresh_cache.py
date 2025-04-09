# scripts/refresh_cache.py
import os
import sys
import asyncio
import json
import traceback
import logging
from datetime import datetime
from dotenv import load_dotenv
import concurrent.futures
import hashlib  # Added for cache key generation
import pickle

import redis  # Added for pickle caching if used by cms_content

# --- Setup Paths and Load Env ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, ".env"))

# --- Import Config and Utils ---
try:
    from config import config
    from utils.auth import get_all_stored_users_decrypted

    # Use standard JSON cache for most, but need pickle for cms_content potentially
    from utils.cache import set_in_cache as set_json_cache  # Alias for clarity
    from utils.cache import generate_cache_key
    from utils.cache import redis_client  # Need raw client for pickle
    from utils.helpers import normalize_course_url  # For CMS content key
except ImportError as e:
    print(f"Error importing config/utils: {e}.", file=sys.stderr)
    sys.exit(1)

# --- Import Scraping Functions ---
try:
    from scraping import (
        scrape_guc_data,
        scrape_schedule,
        filter_schedule_details,
        scrape_cms_courses,
        scrape_grades,
        scrape_attendance,
        scrape_exam_seats,
        # Import specific CMS content/announcement functions
        scrape_course_content,
        scrape_course_announcements,
    )
except ImportError as e:
    print(f"Error importing scraping functions: {e}.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
log_file = os.path.join(project_root, "refresh_cache.log")
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("refresh_cache_script")


# --- Pickle Caching for CMS Content (Matches refresh_cms_content-2.py) ---
# Use separate functions for pickle cache to avoid conflicts with JSON cache client settings
def set_pickle_cache(
    key: str, value, timeout: int = config.CACHE_DEFAULT_TIMEOUT
):  # Use config.CACHE_EXPIRY from original
    if not redis_client:
        return False  # redis_client uses decode_responses=False
    try:
        pickled_value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        # Key needs to be bytes for this client
        redis_client.setex(key.encode("utf-8"), timeout, pickled_value)
        logger.info(f"Set PICKLE cache for key {key} with expiry {timeout} seconds")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Pickle Cache: Redis connection error on set '{key}': {e}")
    except Exception as e:
        logger.error(f"Pickle Cache: Error setting key {key}: {e}", exc_info=True)
    return False


def generate_cms_content_cache_key(username: str, course_url: str) -> str:
    """Generates the specific cache key used by cms_content (hash based)."""
    # This matches the logic in the old refresh_cms_content-2.py
    normalized_url = normalize_course_url(course_url)
    key_string = f"{username}:{normalized_url}"
    hash_value = hashlib.md5(key_string.encode("utf-8")).hexdigest()
    # Use the specific prefix from the old script
    return f"cms_content:{hash_value}"


# --- Script Constants ---
REFRESH_CONFIG = {
    "guc_data": {
        "func": scrape_guc_data,
        "args": [],
        "cache_prefix": "guc_data",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
    },
    "schedule": {
        "func": scrape_schedule,
        "args": [],
        "cache_prefix": "schedule",
        "timeout": config.CACHE_LONG_TIMEOUT,
        "cache_func": set_json_cache,
    },
    "cms_courses": {
        "func": scrape_cms_courses,
        "args": [],
        "cache_prefix": "cms_courses",
        "timeout": config.CACHE_LONG_TIMEOUT,
        "cache_func": set_json_cache,
    },
    "grades": {
        "func": scrape_grades,
        "args": [],
        "cache_prefix": "grades",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
    },
    "attendance": {
        "func": scrape_attendance,
        "args": [],
        "cache_prefix": "attendance",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
    },
    "exam_seats": {
        "func": scrape_exam_seats,
        "args": [],
        "cache_prefix": "exam_seats",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
    },
    # Add entry for cms_content - function will be a wrapper defined below
    "cms_content": {
        "func": None,
        "args": [],
        "cache_prefix": "cms_content",
        "timeout": config.CACHE_LONG_CMS_CONTENT_TIMEOUT,  # Matches old script
        "cache_func": set_pickle_cache,
    },  # Use pickle cache
}

SECTION_MAP = {
    "1": ["guc_data", "schedule"],
    "2": ["cms_courses", "grades"],
    "3": ["attendance", "exam_seats"],
    "4": ["cms_content"],  # New section for deep CMS refresh
}


# --- Wrapper for Single Course Content Refresh ---
async def _refresh_single_cms_course(username, password, course_entry):
    """Fetches, assembles, and caches data for ONE cms course."""
    course_url = course_entry.get("course_url")
    course_name = course_entry.get("course_name", "Unknown")
    if not course_url:
        logger.error(
            f"Missing course_url in course entry for {username}: {course_entry}"
        )
        return {"status": "skipped", "reason": "missing url"}

    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        logger.error(
            f"URL normalization failed for {username} - {course_name}: {course_url}"
        )
        return {"status": "skipped", "reason": "normalization failed"}

    cache_key = generate_cms_content_cache_key(
        username, normalized_url
    )  # Use specific key format
    logger.debug(
        f"Refreshing CMS content for {username} - {course_name} ({normalized_url})"
    )

    content_list = None
    announcement_result = None
    fetch_success = False
    loop = asyncio.get_running_loop()

    # Run content and announcement fetching concurrently using threads
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=2, thread_name_prefix="CourseFetch"
    ) as pool:
        try:
            content_future = loop.run_in_executor(
                pool, scrape_course_content, username, password, normalized_url
            )
            announcement_future = loop.run_in_executor(
                pool, scrape_course_announcements, username, password, normalized_url
            )

            # Await results
            content_list = await content_future
            if content_list is not None:
                fetch_success = True

            announcement_result = await announcement_future
            if announcement_result is not None:
                fetch_success = True

        except Exception as fetch_exc:
            logger.error(
                f"Exception during concurrent fetch for {normalized_url}: {fetch_exc}",
                exc_info=True,
            )
            fetch_success = False  # Mark as failed

    if not fetch_success:
        logger.warning(
            f"Fetch failed for {username} - {course_name} ({normalized_url}). Cache not updated."
        )
        return {"status": "failed", "reason": "fetch failed"}

    # --- Assemble data WITH Mock Week (logic from refresh_cms_content-2.py) ---
    mock_week = {
        "week_name": "Mock Week",
        "announcement": "",
        "description": "Placeholder",
        "contents": [],
    }
    combined_data_for_cache = []
    course_announcement_dict_to_add = None

    if announcement_result and isinstance(announcement_result, dict):
        html_content = announcement_result.get("announcements_html")
        if html_content and html_content.strip():
            course_announcement_dict_to_add = {"course_announcement": html_content}
            combined_data_for_cache.append(course_announcement_dict_to_add)
        elif "error" in announcement_result:
            logger.warning(
                f"Announcement scraping failed for {normalized_url}: {announcement_result['error']}"
            )

    combined_data_for_cache.append(mock_week)

    if content_list is not None and isinstance(content_list, list):
        combined_data_for_cache.extend(content_list)

    # --- Cache using Pickle ---
    has_announcement = course_announcement_dict_to_add is not None
    has_content_weeks = isinstance(content_list, list) and bool(content_list)

    if has_announcement or has_content_weeks:
        cache_func = REFRESH_CONFIG["cms_content"]["cache_func"]
        timeout = REFRESH_CONFIG["cms_content"]["timeout"]
        if cache_func(cache_key, combined_data_for_cache, timeout=timeout):
            logger.info(
                f"Successfully refreshed CMS content cache for {username} - {course_name}"
            )
            return {"status": "updated"}
        else:
            logger.error(
                f"Failed to set CMS content cache for {username} - {course_name}"
            )
            return {"status": "failed", "reason": "cache set error"}
    else:
        logger.warning(
            f"Skipped CMS content cache update for {username} - {course_name} - no data."
        )
        return {"status": "skipped", "reason": "no data found"}


# --- Refactored Async Task Runner ---
async def run_refresh_for_user(username, password, data_types_to_run):
    """Runs scraping tasks for a user based on requested data types."""
    user_results = {}
    tasks_to_await = []
    data_type_map = {}  # Maps task to its simple data_type name

    logger.info(f"Processing user: {username} for data types: {data_types_to_run}")

    # --- Handle CMS Content Section Separately ---
    if "cms_content" in data_types_to_run:
        logger.info(f"Initiating deep CMS content refresh for user: {username}")
        # 1. Fetch course list synchronously first
        course_list = None
        try:
            # Run sync course list fetch in a thread
            loop = asyncio.get_running_loop()
            course_list = await loop.run_in_executor(
                None, scrape_cms_courses, username, password
            )
        except Exception as e:
            logger.error(
                f"Failed to fetch course list for {username} during cms_content refresh: {e}",
                exc_info=True,
            )
            user_results["cms_content"] = "failed: could not get course list"

        if isinstance(course_list, list) and course_list:
            logger.info(
                f"Found {len(course_list)} courses for {username}. Creating refresh tasks."
            )
            course_tasks = []
            for course_entry in course_list:
                # Create an async task for each course using the wrapper
                task = asyncio.create_task(
                    _refresh_single_cms_course(username, password, course_entry),
                    name=f"{username}_cms_content_{course_entry.get('course_name', 'unknown')}",
                )
                course_tasks.append(task)

            if course_tasks:
                # Await all course tasks for this user
                course_results_list = await asyncio.gather(
                    *course_tasks, return_exceptions=True
                )
                # Summarize course results under a single "cms_content" entry
                success_count = 0
                skipped_count = 0
                failed_count = 0
                for res in course_results_list:
                    if isinstance(res, Exception):
                        failed_count += 1
                        logger.error(
                            f"CMS Content course task failed with exception for {username}: {res}"
                        )
                    elif isinstance(res, dict):
                        status = res.get("status", "unknown")
                        if status == "updated":
                            success_count += 1
                        elif status == "skipped":
                            skipped_count += 1
                        elif status == "failed":
                            failed_count += 1
                summary = f"updated={success_count}, skipped={skipped_count}, failed={failed_count}"
                user_results["cms_content"] = summary
                logger.info(f"CMS Content refresh summary for {username}: {summary}")
            else:
                user_results["cms_content"] = "skipped: no course tasks created"
        elif isinstance(course_list, list):  # Empty list returned
            user_results["cms_content"] = "skipped: no courses found"
        # else: course_list fetch failed, error already logged and result set

        # Remove 'cms_content' from data_types_to_run if it was the only one
        if data_types_to_run == ["cms_content"]:
            return user_results  # Finished if only cms_content was requested
        else:
            # Remove it so the loop below doesn't process it again
            data_types_to_run = [dt for dt in data_types_to_run if dt != "cms_content"]

    # --- Prepare & Run Other Sync Tasks ---
    sync_task_details = []
    for data_type in data_types_to_run:  # Process remaining types
        if data_type in REFRESH_CONFIG:
            config_item = REFRESH_CONFIG[data_type]
            if config_item[
                "func"
            ]:  # Check if function exists (cms_content func is None)
                full_args = [username, password] + config_item["args"]
                sync_task_details.append((config_item["func"], full_args, data_type))
            # else: skip types without a direct function (like cms_content wrapper)
        else:
            logger.warning(
                f"Unknown data type '{data_type}' requested for user {username}. Skipping."
            )
            user_results[data_type] = "skipped: unknown type"

    if sync_task_details:
        loop = asyncio.get_running_loop()
        tasks = []
        for func, args, data_type in sync_task_details:
            coro = asyncio.to_thread(func, *args)
            task = asyncio.create_task(coro, name=f"{username}_{data_type}")
            tasks.append(task)
            data_type_map[task] = data_type

        logger.debug(f"Awaiting {len(tasks)} other threaded tasks for user {username}")
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug(f"Finished awaiting other tasks for user {username}")

        # Process results for these other tasks
        for i, result_or_exc in enumerate(results_list):
            task = tasks[i]
            data_type = data_type_map.get(task, f"unknown_type_{i}")
            cache_prefix = REFRESH_CONFIG[data_type]["cache_prefix"]
            timeout = REFRESH_CONFIG[data_type]["timeout"]
            cache_func = REFRESH_CONFIG[data_type][
                "cache_func"
            ]  # Get correct cache func
            cache_key_identifier = None

            is_failure = False
            error_message = None
            final_data = None

            if isinstance(result_or_exc, Exception):
                is_failure = True
                error_message = f"failed: Task exception - {result_or_exc}"
                logger.error(
                    f"Refresh task {task.get_name()} failed: {result_or_exc}",
                    exc_info=False,
                )
            elif isinstance(result_or_exc, dict) and "error" in result_or_exc:
                is_failure = True
                error_message = f"skipped: {result_or_exc['error']}"
                logger.warning(
                    f"Refresh task {task.get_name()} returned error: {error_message}"
                )
            elif result_or_exc is None:
                is_failure = True
                error_message = "skipped: scraper returned None"
                logger.warning(f"Refresh task {task.get_name()} returned None.")
            else:
                final_data = result_or_exc

            if is_failure:
                user_results[data_type] = error_message
                continue

            if final_data is not None:
                cache_key = generate_cache_key(
                    cache_prefix, username, cache_key_identifier
                )
                data_to_cache = final_data

                if data_type == "schedule":
                    try:
                        filtered = filter_schedule_details(final_data)
                        timings = {
                            "0": "8:15AM-9:45AM",
                            "1": "10:00AM-11:30AM",
                            "2": "11:45AM-1:15PM",
                            "3": "1:45PM-3:15PM",
                            "4": "3:45PM-5:15PM",
                        }
                        data_to_cache = (filtered, timings)
                    except Exception as e_filter:
                        logger.error(
                            f"Failed to filter schedule data for {task.get_name()}: {e_filter}"
                        )
                        user_results[data_type] = "failed: result filtering error"
                        continue

                logger.debug(
                    f"Attempting to cache data for {task.get_name()} under key {cache_key}"
                )
                # Use the correct cache function (JSON for most, Pickle for cms_content if needed)
                set_success = cache_func(cache_key, data_to_cache, timeout=timeout)
                if set_success:
                    user_results[data_type] = "updated"
                    logger.debug(f"Successfully cached data for {task.get_name()}")
                else:
                    user_results[data_type] = "failed: cache set error"
                    logger.error(
                        f"Failed to set cache for {task.get_name()} (key: {cache_key})"
                    )
            else:
                logger.error(
                    f"Internal logic error: Reached cache update but final_data is None for {task.get_name()}"
                )
                user_results[data_type] = "failed: internal logic error"

    return user_results


# --- Main Script Logic ---
async def main():
    """Main async function to drive the cache refresh."""
    start_time = datetime.now()
    logger.info(f"--- Cache Refresh Script Started: {start_time.isoformat()} ---")

    if len(sys.argv) < 2 or sys.argv[1] not in SECTION_MAP:
        print(
            f"Usage: python {sys.argv[0]} <section_number> [username]", file=sys.stderr
        )
        print(f"  section_number: {', '.join(SECTION_MAP.keys())}", file=sys.stderr)
        print(
            "  username (optional): Refresh only for this specific user.",
            file=sys.stderr,
        )
        sys.exit(1)

    section = sys.argv[1]
    target_username = sys.argv[2] if len(sys.argv) > 2 else None
    data_types_to_run = SECTION_MAP[section]

    logger.info("Retrieving user credentials...")
    all_users_decrypted = get_all_stored_users_decrypted()

    if not all_users_decrypted:
        logger.warning("No stored users found. Exiting.")
        return

    users_to_process = {}
    if target_username:
        # ... (handle target_username or all users) ...
        if target_username in all_users_decrypted:
            if all_users_decrypted[target_username] == "DECRYPTION_ERROR":
                logger.error(
                    f"Cannot refresh target user {target_username}: Decryption failed."
                )
                return
            users_to_process = {target_username: all_users_decrypted[target_username]}
            logger.info(f"Targeting refresh for single user: {target_username}")
        else:
            logger.error(
                f"Target user {target_username} not found in stored credentials."
            )
            return
    else:
        users_to_process = {
            u: p for u, p in all_users_decrypted.items() if p != "DECRYPTION_ERROR"
        }
        decryption_failures = len(all_users_decrypted) - len(users_to_process)
        if decryption_failures > 0:
            logger.warning(
                f"Skipping {decryption_failures} users due to password decryption errors."
            )
        logger.info(
            f"Refreshing section {section} ({', '.join(data_types_to_run)}) for {len(users_to_process)} users."
        )

    if not users_to_process:
        logger.warning("No users to process after filtering. Exiting.")
        return

    overall_results = {}
    user_refresh_coroutines = [
        run_refresh_for_user(
            username, password, list(data_types_to_run)
        )  # Pass a copy of the list
        for username, password in users_to_process.items()
    ]

    results_list = await asyncio.gather(
        *user_refresh_coroutines, return_exceptions=True
    )

    # Map results back to usernames
    processed_usernames = list(users_to_process.keys())
    for i, user_result_or_exc in enumerate(results_list):
        username = processed_usernames[i]
        if isinstance(user_result_or_exc, Exception):
            overall_results[username] = {
                "error": f"User task failed: {user_result_or_exc}"
            }
            logger.error(
                f"Refresh process for user {username} failed with exception: {user_result_or_exc}",
                exc_info=True,
            )
        else:
            overall_results[username] = user_result_or_exc

    # --- Log Summary ---
    logger.info("--- Cache Refresh Summary ---")
    total_updated = 0
    total_skipped = 0
    total_failed = 0
    cms_content_stats = {"updated": 0, "skipped": 0, "failed": 0}
    for username, results in overall_results.items():
        user_summary_parts = []
        if results.get("error"):
            user_summary_parts.append(f"USER_ERROR: {results['error']}")
            total_failed += len(data_types_to_run)
        else:
            for data_type in data_types_to_run:
                status = results.get(data_type, "not_run")
                user_summary_parts.append(f"{data_type}: {status}")
                if data_type == "cms_content":
                    # Parse the summary string like "updated=5, skipped=1, failed=0"
                    try:
                        parts = status.split(", ")
                        for part in parts:
                            k, v = part.split("=")
                            if k in cms_content_stats:
                                cms_content_stats[k] += int(v)
                        # Add totals for cms_content based on overall status string if needed
                        # This logic might need refinement based on how errors are reported
                        if "failed=0" in status:
                            total_updated += 1  # Crude count
                        elif "failed" in status:
                            total_failed += 1
                        else:
                            total_skipped += 1

                    except Exception:
                        if status == "updated":
                            cms_content_stats["updated"] += 1
                            total_updated += 1
                        elif status == "skipped":
                            cms_content_stats["skipped"] += 1
                            total_skipped += 1
                        elif status == "failed":
                            cms_content_stats["failed"] += 1
                            total_failed += 1
                        elif status == "not_run":
                            total_failed += 1

                # Count other types
                elif status == "updated":
                    total_updated += 1
                elif "skipped" in status:
                    total_skipped += 1
                elif "failed" in status:
                    total_failed += 1
                elif status == "not_run":
                    total_failed += 1
        logger.info(f"User: {username} -> {'; '.join(user_summary_parts)}")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(
        f"--- Cache Refresh Script Finished: {end_time.isoformat()} (Duration: {duration}) ---"
    )
    if "cms_content" in data_types_to_run:
        logger.info(
            f"CMS Content Courses: Updated={cms_content_stats['updated']}, Skipped={cms_content_stats['skipped']}, Failed={cms_content_stats['failed']}"
        )
    logger.info(
        f"Total Items (excl. CMS content courses): Updated={total_updated}, Skipped={total_skipped}, Failed={total_failed}"
    )


# --- Entry Point ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Refresh script interrupted by user.")
    except Exception as e:
        logger.critical(f"Critical error during script execution: {e}", exc_info=True)
        sys.exit(1)
