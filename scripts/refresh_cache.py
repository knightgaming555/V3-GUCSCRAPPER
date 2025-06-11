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
    from utils.cache import get_from_cache  # For getting old data
    from utils.cache import generate_cache_key
    # Import pickle cache functions from utils.cache
    from utils.cache import get_pickle_cache, set_pickle_cache 
    # from utils.cache import redis_client # No longer need raw client here directly if set_pickle_cache in utils handles it
    from utils.helpers import normalize_course_url  # For CMS content key

    # Import the comparison functions
    from utils.notifications_utils import (
        compare_grades,
        compare_attendance,
        compare_guc_data,
    )
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
    from api.schedule import is_schedule_empty
except ImportError as e:
    print(f"Error importing scraping functions: {e}.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
log_file = os.path.join(project_root, "refresh_cache.log")
logging.basicConfig(
    level=config.LOG_LEVEL,  # Ensure this is DEBUG or INFO to see detailed logs
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",  # Added logger name
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("refresh_cache_script")
logging.getLogger("urllib3").setLevel(logging.WARNING)  # Quieten noisy libraries
logging.getLogger("utils.notifications_utils").setLevel(logging.DEBUG) # Ensure debug logs from this module are shown


# --- Pickle Caching for CMS Content (Matches refresh_cms_content-2.py) ---
# Use separate functions for pickle cache to avoid conflicts with JSON cache client settings
# def set_pickle_cache(key: str, value, timeout: int = config.CACHE_DEFAULT_TIMEOUT):
#     raw_redis_client = None
#     try:
#         # Use from_url to correctly parse REDIS_URL
#         raw_redis_client = redis.Redis.from_url(
#             config.REDIS_URL, 
#             db=config.REDIS_DB if hasattr(config, 'REDIS_DB') else 0, # from_url might handle db in URL, but explicit is safer if REDIS_DB is ever defined separately
#             decode_responses=False # Important: Do not decode for pickle
#         )
#         pickled_value = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
#         raw_redis_client.setex(key.encode("utf-8"), timeout, pickled_value)
#         logger.info(f"Set PICKLE cache for key {key} with expiry {timeout} seconds")
#         return True
#     except redis.exceptions.ConnectionError as e:
#         logger.error(f"Pickle Cache: Redis connection error on set '{key}': {e}")
#     except Exception as e:
#         logger.error(f"Pickle Cache: Error setting key {key}: {e}", exc_info=True)
#     finally:
#         # Ensure connection is closed if it was opened here
#         # Check if raw_redis_client was initialized before trying to close
#         if raw_redis_client:
#             try:
#                 raw_redis_client.close()
#             except Exception:
#                 pass  # Ignore errors during close
#     return False


# def generate_cms_content_cache_key(course_url: str) -> str: # Keep this local as it's specific to cms_content structure
#     """Generates a global cache key for cms_content based on the course URL."""
#     normalized_url = normalize_course_url(course_url)
#     if not normalized_url:
#         return None
#     # Key no longer includes username, only the normalized URL's hash.
#     hash_value = hashlib.md5(normalized_url.encode("utf-8")).hexdigest()
#     return f"cms_content:{hash_value}"

# Keep generate_cms_content_cache_key local or move to utils if it becomes more general.
# For now, it seems specific enough to stay if it encapsulates the "cms_content:{hash}" structure.
# Let's assume it should stay local for now to minimize changes not directly requested.
# The request was about set_pickle_cache consistency.

# The `generate_cms_content_cache_key` uses `normalize_course_url` and `hashlib.md5`
# and prepends "cms_content:". This logic is specific to how CMS content keys are formed.
# It's not a general cache key generation utility. So, it makes sense to keep it here
# or move it to a more specific cms_utils.py if that existed.
# The `utils.cache.generate_cache_key` is a more general username-based key generator.

# Retaining the local `generate_cms_content_cache_key` function:
def generate_cms_content_cache_key(course_url: str) -> str:
    """Generates a global cache key for cms_content based on the course URL."""
    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        return None
    hash_value = hashlib.md5(normalized_url.encode("utf-8")).hexdigest()
    return f"cms_content:{hash_value}"


def _is_cms_content_substantial(content_data: list) -> bool:
    """
    Evaluates if CMS content data is substantial (has real content weeks with materials).

    Args:
        content_data: List containing announcements, mock weeks, and actual content weeks

    Returns:
        bool: True if content has substantial material beyond just announcements/mock data
    """
    if not content_data or not isinstance(content_data, list):
        return False

    # Count actual content weeks (not announcements or mock weeks)
    actual_content_weeks = 0
    total_materials = 0

    for item in content_data:
        if isinstance(item, dict):
            # Skip announcements
            if "course_announcement" in item:
                continue

            # Check if this is a week with actual content
            # The actual CMS data structure uses "week_name" and "contents" keys
            if "week_name" in item and "contents" in item:
                week_contents = item.get("contents", [])
                if isinstance(week_contents, list) and len(week_contents) > 0:
                    # Check if it's not just a mock week
                    week_name = item.get("week_name", "").lower()
                    if "mock" not in week_name and "placeholder" not in week_name:
                        actual_content_weeks += 1
                        total_materials += len(week_contents)

    # Consider content substantial if it has at least 1 real week with materials
    # or at least 3 total materials across weeks
    return actual_content_weeks >= 1 or total_materials >= 3


# --- Script Constants ---
REFRESH_CONFIG = {
    "guc_data": {
        "func": scrape_guc_data,
        "args": [],
        "cache_prefix": "guc_data",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": compare_guc_data,  # Added compare function
    },
    "schedule": {
        "func": scrape_schedule,
        "args": [],
        "cache_prefix": "schedule",
        "timeout": config.CACHE_LONG_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": None,  # No comparison needed
    },
    "cms_courses": {
        "func": scrape_cms_courses,
        "args": [],
        "cache_prefix": "cms_courses",
        "timeout": config.CACHE_LONG_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": None,  # No comparison needed
    },
    "grades": {
        "func": scrape_grades,
        "args": [],
        "cache_prefix": "grades",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": compare_grades,  # Added compare function
    },
    "attendance": {
        "func": scrape_attendance,
        "args": [],
        "cache_prefix": "attendance",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": compare_attendance,  # Added compare function
    },
    "exam_seats": {
        "func": scrape_exam_seats,
        "args": [],
        "cache_prefix": "exam_seats",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": None,  # No comparison needed
    },
    # Add entry for cms_content - function will be a wrapper defined below
    "cms_content": {
        "func": None,  # Wrapper handles this
        "args": [],
        "cache_prefix": "cms_content",  # Prefix used for key generation
        "timeout": 3600, # 1 hour timeout as requested
        "cache_func": set_pickle_cache,
        "compare_func": None,  # Comparison handled differently or not implemented
    },
}

TARGET_NOTIFICATION_USERS = ["mohamed.elsaadi", "seif.elkady"] # Added for specific user notifications
MAX_NOTIFICATIONS_LIMIT = 5 # Max notifications to keep per user
RETRY_DELAY_SECONDS = 5 # Delay between retries for CMS content fetching

SECTION_MAP = {
    "1": ["guc_data", "schedule"],
    "2": ["cms_courses", "grades"],
    "3": ["attendance", "exam_seats"],
    "4": ["cms_content"],
}


# --- Wrapper for Single Course Content Refresh ---
async def _refresh_single_cms_course(username_for_creds, password_for_creds, course_entry):
    """
    Fetches, assembles, and caches data for ONE cms course using pickle.
    Implements retry logic. Does NOT check if already refreshed this run (caller handles that).
    """
    course_url = course_entry.get("course_url")
    course_name = course_entry.get("course_name", "Unknown")

    if not course_url:
        logger.error(
            f"Missing course_url in course entry for {username_for_creds} (used for creds): {course_entry}"
        )
        return {"status": "skipped", "reason": "missing url"}

    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        logger.error(
            f"URL normalization failed for {username_for_creds} (used for creds) - {course_name}: {course_url}"
        )
        return {"status": "skipped", "reason": "normalization failed"}

    # Use the global hash-based key generator for cms_content
    cache_key = generate_cms_content_cache_key(normalized_url) # Pass normalized_url
    if not cache_key:
        logger.error(f"Could not generate GLOBAL cache key for {course_name} (URL: {normalized_url})")
        return {"status": "failed", "reason": "global key generation error"}

    logger.debug(
        f"Refreshing CMS content for course {course_name} ({normalized_url}) using creds of {username_for_creds}"
    )

    content_list = None
    announcement_result = None
    fetch_success = False
    loop = asyncio.get_running_loop()
    
    max_retries = 3
    for attempt in range(max_retries):
        logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch CMS content for {course_name} ({normalized_url})")
        current_attempt_fetch_success = False
        # Run content and announcement fetching concurrently using threads
        max_workers_cms = min(4, os.cpu_count() or 1)
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers_cms, thread_name_prefix="CourseFetchAttempt"
        ) as pool:
            try:
                content_future = loop.run_in_executor(
                    pool, scrape_course_content, username_for_creds, password_for_creds, normalized_url
                )
                announcement_future = loop.run_in_executor(
                    pool, scrape_course_announcements, username_for_creds, password_for_creds, normalized_url
                )

                content_list = await content_future
                announcement_result = await announcement_future

                if content_list is not None or announcement_result is not None:
                    current_attempt_fetch_success = True
                    fetch_success = True # Mark overall success
                    if content_list is None:
                        logger.warning(
                            f"CMS Content fetch (Attempt {attempt+1}) returned None for {course_name}"
                        )
                    if announcement_result is None:
                        logger.warning(
                            f"CMS Announcement fetch (Attempt {attempt+1}) returned None for {course_name}"
                        )
                    break # Successful attempt, exit retry loop
                else: # Both are None from this attempt
                    logger.warning(f"Attempt {attempt+1}: Both content and announcement fetch returned None for {course_name}.")

            except Exception as fetch_exc:
                logger.error(
                    f"Exception during concurrent fetch (Attempt {attempt + 1}) for {course_name} ({normalized_url}): {fetch_exc}",
                    exc_info=True if attempt == max_retries -1 else False, # Full traceback on last attempt
                )
        
        if fetch_success: # If any attempt was successful
            break 

        if attempt < max_retries - 1:
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries} failed for {course_name}. Retrying in {RETRY_DELAY_SECONDS}s..."
            )
            await asyncio.sleep(RETRY_DELAY_SECONDS)
        else:
            logger.error(
                f"All {max_retries} attempts failed to fetch CMS content for {course_name} ({normalized_url})."
            )
            # fetch_success remains False

    if not fetch_success:
        logger.warning(
            f"All fetch attempts failed for {course_name}. Cache not updated."
        )
        return {"status": "failed_fetch_retries", "reason": "fetch failed after retries"}

    # --- Assemble data (logic from refresh_cms_content-2.py) ---
    combined_data_for_cache = []
    course_announcement_dict_to_add = None

    # Handle announcements first
    if announcement_result and isinstance(announcement_result, dict):
        html_content = announcement_result.get("announcements_html")
        if html_content and html_content.strip():
            # Store only the HTML content directly under the key? Original script put it in a dict.
            # Let's match the original structure for consistency:
            course_announcement_dict_to_add = {"course_announcement": html_content}
            combined_data_for_cache.append(course_announcement_dict_to_add)
        elif "error" in announcement_result:
            logger.warning(
                f"Announcement scraping reported error for {normalized_url}: {announcement_result['error']}"
            )
        # If None or empty, just don't add it.

    # Add mock week if needed (or if content list is empty?) - matching original
    # Original logic seemed to always add it? Check if this is desired.
    # Let's add it only if there's content or announcements, maybe?
    # Replicating original: add it regardless if fetch succeeded overall.

    # Add actual content weeks
    if content_list is not None and isinstance(content_list, list):
        combined_data_for_cache.extend(content_list)
    elif content_list is None:
        # Already logged warning above if announcements succeeded but content failed
        pass
    elif isinstance(content_list, dict) and "error" in content_list:
        logger.warning(
            f"Content scraping reported error for {normalized_url}: {content_list['error']}"
        )

    # --- Cache using Pickle with Fallback Logic ---
    # Check if newly fetched data is substantial
    new_data_is_substantial = _is_cms_content_substantial(combined_data_for_cache)

    if combined_data_for_cache:
        cache_func = REFRESH_CONFIG["cms_content"]["cache_func"]
        timeout = REFRESH_CONFIG["cms_content"]["timeout"] # This is now 1 hour

        # If new data is not substantial, check if we should preserve existing cache
        if not new_data_is_substantial:
            logger.warning(
                f"Newly fetched CMS content for {course_name} is not substantial (empty/minimal content)"
            )

            # Try to get existing cached data
            existing_cached_data = get_pickle_cache(cache_key)
            existing_data_is_substantial = _is_cms_content_substantial(existing_cached_data)

            if existing_cached_data and existing_data_is_substantial:
                logger.info(
                    f"Preserving existing substantial CMS content cache for {course_name} "
                    f"instead of overwriting with insufficient new data (Key: {cache_key})"
                )
                # Extend the timeout of existing cache to keep it fresh
                if cache_func(cache_key, existing_cached_data, timeout=timeout):
                    logger.info(
                        f"Successfully preserved and refreshed existing CMS content cache for {course_name}"
                    )
                    return {"status": "preserved_existing", "refreshed_url": normalized_url}
                else:
                    logger.error(
                        f"Failed to refresh existing CMS content cache timeout for {course_name}"
                    )
                    # Fall through to cache new data anyway
            else:
                logger.info(
                    f"No existing substantial cache found for {course_name}, will cache new data even if minimal"
                )

        # Cache the new data (either it's substantial, or no good existing cache exists)
        if cache_func(cache_key, combined_data_for_cache, timeout=timeout):
            status_msg = "updated" if new_data_is_substantial else "updated_minimal"
            logger.info(
                f"Successfully {'refreshed' if new_data_is_substantial else 'cached minimal'} "
                f"GLOBAL CMS content cache for {course_name} (Key: {cache_key})"
            )
            return {"status": status_msg, "refreshed_url": normalized_url}
        else:
            logger.error(
                f"Failed to set GLOBAL CMS content pickle cache for {course_name} (Key: {cache_key})"
            )
            return {"status": "failed_cache_set", "reason": "cache set error"}
    else:
        # This case should ideally not happen if fetch_success is true, but as a safeguard:
        logger.warning(
            f"Skipped GLOBAL CMS content cache update for {course_name} - no data assembled."
        )
        return {"status": "skipped", "reason": "no data assembled"}


# --- Refactored Async Task Runner ---
async def run_refresh_for_user(username, password, data_types_to_run, refreshed_course_urls_this_run: set):
    """Runs scraping tasks for a user based on requested data types, handling global CMS refresh."""
    user_results = {}
    max_concurrent_fetches = config.MAX_CONCURRENT_FETCHES  # Use config
    current_run_all_update_messages = [] # Initialize list to collect all updates for this user in this run

    logger.info(f"Processing user: {username} for data types: {data_types_to_run}")

    # --- Handle CMS Content Section Separately (Section 4) ---
    if "cms_content" in data_types_to_run:
        logger.info(f"Processing CMS content section for user: {username}")
        course_list = None
        try:
            loop = asyncio.get_running_loop()
            course_list = await loop.run_in_executor(
                None, scrape_cms_courses, username, password
            )
        except Exception as e:
            logger.error(
                f"Failed to fetch course list for {username} during cms_content processing: {e}",
                exc_info=True,
            )
            user_results["cms_content"] = "failed: could not get course list for this user"
            course_list = None

        if isinstance(course_list, list) and course_list:
            logger.info(
                f"User {username} has {len(course_list)} courses. Checking against globally refreshed list."
            )
            course_tasks = []
            semaphore = asyncio.Semaphore(max_concurrent_fetches)

            cms_success_count_for_user = 0
            cms_skipped_this_run_count_for_user = 0
            cms_failed_count_for_user = 0
            cms_skipped_fetch_issues_for_user = 0 # Tracks skips due to URL issues, etc.

            async def fetch_with_semaphore(coro):
                async with semaphore:
                    return await coro

            for course_entry in course_list:
                course_url = course_entry.get("course_url")
                course_name = course_entry.get("course_name", "unknown")
                
                if not course_url:
                    logger.warning(f"Skipping course with no URL for user {username}: {course_entry}")
                    cms_skipped_fetch_issues_for_user += 1
                    continue

                normalized_course_url = normalize_course_url(course_url)
                if not normalized_course_url:
                    logger.warning(f"Skipping course with unnormalizable URL '{course_url}' for user {username}")
                    cms_skipped_fetch_issues_for_user +=1
                    continue

                if normalized_course_url in refreshed_course_urls_this_run:
                    logger.info(f"CMS Content for course '{course_name}' ({normalized_course_url}) already refreshed this run. Skipping for user {username}.")
                    cms_skipped_this_run_count_for_user += 1
                    continue

                # If not skipped, create task to refresh it globally
                logger.info(f"Course '{course_name}' ({normalized_course_url}) for user {username} needs global refresh.")
                task_coro = _refresh_single_cms_course(username, password, course_entry)
                task = asyncio.create_task(
                    fetch_with_semaphore(task_coro),
                    name=f"GLOBAL_CMS_{normalized_course_url.split('/')[-1]}", # More generic task name
                )
                course_tasks.append(task)

            if course_tasks: # Only if there are tasks to run for global refresh initiated by this user
                logger.info(f"Awaiting {len(course_tasks)} global CMS refresh tasks initiated by {username}.")
                course_results_list = await asyncio.gather(
                    *course_tasks, return_exceptions=True
                )
                
                for res in course_results_list:
                    if isinstance(res, Exception):
                        cms_failed_count_for_user += 1
                        logger.error(
                            f"A global CMS Content course task (initiated by {username}) failed with exception: {res}"
                        )
                    elif isinstance(res, dict):
                        status = res.get("status", "unknown")
                        refreshed_url_val = res.get("refreshed_url")

                        if status in ["updated", "updated_minimal", "preserved_existing"] and refreshed_url_val:
                            cms_success_count_for_user += 1
                            refreshed_course_urls_this_run.add(refreshed_url_val) # Add to global set
                            if status == "preserved_existing":
                                logger.info(f"Global cache preservation for {refreshed_url_val} (initiated by {username}) successful.")
                            elif status == "updated_minimal":
                                logger.info(f"Global minimal refresh for {refreshed_url_val} (initiated by {username}) successful.")
                            else:
                                logger.info(f"Global refresh for {refreshed_url_val} (initiated by {username}) successful.")
                        elif status == "skipped": # Skipped by _refresh_single_cms_course (e.g. missing URL in entry)
                            cms_skipped_fetch_issues_for_user += 1
                        elif status in ["failed_fetch_retries", "failed_cache_set", "failed"]:
                            cms_failed_count_for_user += 1
                        else:
                            cms_failed_count_for_user += 1
                            logger.warning(
                                f"Global CMS Content course task (initiated by {username}) returned unknown status: {status}"
                            )
                    else:
                        cms_failed_count_for_user += 1
                        logger.error(
                            f"Global CMS Content course task (initiated by {username}) returned unexpected type: {type(res)}"
                        )
            
            summary = (
                f"Processed for user {username}: "
                f"Globally Updated Now={cms_success_count_for_user}, "
                f"Skipped (already updated this run)={cms_skipped_this_run_count_for_user}, "
                f"Global Fails Now={cms_failed_count_for_user}, "
                f"Skipped (fetch/URL issues)={cms_skipped_fetch_issues_for_user}"
            )
            user_results["cms_content"] = summary
            logger.info(f"CMS Content processing summary for user {username}: {summary}")

        elif isinstance(course_list, dict) and "error" in course_list:
            error_msg = course_list["error"]
            user_results["cms_content"] = f"failed to list courses for user: {error_msg}"
            logger.warning(f"Fetching course list failed for user {username}: {error_msg}")
        elif isinstance(course_list, list) and not course_list:
            user_results["cms_content"] = "skipped: user has no courses listed"
        elif course_list is None and "cms_content" not in user_results:
            pass
        else:
            user_results["cms_content"] = "failed: unexpected course list format for user"
            logger.error(
                f"Unexpected course list format for user {username}: {type(course_list)}"
            )

        # Remove 'cms_content' from data_types_to_run if processed
        data_types_to_run = [dt for dt in data_types_to_run if dt != "cms_content"]
        # If only cms_content was requested, return now
        if not data_types_to_run:
            return user_results

    # --- Prepare & Run Other Sync Tasks Concurrently (Sections 1, 2, 3) ---
    other_tasks = []
    data_type_map = {}  # Maps task back to data_type string
    semaphore = asyncio.Semaphore(
        max_concurrent_fetches
    )  # Limit overall concurrent scrapes

    async def run_scrape_with_semaphore(func, args, data_type):
        async with semaphore:
            logger.debug(f"Starting scrape task for {username} - {data_type}")
            loop = asyncio.get_running_loop()
            # Use asyncio.to_thread for truly blocking IO-bound sync functions
            result = await loop.run_in_executor(None, func, *args)
            logger.debug(f"Finished scrape task for {username} - {data_type}")
            return result

    for data_type in data_types_to_run:
        if data_type in REFRESH_CONFIG:
            config_item = REFRESH_CONFIG[data_type]
            if config_item.get("func"):  # Check if function exists
                full_args = [username, password] + config_item["args"]
                # Create task using the semaphore wrapper
                task = asyncio.create_task(
                    run_scrape_with_semaphore(
                        config_item["func"], full_args, data_type
                    ),
                    name=f"{username}_{data_type}",
                )
                other_tasks.append(task)
                data_type_map[task] = data_type
            else:
                logger.warning(
                    f"No function defined for data type '{data_type}' for user {username}. Skipping."
                )
                user_results[data_type] = "skipped: no function defined"
        else:
            logger.warning(
                f"Unknown data type '{data_type}' requested for user {username}. Skipping."
            )
            user_results[data_type] = "skipped: unknown type"

    if other_tasks:
        logger.debug(f"Awaiting {len(other_tasks)} other tasks for user {username}")
        results_list = await asyncio.gather(*other_tasks, return_exceptions=True)
        logger.debug(f"Finished awaiting other tasks for user {username}")

        # Process results for these other tasks
        for i, result_or_exc in enumerate(results_list):
            task = other_tasks[i]
            task_name = task.get_name()
            data_type = data_type_map.get(
                task, f"unknown_type_{i}"
            )  # Get data_type string

            # Ensure we have config for this data_type before proceeding
            if data_type not in REFRESH_CONFIG:
                logger.error(
                    f"Internal Error: Task result received for unknown data_type '{data_type}' ({task_name})"
                )
                user_results[data_type] = "failed: internal config error"
                continue

            config_item = REFRESH_CONFIG[data_type]
            cache_prefix = config_item["cache_prefix"]
            timeout = config_item["timeout"]
            cache_func = config_item["cache_func"]
            compare_func = config_item.get("compare_func")  # Get compare func if exists

            is_failure = False
            error_message = None
            final_data = None

            if isinstance(result_or_exc, Exception):
                is_failure = True
                # Extract specific error message if possible
                error_detail = str(result_or_exc)
                error_message = f"failed: Task exception - {error_detail}"
                logger.error(
                    f"Refresh task {task_name} failed: {result_or_exc}",
                    exc_info=False,  # Keep log concise, set True for deep debug
                )
                # Optionally log traceback separately if needed without making message huge
                # logger.debug(f"Traceback for {task_name} exception:", exc_info=True)

            elif isinstance(result_or_exc, dict) and "error" in result_or_exc:
                is_failure = True
                error_message = f"skipped: {result_or_exc['error']}"  # Scraper reported controlled error
                logger.warning(
                    f"Refresh task {task_name} returned error: {error_message}"
                )
            elif result_or_exc is None:
                # Treat None as skippable, potentially normal (e.g., no data found)
                # Or treat as failure depending on scraper contract
                is_failure = True  # Let's treat unexpected None as failure unless scraper explicitly allows it
                error_message = "skipped: scraper returned None"
                logger.warning(f"Refresh task {task_name} returned None.")
            else:
                # Successful scrape
                final_data = result_or_exc

            if is_failure:
                user_results[data_type] = error_message
                continue  # Skip caching and comparison for failed tasks

            # --- Process successful scrape result ---
            if final_data is not None:
                cache_key = generate_cache_key(
                    cache_prefix, username
                )  # Simple key for these types
                data_to_cache = final_data

                # --- Data Transformation (e.g., Schedule Filtering) ---
                if data_type == "schedule":
                    try:
                        filtered = filter_schedule_details(final_data)

                        # Check if the schedule is empty (no meaningful course data)
                        if is_schedule_empty(filtered):
                            logger.info(f"Schedule for {username} contains no meaningful course data, caching empty array")
                            data_to_cache = []  # Store empty array instead of tuple
                        else:
                            # Define timings directly here or import from config/constants
                            timings = {
                                "0": "8:15AM-9:45AM",
                                "1": "10:00AM-11:30AM",
                                "2": "11:45AM-1:15PM",
                                "3": "1:45PM-3:15PM",
                                "4": "3:45PM-5:15PM",
                            }
                            data_to_cache = (filtered, timings)  # Store as tuple
                    except Exception as e_filter:
                        logger.error(
                            f"Failed to filter schedule data for {task_name}: {e_filter}"
                        )
                        user_results[data_type] = "failed: result filtering error"
                        continue  # Skip caching bad data

                # --- Comparison and Notification Generation ---
                changes_detected_by_compare_func = []

                if compare_func:  # Check if a comparison function is defined
                    try:
                        # Get old data from cache for comparison
                        old_data = get_from_cache(cache_key)

                        if old_data:
                            # Call compare_func. It will perform its original duties
                            # (which might include calling its own add_notification for a different system)
                            # and return a list of [type, description] for items it considered new THIS RUN.
                            logger.debug(
                                f"Calling compare_func for {data_type} for {username}"
                            )
                            changes_detected_by_compare_func = compare_func(
                                username, old_data, data_to_cache
                            )

                            if changes_detected_by_compare_func:
                                logger.info(
                                    f"Compare function for {data_type} found {len(changes_detected_by_compare_func)} changes for {username}."
                                )
                                logger.debug(
                                    f"Changes from compare_func for {username} ({data_type}): {changes_detected_by_compare_func}"
                                )

                                # --- New User-Specific Persistent Notification Logic ---
                                if username in TARGET_NOTIFICATION_USERS:
                                    # Collect all formatted messages for this data_type
                                    for change_item in changes_detected_by_compare_func:
                                        if isinstance(change_item, list) and len(change_item) == 2:
                                            category = data_type.capitalize()
                                            description = change_item[1]
                                            formatted_msg = f"[{category}] {description}"
                                            current_run_all_update_messages.append(formatted_msg)
                                        else:
                                            logger.warning(f"Unexpected format for change_item from compare_func for {username} ({data_type}): {change_item}")
                                    # Batch caching will happen after all data_types for this user are processed

                            # The original logging related to config.NOTIFICATION_ENABLED_USERS is largely handled
                            # within the compare_xxx functions themselves if they call add_notification.
                            # The previous logging block here for `notifications_generated` is thus redundant
                            # if compare_func itself logs its actions.

                        else: # if not old_data
                            logger.debug(
                                f"Skipping {data_type} comparison for {username} (no old data found in cache key {cache_key})"
                            )

                    except Exception as e_comp:
                        logger.error(
                            f"Error during {data_type} comparison or new notification processing for {username}: {e_comp}",
                            exc_info=True,
                        )
                        # Decide whether to proceed with caching despite comparison error
                        # Caching of main data_type might be desired even if notifications failed

                # --- Caching ---
                logger.debug(
                    f"Attempting to cache {data_type} for {task_name} under key {cache_key}"
                )
                set_success = cache_func(cache_key, data_to_cache, timeout=timeout)

                if set_success:
                    user_results[data_type] = "updated"
                    logger.debug(f"Successfully cached {data_type} for {task_name}")
                else:
                    user_results[data_type] = "failed: cache set error"
                    logger.error(
                        f"Failed to set {data_type} cache for {task_name} (key: {cache_key})"
                    )

            else:
                # This path should not be reached if is_failure is False, but as a safeguard:
                logger.error(
                    f"Internal logic error: Reached cache step but final_data is None for {task_name}"
                )
                user_results[data_type] = (
                    "failed: internal logic error (final_data None)"
                )

    # --- After processing all data_types for the user, handle consolidated notification caching ---
    if username in TARGET_NOTIFICATION_USERS:
        if current_run_all_update_messages:
            logger.info(f"Collected {len(current_run_all_update_messages)} new user-specific update message(s) for {username} in this run. Preparing batch.")
            user_notif_cache_key = f"user_notifications_{username}"
            new_batch_entry = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "messages": current_run_all_update_messages
            }
            
            existing_user_updates = get_from_cache(user_notif_cache_key) or []
            if not isinstance(existing_user_updates, list):
                logger.warning(f"Corrupted user notifications cache for {user_notif_cache_key} (type: {type(existing_user_updates)}). Resetting to empty list.")
                existing_user_updates = []

            updated_user_updates = [new_batch_entry] + existing_user_updates
            updated_user_updates = updated_user_updates[:MAX_NOTIFICATIONS_LIMIT] # Limit to N batches
            
            VERY_LONG_TIMEOUT = 365 * 24 * 60 * 60  # 1 year
            if set_json_cache(user_notif_cache_key, updated_user_updates, timeout=VERY_LONG_TIMEOUT):
                logger.info(f"Successfully cached consolidated batch of {len(current_run_all_update_messages)} updates for {username}. Total batches: {len(updated_user_updates)}.")
            else:
                logger.error(f"Failed to cache consolidated batch of updates for {username} at key {user_notif_cache_key}")
        else:
            logger.info(f"No new user-specific update messages collected for {username} in this run. No batch to cache.")

    return user_results


# --- Main Script Logic ---
async def main():
    """Main async function to drive the cache refresh."""
    start_time = datetime.now()
    logger.info(f"--- Cache Refresh Script Started: {start_time.isoformat()} ---")

    # --- Argument Parsing ---
    if len(sys.argv) < 2 or sys.argv[1] not in SECTION_MAP:
        valid_sections = ", ".join(SECTION_MAP.keys())
        print(
            f"Usage: python {sys.argv[0]} <section_number> [username]", file=sys.stderr
        )
        print(f"  section_number: {valid_sections}", file=sys.stderr)
        print(
            "  username (optional): Refresh only for this specific user.",
            file=sys.stderr,
        )
        sys.exit(1)

    section = sys.argv[1]
    target_username = sys.argv[2] if len(sys.argv) > 2 else None
    data_types_to_run = SECTION_MAP[section]

    # --- User Credential Retrieval ---
    logger.info("Retrieving user credentials...")
    all_users_decrypted = get_all_stored_users_decrypted()

    if not all_users_decrypted:
        logger.warning("No stored users found. Exiting.")
        return

    users_to_process = {}
    if target_username:
        if target_username in all_users_decrypted:
            password = all_users_decrypted[target_username]
            if (
                password == "DECRYPTION_ERROR" or not password
            ):  # Check for error or empty password
                logger.error(
                    f"Cannot refresh target user {target_username}: Decryption failed or password missing."
                )
                return  # Exit if target user has bad credentials
            users_to_process = {target_username: password}
            logger.info(f"Targeting refresh for single user: {target_username}")
        else:
            logger.error(
                f"Target user {target_username} not found in stored credentials."
            )
            return  # Exit if target user doesn't exist
    else:
        # Filter out users with decryption errors or missing passwords
        users_to_process = {
            u: p
            for u, p in all_users_decrypted.items()
            if p and p != "DECRYPTION_ERROR"
        }
        total_users = len(all_users_decrypted)
        valid_users = len(users_to_process)
        skipped_count = total_users - valid_users
        if skipped_count > 0:
            logger.warning(
                f"Skipping {skipped_count} out of {total_users} users due to password decryption errors or missing passwords."
            )
        logger.info(
            f"Refreshing section {section} ({', '.join(data_types_to_run)}) for {valid_users} users."
        )

    if not users_to_process:
        logger.warning("No valid users to process after filtering. Exiting.")
        return

    # --- Run Refresh Tasks Concurrently per User ---
    overall_results = {}
    # Limit concurrent users being processed simultaneously if needed
    user_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_USERS)
    refreshed_course_urls_this_run = set() # Initialize set for courses refreshed in this run

    async def process_user_with_semaphore(username, password, data_types, refreshed_urls_set):
        async with user_semaphore:
            logger.info(f"Starting processing for user: {username}")
            result = await run_refresh_for_user(username, password, data_types, refreshed_urls_set)
            logger.info(f"Finished processing for user: {username}")
            return result

    user_refresh_coroutines = [
        process_user_with_semaphore(
            username, 
            password, 
            list(data_types_to_run), # Pass a copy of the list
            refreshed_course_urls_this_run # Pass the shared set
        )
        for username, password in users_to_process.items()
    ]

    results_list = await asyncio.gather(
        *user_refresh_coroutines, return_exceptions=True
    )

    # --- Process and Log Results ---
    processed_usernames = list(users_to_process.keys())
    for i, user_result_or_exc in enumerate(results_list):
        username = processed_usernames[i]
        if isinstance(user_result_or_exc, Exception):
            # Log the exception from the gather result for the whole user task
            overall_results[username] = {
                "error": f"User task group failed: {user_result_or_exc}"
            }
            logger.error(
                f"Refresh process for user {username} failed catastrophically: {user_result_or_exc}",
                exc_info=True,  # Include traceback for catastrophic user failure
            )
        elif isinstance(user_result_or_exc, dict):
            overall_results[username] = user_result_or_exc
        else:
            overall_results[username] = {
                "error": f"User task returned unexpected type: {type(user_result_or_exc)}"
            }
            logger.error(
                f"User task for {username} returned unexpected type: {type(user_result_or_exc)}"
            )

    # --- Log Summary ---
    logger.info("--- Cache Refresh Summary ---")
    total_items_processed = (
        0  # Count across all users and data types (excl. cms content courses)
    )
    total_updated = 0
    total_skipped = 0
    total_failed = 0
    cms_content_stats = {"updated": 0, "skipped": 0, "failed": 0}

    # Determine the data types actually expected to run (excluding cms_content for summary counts)
    summary_data_types = [dt for dt in data_types_to_run if dt != "cms_content"]

    # Specific tracking for global CMS refreshes if section 4 was run
    total_cms_courses_globally_updated_this_run = 0
    total_cms_courses_globally_failed_this_run = 0 # Tracks actual refresh failures
    
    if "cms_content" in data_types_to_run:
        # The set `refreshed_course_urls_this_run` contains successfully updated unique courses.
        # We need to sum up failures from individual user's attempts to initiate global refreshes.
        # This is tricky because `overall_results` contains per-user summaries of *their contribution* to global refresh.
        # A simpler global metric might be the size of `refreshed_course_urls_this_run` for successes.
        # Failures are harder to aggregate without double counting if multiple users triggered a fail for the same course.
        # For now, we rely on the per-user logging and the final size of the refreshed set.
        total_cms_courses_globally_updated_this_run = len(refreshed_course_urls_this_run)
        
        # To get total global failures, we'd need to parse the complex summary strings.
        # Let's log the number of unique courses updated this run.
        # The individual user logs from `run_refresh_for_user` for cms_content will give more detail.
        pass # Detailed parsing of individual user cms_content results for global sum is complex here

    for username, results in overall_results.items():
        user_summary_parts = []
        if results.get("error"):
            # User level error - mark all expected types as failed for this user
            user_summary_parts.append(f"USER_ERROR: {results['error']}")
            total_failed += len(data_types_to_run)  # Count all expected types as failed
            total_items_processed += len(data_types_to_run)
        else:
            # Process results per data type for this user
            for (
                data_type
            ) in data_types_to_run:  # Iterate through types requested for the section
                status = results.get(data_type, "not_run")  # Default if missing
                user_summary_parts.append(f"{data_type}: {status}")
                total_items_processed += 1  # Increment total items processed

                if data_type == "cms_content":
                    # The status for cms_content is now a more complex string per user
                    # reflecting their interaction with the global refresh process.
                    # Example: "Processed for user X: Globally Updated Now=1, Skipped (already updated this run)=2, ..."
                    # We don't try to sum these up into the old cms_content_stats buckets here,
                    # as the meaning has changed. The new global count is separate.
                    if isinstance(status, str): # Should be the summary string
                        pass # Already logged per user, and global summary is separate
                    else: # Fallback if status isn't the detailed string
                        logger.warning(f"Unexpected cms_content status format for {username}: {status}")
                        total_failed +=1 # Count this user's cms_content "task" as failed in overall user stats
                else:  # Handle non-cms_content types
                    if status == "updated":
                        total_updated += 1
                    elif "skipped" in status:  # Covers "skipped: reason"
                        total_skipped += 1
                    elif "failed" in status:  # Covers "failed: reason"
                        total_failed += 1
                    elif status == "not_run":
                        total_failed += 1  # Count not_run as failed
                    else:
                        logger.warning(
                            f"Unknown status '{status}' for {data_type} for {username}"
                        )
                        total_failed += 1  # Count unknown status as failed

        logger.info(f"User: {username} -> {'; '.join(user_summary_parts)}")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(
        f"--- Cache Refresh Script Finished: {end_time.isoformat()} (Duration: {duration}) ---"
    )
    if "cms_content" in data_types_to_run:
        logger.info(
            f"CMS Content Global Summary: Total unique courses successfully refreshed and cached this run = {total_cms_courses_globally_updated_this_run}"
        )
        # Old cms_content_stats is no longer directly applicable for global summary.
        # logger.info(
        #     f"CMS Content Courses Summary: Updated={cms_content_stats['updated']}, Skipped={cms_content_stats['skipped']}, Failed={cms_content_stats['failed']}"
        # )

    # Recalculate non-CMS totals based ONLY on summary_data_types status
    non_cms_updated = 0
    non_cms_skipped = 0
    non_cms_failed = 0
    for username, results in overall_results.items():
        if not results.get("error"):
            for data_type in summary_data_types:  # Only non-cms types
                status = results.get(data_type, "not_run")
                if status == "updated":
                    non_cms_updated += 1
                elif "skipped" in status:
                    non_cms_skipped += 1
                elif "failed" in status:
                    non_cms_failed += 1
                elif status == "not_run":
                    non_cms_failed += 1
                else:
                    non_cms_failed += 1
        else:
            non_cms_failed += len(
                summary_data_types
            )  # Count all as failed if user error

    logger.info(
        f"Overall Items Summary (excluding CMS content courses): Updated={non_cms_updated}, Skipped={non_cms_skipped}, Failed={non_cms_failed}"
    )


# --- Entry Point ---
if __name__ == "__main__":
    # Add configuration for things like notification users
    # This should ideally be in config.py but adding here for completeness if missing
    if not hasattr(config, "NOTIFICATION_ENABLED_USERS"):
        # Default to ALL users or load from ENV var 'NOTIFICATION_USERS' (e.g., "user1,user2")
        notification_users_str = os.getenv("NOTIFICATION_USERS", "ALL")
        if notification_users_str == "ALL":
            config.NOTIFICATION_ENABLED_USERS = "ALL"
        else:
            config.NOTIFICATION_ENABLED_USERS = [
                u.strip() for u in notification_users_str.split(",") if u.strip()
            ]
        logger.info(
            f"Notifications enabled for users: {config.NOTIFICATION_ENABLED_USERS}"
        )

    if not hasattr(config, "MAX_CONCURRENT_FETCHES"):
        config.MAX_CONCURRENT_FETCHES = 5  # Default concurrent fetches per user
        logger.info(
            f"Max concurrent fetches per user set to default: {config.MAX_CONCURRENT_FETCHES}"
        )

    if not hasattr(config, "MAX_CONCURRENT_USERS"):
        config.MAX_CONCURRENT_USERS = 10  # Default concurrent users processed
        logger.info(
            f"Max concurrent users set to default: {config.MAX_CONCURRENT_USERS}"
        )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Refresh script interrupted by user.")
    except Exception as e:
        logger.critical(f"Critical error during script execution: {e}", exc_info=True)
        sys.exit(1)
