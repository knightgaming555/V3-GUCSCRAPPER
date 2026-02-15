# Updated full script: scripts/refresh_cache.py
# Changes:
# - buffered MSET writes (JSON + PICKLE) as before
# - sequential global CMS refresh across users (dedupe per normalized course URL)
# - optional MGET prefetch for non-CMS keys to reduce read commands (safe, non-breaking)
# - preserves function signatures so other code doesn't need changes

import os
import sys
import asyncio
import json
import traceback
import logging
from datetime import datetime
from dotenv import load_dotenv
import concurrent.futures
import hashlib
import pickle
import threading

import redis

# --- Setup Paths and Load Env ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, ".env"))

# --- Import Config and Utils ---
try:
    from config import config
    from utils.auth import get_all_stored_users_decrypted

    # keep original get_from_cache reference to call fallback if needed
    from utils.cache import set_in_cache as set_json_cache_original
    from utils.cache import get_from_cache as get_from_cache_original
    from utils.cache import generate_cache_key
    # optional pickle helpers (may not exist)
    try:
        from utils.cache import get_pickle_cache as get_pickle_cache_original
        from utils.cache import set_pickle_cache as set_pickle_cache_original
    except Exception:
        get_pickle_cache_original = None
        set_pickle_cache_original = None

    from utils.helpers import normalize_course_url

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
        scrape_course_content,
        scrape_course_announcements,
    )
    from scraping.authenticate import authenticate_user
    from api.schedule import is_schedule_empty
except ImportError as e:
    print(f"Error importing scraping functions: {e}.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
log_file = os.path.join(project_root, "refresh_cache.log")
logging.basicConfig(
    level=getattr(config, "LOG_LEVEL", logging.INFO),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("refresh_cache_script")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("utils.notifications_utils").setLevel(logging.DEBUG)

# -----------------
# Buffering MSET code (writes)
# -----------------
JSON_BUFFER = {}
PICKLE_BUFFER = {}
JSON_BUFFER_LOCK = threading.Lock()
PICKLE_BUFFER_LOCK = threading.Lock()
MSET_CHUNK_SIZE = getattr(config, "MSET_CHUNK_SIZE", 200)

def set_json_cache_buffered(key: str, value, timeout: int = None) -> bool:
    """Buffer JSON-serializable data for later MSET flush. TTL ignored intentionally."""
    try:
        if isinstance(value, (str, bytes)):
            if isinstance(value, bytes):
                try:
                    ser = value.decode("utf-8")
                except Exception:
                    ser = repr(value)
            else:
                ser = value
        else:
            ser = json.dumps(value, default=str)
        with JSON_BUFFER_LOCK:
            JSON_BUFFER[key] = ser
        return True
    except Exception as e:
        logger.error(f"set_json_cache_buffered: failed to buffer {key}: {e}", exc_info=True)
        return False

def set_pickle_cache_buffered(key: str, value, timeout: int = None) -> bool:
    """Buffer Python object (pickled) for later MSET flush. TTL ignored intentionally."""
    try:
        pickled = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        with PICKLE_BUFFER_LOCK:
            PICKLE_BUFFER[key] = pickled
        return True
    except Exception as e:
        logger.error(f"set_pickle_cache_buffered: failed to buffer {key}: {e}", exc_info=True)
        return False

def flush_json_buffer(redis_url: str, chunk_size: int = MSET_CHUNK_SIZE) -> int:
    with JSON_BUFFER_LOCK:
        items = list(JSON_BUFFER.items())
        JSON_BUFFER.clear()
    if not items:
        return 0
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        logger.error(f"flush_json_buffer: failed to create redis client: {e}")
        with JSON_BUFFER_LOCK:
            for k, v in items:
                JSON_BUFFER[k] = v
        return 0
    flushed = 0
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i+chunk_size]
        mapping = {k: v for k, v in chunk}
        try:
            client.mset(mapping)
            flushed += len(mapping)
        except Exception as e:
            logger.error(f"flush_json_buffer: mset failed for chunk: {e}", exc_info=True)
            with JSON_BUFFER_LOCK:
                for k, v in chunk:
                    JSON_BUFFER[k] = v
    try:
        client.close()
    except Exception:
        pass
    return flushed

def flush_pickle_buffer(redis_url: str, chunk_size: int = MSET_CHUNK_SIZE) -> int:
    with PICKLE_BUFFER_LOCK:
        items = list(PICKLE_BUFFER.items())
        PICKLE_BUFFER.clear()
    if not items:
        return 0
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=False)
    except Exception as e:
        logger.error(f"flush_pickle_buffer: failed to create redis client: {e}")
        with PICKLE_BUFFER_LOCK:
            for k, v in items:
                PICKLE_BUFFER[k] = v
        return 0
    flushed = 0
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i+chunk_size]
        mapping = {k: v for k, v in chunk}
        try:
            client.mset(mapping)
            flushed += len(mapping)
        except Exception as e:
            logger.error(f"flush_pickle_buffer: mset failed for chunk: {e}", exc_info=True)
            with PICKLE_BUFFER_LOCK:
                for k, v in chunk:
                    PICKLE_BUFFER[k] = v
    try:
        client.close()
    except Exception:
        pass
    return flushed

def flush_all_buffers():
    try:
        redis_url = config.REDIS_URL
    except Exception as e:
        logger.error(f"flush_all_buffers: REDIS_URL missing in config: {e}")
        return
    j = flush_json_buffer(redis_url)
    p = flush_pickle_buffer(redis_url)
    logger.info(f"flush_all_buffers: flushed JSON={j} PICKLE={p}")

# Override cache functions used in this script so the rest of the code can call them unchanged
set_json_cache = set_json_cache_buffered
set_pickle_cache = set_pickle_cache_buffered

# Keep original get_pickle_cache reference if present
get_pickle_cache = get_pickle_cache_original if callable(get_pickle_cache_original) else (lambda k: None)

# -----------------
# MGET prefetch (reads) - optional, safe
# -----------------
# Local read cache used by this module. We don't change external modules.
LOCAL_READ_CACHE = {}
LOCAL_READ_CACHE_LOCK = threading.Lock()
MGET_CHUNK_SIZE = getattr(config, "MGET_CHUNK_SIZE", 200)

def bulk_mget_parse(redis_url: str, keys: list, decode_json=True, use_decode_responses=True):
    """Return dict key -> parsed value (JSON parsed if decode_json True)."""
    result = {}
    if not keys:
        return result
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=use_decode_responses)
    except Exception as e:
        logger.error(f"bulk_mget_parse: redis client creation failed: {e}")
        return result
    for i in range(0, len(keys), MGET_CHUNK_SIZE):
        chunk = keys[i:i+MGET_CHUNK_SIZE]
        try:
            values = client.mget(chunk)
        except Exception as e:
            logger.error(f"bulk_mget_parse: mget failed: {e}", exc_info=True)
            # keep whatever we've got so far
            break
        for k, v in zip(chunk, values):
            if v is None:
                result[k] = None
            else:
                if decode_json:
                    try:
                        result[k] = json.loads(v)
                    except Exception:
                        result[k] = v
                else:
                    # return raw bytes if decode_responses=False, else raw string
                    result[k] = v
    try:
        client.close()
    except Exception:
        pass
    return result

def prefill_local_read_cache_for_prefix(prefix: str, usernames: list, redis_url: str, decode_json=True):
    """Build keys for prefix and usernames, bulk mget them and populate LOCAL_READ_CACHE."""
    keys = []
    for u in usernames:
        try:
            k = generate_cache_key(prefix, u)
            if k:
                keys.append(k)
        except Exception:
            # fallback: skip user if key gen fails
            continue
    if not keys:
        return 0
    kv = bulk_mget_parse(redis_url, keys, decode_json=decode_json, use_decode_responses=True)
    with LOCAL_READ_CACHE_LOCK:
        LOCAL_READ_CACHE.update(kv)
    return len(kv)

# monkeypatch get_from_cache within this module to check local cache first
def get_from_cache(key):
    with LOCAL_READ_CACHE_LOCK:
        if key in LOCAL_READ_CACHE:
            return LOCAL_READ_CACHE[key]
    # fallback to original
    try:
        return get_from_cache_original(key)
    except Exception as e:
        logger.debug(f"get_from_cache fallback failed for {key}: {e}")
        return None

# --- Helper functions ---
def generate_cms_content_cache_key(course_url: str) -> str:
    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        return None
    hash_value = hashlib.md5(normalized_url.encode("utf-8")).hexdigest()
    return f"cms_content:{hash_value}"

def _is_cms_content_substantial(content_data: list) -> bool:
    if not content_data or not isinstance(content_data, list):
        return False
    actual_content_weeks = 0
    total_materials = 0
    for item in content_data:
        if isinstance(item, dict):
            if "course_announcement" in item:
                continue
            if "week_title" in item and "week_content" in item:
                week_content = item.get("week_content", [])
                if isinstance(week_content, list) and len(week_content) > 0:
                    week_title = item.get("week_title", "").lower()
                    if "mock" not in week_title and "placeholder" not in week_title:
                        actual_content_weeks += 1
                        total_materials += len(week_content)
    return actual_content_weeks >= 1 or total_materials >= 3

# --- Script Constants ---
REFRESH_CONFIG = {
    "guc_data": {
        "func": scrape_guc_data,
        "args": [],
        "cache_prefix": "guc_data",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": compare_guc_data,
    },
    "schedule": {
        "func": scrape_schedule,
        "args": [],
        "cache_prefix": "schedule",
        "timeout": config.CACHE_LONG_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": None,
    },
    "cms_courses": {
        "func": scrape_cms_courses,
        "args": [],
        "cache_prefix": "cms_courses",
        "timeout": config.CACHE_LONG_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": None,
    },
    "grades": {
        "func": scrape_grades,
        "args": [],
        "cache_prefix": "grades",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": compare_grades,
    },
    "attendance": {
        "func": scrape_attendance,
        "args": [],
        "cache_prefix": "attendance",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": compare_attendance,
    },
    "exam_seats": {
        "func": scrape_exam_seats,
        "args": [],
        "cache_prefix": "exam_seats",
        "timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_func": set_json_cache,
        "compare_func": None,
    },
    "cms_content": {
        "func": None,
        "args": [],
        "cache_prefix": "cms_content",
        "timeout": 18000,
        "cache_func": set_pickle_cache,
        "compare_func": None,
    },
}

TARGET_NOTIFICATION_USERS = getattr(config, 'TARGET_NOTIFICATION_USERS', ["mohamed.elsaadi", "seif.elkady"])
MAX_NOTIFICATIONS_LIMIT = getattr(config, 'MAX_NOTIFICATIONS_LIMIT', 5)
RETRY_DELAY_SECONDS = getattr(config, 'RETRY_DELAY_SECONDS', 5)
SECTION_MAP = {
    "1": ["guc_data", "schedule"],
    "2": ["cms_courses", "grades"],
    "3": ["attendance", "exam_seats"],
    "4": ["cms_content"],
}
AUTH_PRECHECK_ENABLED = os.getenv("REFRESH_PRECHECK_AUTH", "true").strip().lower() in ("true", "1", "yes", "y")
MAX_CONCURRENT_AUTH_CHECKS = int(os.getenv("REFRESH_PRECHECK_CONCURRENCY", "10"))

# --- Wrapper for Single Course Content Refresh ---
async def _refresh_single_cms_course(username_for_creds, password_for_creds, course_entry):
    course_url = course_entry.get("course_url")
    course_name = course_entry.get("course_name", "Unknown")

    if not course_url:
        logger.error(f"Missing course_url in course entry for {username_for_creds} (used for creds): {course_entry}")
        return {"status": "skipped", "reason": "missing url"}

    normalized_url = normalize_course_url(course_url)
    if not normalized_url:
        logger.error(f"URL normalization failed for {username_for_creds} (used for creds) - {course_name}: {course_url}")
        return {"status": "skipped", "reason": "normalization failed"}

    cache_key = generate_cms_content_cache_key(normalized_url)
    if not cache_key:
        logger.error(f"Could not generate GLOBAL cache key for {course_name} (URL: {normalized_url})")
        return {"status": "failed", "reason": "global key generation error"}

    logger.debug(f"Refreshing CMS content for course {course_name} ({normalized_url}) using creds of {username_for_creds}")

    content_list = None
    announcement_result = None
    fetch_success = False
    loop = asyncio.get_running_loop()

    max_retries = 3
    for attempt in range(max_retries):
        logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch CMS content for {course_name} ({normalized_url})")
        current_attempt_fetch_success = False
        max_workers_cms = min(4, os.cpu_count() or 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_cms, thread_name_prefix="CourseFetchAttempt") as pool:
            try:
                content_future = loop.run_in_executor(pool, scrape_course_content, username_for_creds, password_for_creds, normalized_url)
                announcement_future = loop.run_in_executor(pool, scrape_course_announcements, username_for_creds, password_for_creds, normalized_url)
                content_list = await content_future
                announcement_result = await announcement_future

                if content_list is not None or announcement_result is not None:
                    current_attempt_fetch_success = True
                    fetch_success = True
                    if content_list is None:
                        logger.warning(f"CMS Content fetch (Attempt {attempt+1}) returned None for {course_name}")
                    if announcement_result is None:
                        logger.warning(f"CMS Announcement fetch (Attempt {attempt+1}) returned None for {course_name}")
                    break
                else:
                    logger.warning(f"Attempt {attempt+1}: Both content and announcement fetch returned None for {course_name}.")
            except Exception as fetch_exc:
                logger.error(f"Exception during concurrent fetch (Attempt {attempt + 1}) for {course_name} ({normalized_url}): {fetch_exc}", exc_info=True if attempt == max_retries -1 else False)
        if fetch_success:
            break
        if attempt < max_retries - 1:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {course_name}. Retrying in {RETRY_DELAY_SECONDS}s...")
            await asyncio.sleep(RETRY_DELAY_SECONDS)
        else:
            logger.error(f"All {max_retries} attempts failed to fetch CMS content for {course_name} ({normalized_url}).")

    if not fetch_success:
        logger.warning(f"All fetch attempts failed for {course_name}. Cache not updated.")
        return {"status": "failed_fetch_retries", "reason": "fetch failed after retries"}

    combined_data_for_cache = []
    course_announcement_dict_to_add = None

    if announcement_result and isinstance(announcement_result, dict):
        html_content = announcement_result.get("announcements_html")
        if html_content and html_content.strip():
            course_announcement_dict_to_add = {"course_announcement": html_content}
            combined_data_for_cache.append(course_announcement_dict_to_add)
        elif "error" in announcement_result:
            logger.warning(f"Announcement scraping reported error for {normalized_url}: {announcement_result['error']}")

    if content_list is not None and isinstance(content_list, list):
        combined_data_for_cache.extend(content_list)

    new_data_is_substantial = _is_cms_content_substantial(combined_data_for_cache)

    if combined_data_for_cache:
        cache_func = REFRESH_CONFIG["cms_content"]["cache_func"]
        timeout = REFRESH_CONFIG["cms_content"]["timeout"]

        if not new_data_is_substantial:
            logger.warning(f"Newly fetched CMS content for {course_name} is not substantial (empty/minimal content)")
            existing_cached_data = get_pickle_cache(cache_key) if callable(get_pickle_cache) else None
            existing_data_is_substantial = _is_cms_content_substantial(existing_cached_data)

            if existing_cached_data and existing_data_is_substantial:
                logger.info(f"Preserving existing substantial CMS content cache for {course_name} instead of overwriting with insufficient new data (Key: {cache_key})")
                if cache_func(cache_key, existing_cached_data, timeout=timeout):
                    logger.info(f"Successfully preserved and refreshed existing CMS content cache for {course_name}")
                    return {"status": "preserved_existing", "refreshed_url": normalized_url}
                else:
                    logger.error(f"Failed to refresh existing CMS content cache timeout for {course_name}")
            else:
                logger.info(f"No existing substantial cache found for {course_name}, will cache new data even if minimal")

        if cache_func(cache_key, combined_data_for_cache, timeout=timeout):
            status_msg = "updated" if new_data_is_substantial else "updated_minimal"
            logger.info(f"Successfully {'refreshed' if new_data_is_substantial else 'cached minimal'} GLOBAL CMS content cache for {course_name} (Key: {cache_key})")
            return {"status": status_msg, "refreshed_url": normalized_url}
        else:
            logger.error(f"Failed to set GLOBAL CMS content pickle cache for {course_name} (Key: {cache_key})")
            return {"status": "failed_cache_set", "reason": "cache set error"}
    else:
        logger.warning(f"Skipped GLOBAL CMS content cache update for {course_name} - no data assembled.")
        return {"status": "skipped", "reason": "no data assembled"}

# --- Refactored Async Task Runner ---
async def run_refresh_for_user(username, password, data_types_to_run, refreshed_course_urls_this_run: set):
    user_results = {}
    max_concurrent_fetches = getattr(config, 'MAX_CONCURRENT_FETCHES', 5)
    current_run_all_update_messages = []

    logger.info(f"Processing user: {username} for data types: {data_types_to_run}")

    if "cms_content" in data_types_to_run:
        logger.info(f"Processing CMS content section for user: {username}")
        course_list = None
        try:
            loop = asyncio.get_running_loop()
            course_list = await loop.run_in_executor(None, scrape_cms_courses, username, password)
        except Exception as e:
            logger.error(f"Failed to fetch course list for {username} during cms_content processing: {e}", exc_info=True)
            user_results["cms_content"] = "failed: could not get course list for this user"
            course_list = None

        if isinstance(course_list, list) and course_list:
            logger.info(f"User {username} has {len(course_list)} courses. Checking against globally refreshed list.")
            course_tasks = []
            semaphore = asyncio.Semaphore(max_concurrent_fetches)

            cms_success_count_for_user = 0
            cms_skipped_this_run_count_for_user = 0
            cms_failed_count_for_user = 0
            cms_skipped_fetch_issues_for_user = 0

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

                # IMPORTANT: check the shared refreshed set to avoid duplicate global refreshes.
                # run_refresh_for_user is now called sequentially for cms_content across users in main(),
                # so this check prevents redundant refreshes if a prior user already did it this run.
                if normalized_course_url in refreshed_course_urls_this_run:
                    logger.info(f"CMS Content for course '{course_name}' ({normalized_course_url}) already refreshed this run. Skipping for user {username}.")
                    cms_skipped_this_run_count_for_user += 1
                    continue

                logger.info(f"Course '{course_name}' ({normalized_course_url}) for user {username} needs global refresh.")
                # Create the global refresh task (it will set cache when done)
                task_coro = _refresh_single_cms_course(username, password, course_entry)
                task = asyncio.create_task(fetch_with_semaphore(task_coro), name=f"GLOBAL_CMS_{normalized_course_url.split('/')[-1]}")
                course_tasks.append((task, normalized_course_url))

            if course_tasks:
                logger.info(f"Awaiting {len(course_tasks)} global CMS refresh tasks initiated by {username}.")
                # Await tasks but attach their normalized urls so we can add successful ones to the shared set.
                coros = [t for t, _ in course_tasks]
                results = await asyncio.gather(*coros, return_exceptions=True)

                for idx, res in enumerate(results):
                    normalized_url = course_tasks[idx][1]
                    if isinstance(res, Exception):
                        cms_failed_count_for_user += 1
                        logger.error(f"A global CMS Content course task (initiated by {username}) failed with exception: {res}")
                    elif isinstance(res, dict):
                        status = res.get("status", "unknown")
                        refreshed_url_val = res.get("refreshed_url")
                        if status in ["updated", "updated_minimal", "preserved_existing"] and refreshed_url_val:
                            cms_success_count_for_user += 1
                            # Add to shared set so subsequent users skip it
                            refreshed_course_urls_this_run.add(refreshed_url_val)
                        elif status == "skipped":
                            cms_skipped_fetch_issues_for_user += 1
                        elif status in ["failed_fetch_retries", "failed_cache_set", "failed"]:
                            cms_failed_count_for_user += 1
                        else:
                            cms_failed_count_for_user += 1
                            logger.warning(f"Global CMS Content course task (initiated by {username}) returned unknown status: {status}")

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
            logger.error(f"Unexpected course list format for user {username}: {type(course_list)}")

        # remove cms_content from the data types list for further processing inside this user if present
        data_types_to_run = [dt for dt in data_types_to_run if dt != "cms_content"]
        if not data_types_to_run:
            return user_results

    # --- Prepare & Run Other Sync Tasks Concurrently (Sections 1, 2, 3) ---
    other_tasks = []
    data_type_map = {}
    semaphore = asyncio.Semaphore(max_concurrent_fetches)

    async def run_scrape_with_semaphore(func, args, data_type):
        async with semaphore:
            logger.debug(f"Starting scrape task for {username} - {data_type}")
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, func, *args)
            logger.debug(f"Finished scrape task for {username} - {data_type}")
            return result

    for data_type in data_types_to_run:
        if data_type in REFRESH_CONFIG:
            config_item = REFRESH_CONFIG[data_type]
            if config_item.get("func"):
                full_args = [username, password] + config_item["args"]
                task = asyncio.create_task(run_scrape_with_semaphore(config_item["func"], full_args, data_type), name=f"{username}_{data_type}")
                other_tasks.append(task)
                data_type_map[task] = data_type
            else:
                logger.warning(f"No function defined for data type '{data_type}' for user {username}. Skipping.")
                user_results[data_type] = "skipped: no function defined"
        else:
            logger.warning(f"Unknown data type '{data_type}' requested for user {username}. Skipping.")
            user_results[data_type] = "skipped: unknown type"

    if other_tasks:
        logger.debug(f"Awaiting {len(other_tasks)} other tasks for user {username}")
        results_list = await asyncio.gather(*other_tasks, return_exceptions=True)
        logger.debug(f"Finished awaiting other tasks for user {username}")

        for i, result_or_exc in enumerate(results_list):
            task = other_tasks[i]
            task_name = task.get_name()
            data_type = data_type_map.get(task, f"unknown_type_{i}")

            if data_type not in REFRESH_CONFIG:
                logger.error(f"Internal Error: Task result received for unknown data_type '{data_type}' ({task_name})")
                user_results[data_type] = "failed: internal config error"
                continue

            config_item = REFRESH_CONFIG[data_type]
            cache_prefix = config_item["cache_prefix"]
            timeout = config_item["timeout"]
            cache_func = config_item["cache_func"]
            compare_func = config_item.get("compare_func")

            is_failure = False
            error_message = None
            final_data = None

            if isinstance(result_or_exc, Exception):
                is_failure = True
                error_detail = str(result_or_exc)
                error_message = f"failed: Task exception - {error_detail}"
                logger.error(f"Refresh task {task_name} failed: {result_or_exc}", exc_info=False)

            elif isinstance(result_or_exc, dict) and "error" in result_or_exc:
                is_failure = True
                error_message = f"skipped: {result_or_exc['error']}"
                logger.warning(f"Refresh task {task_name} returned error: {error_message}")
            elif result_or_exc is None:
                is_failure = True
                error_message = "skipped: scraper returned None"
                logger.warning(f"Refresh task {task_name} returned None.")
            else:
                final_data = result_or_exc

            if is_failure:
                user_results[data_type] = error_message
                continue

            if final_data is not None:
                cache_key = generate_cache_key(cache_prefix, username)
                data_to_cache = final_data

                if data_type == "schedule":
                    try:
                        filtered = filter_schedule_details(final_data)
                        if is_schedule_empty(filtered):
                            logger.info(f"Schedule for {username} contains no meaningful course data, caching empty array")
                            data_to_cache = []
                        else:
                            timings = {"0": "8:15AM-9:45AM", "1": "10:00AM-11:30AM", "2": "11:45AM-1:15PM", "3": "1:45PM-3:15PM", "4": "3:45PM-5:15PM"}
                            data_to_cache = (filtered, timings)
                    except Exception as e_filter:
                        logger.error(f"Failed to filter schedule data for {task_name}: {e_filter}")
                        user_results[data_type] = "failed: result filtering error"
                        continue

                changes_detected_by_compare_func = []

                if compare_func:
                    try:
                        old_data = get_from_cache(cache_key)
                        if old_data:
                            logger.debug(f"Calling compare_func for {data_type} for {username}")
                            changes_detected_by_compare_func = compare_func(username, old_data, data_to_cache)
                            if changes_detected_by_compare_func:
                                logger.info(f"Compare function for {data_type} found {len(changes_detected_by_compare_func)} changes for {username}.")
                                logger.debug(f"Changes from compare_func for {username} ({data_type}): {changes_detected_by_compare_func}")
                                if username in TARGET_NOTIFICATION_USERS:
                                    for change_item in changes_detected_by_compare_func:
                                        if isinstance(change_item, list) and len(change_item) == 2:
                                            category = data_type.capitalize()
                                            description = change_item[1]
                                            formatted_msg = f"[{category}] {description}"
                                            current_run_all_update_messages.append(formatted_msg)
                                        else:
                                            logger.warning(f"Unexpected format for change_item from compare_func for {username} ({data_type}): {change_item}")
                        else:
                            logger.debug(f"Skipping {data_type} comparison for {username} (no old data found in cache key {cache_key})")
                    except Exception as e_comp:
                        logger.error(f"Error during {data_type} comparison or new notification processing for {username}: {e_comp}", exc_info=True)

                logger.debug(f"Attempting to cache {data_type} for {task_name} under key {cache_key}")
                set_success = cache_func(cache_key, data_to_cache, timeout=timeout)

                if set_success:
                    user_results[data_type] = "updated"
                    logger.debug(f"Successfully cached {data_type} for {task_name}")
                else:
                    user_results[data_type] = "failed: cache set error"
                    logger.error(f"Failed to set {data_type} cache for {task_name} (key: {cache_key})")
            else:
                logger.error(f"Internal logic error: Reached cache step but final_data is None for {task_name}")
                user_results[data_type] = "failed: internal logic error (final_data None)"

    if username in TARGET_NOTIFICATION_USERS:
        if current_run_all_update_messages:
            logger.info(f"Collected {len(current_run_all_update_messages)} new user-specific update message(s) for {username} in this run. Preparing batch.")
            user_notif_cache_key = f"user_notifications_{username}"
            new_batch_entry = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), "messages": current_run_all_update_messages}
            existing_user_updates = get_from_cache(user_notif_cache_key) or []
            if not isinstance(existing_user_updates, list):
                logger.warning(f"Corrupted user notifications cache for {user_notif_cache_key} (type: {type(existing_user_updates)}). Resetting to empty list.")
                existing_user_updates = []
            updated_user_updates = [new_batch_entry] + existing_user_updates
            updated_user_updates = updated_user_updates[:MAX_NOTIFICATIONS_LIMIT]
            VERY_LONG_TIMEOUT = 365 * 24 * 60 * 60
            if set_json_cache(user_notif_cache_key, updated_user_updates, timeout=VERY_LONG_TIMEOUT):
                logger.info(f"Successfully cached consolidated batch of {len(current_run_all_update_messages)} updates for {username}. Total batches: {len(updated_user_updates)}.")
            else:
                logger.error(f"Failed to cache consolidated batch of updates for {username} at key {user_notif_cache_key}")
        else:
            logger.info(f"No new user-specific update messages collected for {username} in this run. No batch to cache.")

    return user_results

# --- Main Script Logic ---
async def main():
    start_time = datetime.now()
    logger.info(f"--- Cache Refresh Script Started: {start_time.isoformat()} ---")

    if len(sys.argv) < 2 or sys.argv[1] not in SECTION_MAP:
        valid_sections = ", ".join(SECTION_MAP.keys())
        print(f"Usage: python {sys.argv[0]} <section_number> [username]", file=sys.stderr)
        print(f"  section_number: {valid_sections}", file=sys.stderr)
        print("  username (optional): Refresh only for this specific user.", file=sys.stderr)
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
        if target_username in all_users_decrypted:
            password = all_users_decrypted[target_username]
            if password == "DECRYPTION_ERROR" or not password:
                logger.error(f"Cannot refresh target user {target_username}: Decryption failed or password missing.")
                return
            users_to_process = {target_username: password}
            logger.info(f"Targeting refresh for single user: {target_username}")
        else:
            logger.error(f"Target user {target_username} not found in stored credentials.")
            return
    else:
        users_to_process = {u: p for u, p in all_users_decrypted.items() if p and p != "DECRYPTION_ERROR"}
        total_users = len(all_users_decrypted)
        valid_users = len(users_to_process)
        skipped_count = total_users - valid_users
        if skipped_count > 0:
            logger.warning(f"Skipping {skipped_count} out of {total_users} users due to password decryption errors or missing passwords.")
        logger.info(f"Refreshing section {section} ({', '.join(data_types_to_run)}) for {valid_users} users.")

    if not users_to_process:
        logger.warning("No valid users to process after filtering. Exiting.")
        return

    overall_results = {}

    # --- Optional: Pre-check credentials to avoid unnecessary scraping calls ---
    if AUTH_PRECHECK_ENABLED:
        logger.info("Pre-checking user credentials before refresh...")
        auth_semaphore = asyncio.Semaphore(MAX_CONCURRENT_AUTH_CHECKS)
        valid_users = {}
        invalid_users = {}

        async def _check_user_auth(u, p):
            async with auth_semaphore:
                loop = asyncio.get_running_loop()
                try:
                    ok = await loop.run_in_executor(None, authenticate_user, u, p)
                except Exception as e:
                    logger.warning(f"Auth precheck exception for {u}: {e}")
                    ok = False
                return u, ok

        auth_tasks = [asyncio.create_task(_check_user_auth(u, p)) for u, p in users_to_process.items()]
        auth_results = await asyncio.gather(*auth_tasks)

        for u, ok in auth_results:
            if ok:
                valid_users[u] = users_to_process[u]
            else:
                invalid_users[u] = users_to_process[u]

        if invalid_users:
            logger.warning(f"Auth precheck failed for {len(invalid_users)} user(s). Skipping their refresh.")
            for u in invalid_users:
                overall_results[u] = {dt: "skipped: auth_precheck_failed" for dt in data_types_to_run}

        users_to_process = valid_users

        if target_username and target_username not in users_to_process:
            logger.error(f"Target user {target_username} failed auth precheck. Exiting.")
            return

        if not users_to_process:
            logger.warning("No users passed auth precheck. Exiting.")
            return
    user_semaphore = asyncio.Semaphore(getattr(config, 'MAX_CONCURRENT_USERS', 10))
    refreshed_course_urls_this_run = set()

    async def process_user_with_semaphore(username, password, data_types, refreshed_urls_set):
        async with user_semaphore:
            logger.info(f"Starting processing for user: {username}")
            result = await run_refresh_for_user(username, password, data_types, refreshed_urls_set)
            logger.info(f"Finished processing for user: {username}")
            return result

    # ---------------
    # Optional: prefill local read cache for non-CMS prefixes (MGET)
    # ---------------
    # We'll prefetch old cached values for the users for data types that are not cms_content.
    # This reduces per-user GET calls during comparisons.
    non_cms_prefixes = []
    for dt in data_types_to_run:
        if dt in REFRESH_CONFIG and dt != "cms_content":
            non_cms_prefixes.append(REFRESH_CONFIG[dt]["cache_prefix"])
    # unique prefixes
    non_cms_prefixes = list(dict.fromkeys(non_cms_prefixes))
    if non_cms_prefixes:
        usernames = list(users_to_process.keys())
        for prefix in non_cms_prefixes:
            try:
                count = prefill_local_read_cache_for_prefix(prefix, usernames, config.REDIS_URL, decode_json=True)
                logger.info(f"Prefilled local read cache for prefix '{prefix}' with {count} entries")
            except Exception as e:
                logger.debug(f"Prefill local cache failed for prefix {prefix}: {e}", exc_info=True)

    # ---------------
    # Phase 1: If cms_content is requested, process cms_content sequentially per user.
    # This ensures we refresh shared courses only once per run (dedupe).
    # ---------------
    if "cms_content" in data_types_to_run:
        logger.info("Starting sequential CMS-content pass to dedupe global course refreshes...")
        for username, password in users_to_process.items():
            # call only cms_content for this user, sequentially
            try:
                res = await process_user_with_semaphore(username, password, ["cms_content"], refreshed_course_urls_this_run)
                # store or merge results
                existing = overall_results.get(username, {})
                if isinstance(existing, dict):
                    existing.update(res if isinstance(res, dict) else {})
                    overall_results[username] = existing
                else:
                    overall_results[username] = res if isinstance(res, dict) else {}
            except Exception as e:
                overall_results[username] = {"error": f"cms_content phase failed: {e}"}
                logger.error(f"CMS content sequential processing failed for {username}: {e}", exc_info=True)

    # ---------------
    # Phase 2: Run other data types concurrently per user (excluding cms_content)
    # ---------------
    concurrent_coros = []
    usernames_for_coros = []
    for username, password in users_to_process.items():
        # prepare data types excluding cms_content
        dt_other = [dt for dt in data_types_to_run if dt != "cms_content"]
        if not dt_other:
            # nothing else to run for this section for this user
            continue
        concurrent_coros.append(process_user_with_semaphore(username, password, dt_other, refreshed_course_urls_this_run))
        usernames_for_coros.append(username)

    if concurrent_coros:
        logger.info(f"Starting concurrent pass for other data types ({len(concurrent_coros)} user tasks)...")
        gathered = await asyncio.gather(*concurrent_coros, return_exceptions=True)
        for idx, res in enumerate(gathered):
            uname = usernames_for_coros[idx]
            if isinstance(res, Exception):
                overall_results[uname] = {"error": f"User task group failed: {res}"}
                logger.error(f"Refresh process for user {uname} failed catastrophically: {res}", exc_info=True)
            elif isinstance(res, dict):
                # merge with any existing results (e.g., cms_content summary)
                existing = overall_results.get(uname, {})
                if isinstance(existing, dict):
                    existing.update(res)
                    overall_results[uname] = existing
                else:
                    overall_results[uname] = res
            else:
                overall_results[uname] = {"error": f"User task returned unexpected type: {type(res)}"}
                logger.error(f"User task for {uname} returned unexpected type: {type(res)}")

    # ---------------
    # Summarize results (similar to your original logic)
    # ---------------
    logger.info("--- Cache Refresh Summary ---")
    # summary_data_types exclude cms_content when summarizing non-cms totals
    summary_data_types = [dt for dt in data_types_to_run if dt != "cms_content"]

    non_cms_updated = non_cms_skipped = non_cms_failed = 0
    for username, results in overall_results.items():
        if not isinstance(results, dict):
            continue
        if not results.get("error"):
            for data_type in summary_data_types:
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
            non_cms_failed += len(summary_data_types)

    logger.info(f"Overall Items Summary (excluding CMS content courses): Updated={non_cms_updated}, Skipped={non_cms_skipped}, Failed={non_cms_failed}")

    # ---------------
    # Flush buffered cache writes to Redis before finishing
    # ---------------
    try:
        flush_all_buffers()
    except Exception as e:
        logger.error(f"Error while flushing buffers: {e}", exc_info=True)

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"--- Cache Refresh Script Finished: {end_time.isoformat()} (Duration: {duration}) ---")

    if "cms_content" in data_types_to_run:
        logger.info(f"CMS Content Global Summary: Total unique courses successfully refreshed and cached this run = {len(refreshed_course_urls_this_run)}")

# --- Entry Point ---
if __name__ == "__main__":
    if not hasattr(config, "NOTIFICATION_ENABLED_USERS"):
        notification_users_str = os.getenv("NOTIFICATION_USERS", "ALL")
        if notification_users_str == "ALL":
            config.NOTIFICATION_ENABLED_USERS = "ALL"
        else:
            config.NOTIFICATION_ENABLED_USERS = [u.strip() for u in notification_users_str.split(",") if u.strip()]
        logger.info(f"Notifications enabled for users: {config.NOTIFICATION_ENABLED_USERS}")

    if not hasattr(config, "MAX_CONCURRENT_FETCHES"):
        config.MAX_CONCURRENT_FETCHES = 5
        logger.info(f"Max concurrent fetches per user set to default: {config.MAX_CONCURRENT_FETCHES}")

    if not hasattr(config, "MAX_CONCURRENT_USERS"):
        config.MAX_CONCURRENT_USERS = 10
        logger.info(f"Max concurrent users set to default: {config.MAX_CONCURRENT_USERS}")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Refresh script interrupted by user.")
    except Exception as e:
        logger.critical(f"Critical error during script execution: {e}", exc_info=True)
        try:
            flush_all_buffers()
        except Exception:
            pass
        sys.exit(1)
