# scripts/refresh_cache.py
import os
import sys
import asyncio
import traceback
import logging
from datetime import datetime
from dotenv import load_dotenv
import concurrent.futures
import pickle # Used for type checking

import redis # For redis.exceptions

# --- Setup Paths and Load Env ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, ".env"))

# --- Import Config and Utils ---
try:
    from config import config
    from utils.auth import get_all_stored_users_decrypted

    from utils.cache import (
        redis_client,
        get_data_and_stored_hash,
        set_data_and_hash_pipelined,
        expire_keys_pipelined,
        _get_canonical_bytes_for_hashing,
        _generate_data_hash,
        generate_cache_key
    )
    from utils.cache import get_from_cache as get_json_from_cache_direct
    from utils.cache import set_in_cache as set_json_in_cache_direct

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
    from api.schedule import is_schedule_empty
except ImportError as e:
    print(f"Error importing scraping functions: {e}.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
log_file = os.path.join(project_root, "refresh_cache.log")
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("refresh_cache_script")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("utils.notifications_utils").setLevel(logging.DEBUG)


def generate_cms_content_key_base(course_url: str) -> str:
    normalized_url = normalize_course_url(course_url)
    if not normalized_url: return None
    import hashlib as local_hashlib # Keep local import for specific key generation
    hash_value = local_hashlib.md5(normalized_url.encode("utf-8")).hexdigest()
    return f"cms_content:{hash_value}"


def _is_cms_content_substantial(content_data: list) -> bool:
    if not content_data or not isinstance(content_data, list): return False
    actual_content_weeks, total_materials = 0, 0
    for item in content_data:
        if isinstance(item, dict):
            if "course_announcement" in item: continue
            if "week_name" in item and "contents" in item:
                week_contents = item.get("contents", [])
                if isinstance(week_contents, list) and len(week_contents) > 0:
                    week_name = item.get("week_name", "").lower()
                    if "mock" not in week_name and "placeholder" not in week_name:
                        actual_content_weeks += 1
                        total_materials += len(week_contents)
    return actual_content_weeks >= 1 or total_materials >= 3


REFRESH_CONFIG = {
    "guc_data": {"func": scrape_guc_data, "args": [], "cache_prefix": "guc_data", "timeout": config.CACHE_DEFAULT_TIMEOUT, "compare_func": compare_guc_data, "is_pickle": False},
    "schedule": {"func": scrape_schedule, "args": [], "cache_prefix": "schedule", "timeout": config.CACHE_LONG_TIMEOUT, "compare_func": None, "is_pickle": False},
    "cms_courses": {"func": scrape_cms_courses, "args": [], "cache_prefix": "cms_courses", "timeout": config.CACHE_LONG_TIMEOUT, "compare_func": None, "is_pickle": False},
    "grades": {"func": scrape_grades, "args": [], "cache_prefix": "grades", "timeout": config.CACHE_DEFAULT_TIMEOUT, "compare_func": compare_grades, "is_pickle": False},
    "attendance": {"func": scrape_attendance, "args": [], "cache_prefix": "attendance", "timeout": config.CACHE_DEFAULT_TIMEOUT, "compare_func": compare_attendance, "is_pickle": False},
    "exam_seats": {"func": scrape_exam_seats, "args": [], "cache_prefix": "exam_seats", "timeout": config.CACHE_DEFAULT_TIMEOUT, "compare_func": None, "is_pickle": False},
    "cms_content": {"func": None, "args": [], "cache_prefix": "cms_content", "timeout": config.CACHE_LONG_CMS_CONTENT_TIMEOUT, "compare_func": None, "is_pickle": True},
}

TARGET_NOTIFICATION_USERS = ["mohamed.elsaadi", "seif.elkady"] # Example
MAX_NOTIFICATIONS_LIMIT = 5
RETRY_DELAY_SECONDS = 5
SECTION_MAP = {"1": ["guc_data", "schedule"], "2": ["cms_courses", "grades"], "3": ["attendance", "exam_seats"], "4": ["cms_content"]}


async def _refresh_single_cms_course(username_for_creds, password_for_creds, course_entry, cms_pipeline):
    # ... (This function remains unchanged from the previous complete version) ...
    course_url = course_entry.get("course_url")
    course_name = course_entry.get("course_name", "Unknown")
    if not course_url: return {"status": "skipped_no_url", "reason": "missing url"}
    normalized_url = normalize_course_url(course_url)
    if not normalized_url: return {"status": "skipped_norm_fail", "reason": "normalization failed"}
    key_base = generate_cms_content_key_base(normalized_url)
    if not key_base: return {"status": "failed_key_gen", "reason": "global key_base generation error"}

    logger.debug(f"Preparing CMS content for course {course_name} ({normalized_url}) using creds of {username_for_creds}")
    combined_data_for_cache, fetch_success = None, False
    max_retries = 3
    for attempt in range(max_retries):
        logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch CMS content for {course_name} ({normalized_url})")
        loop, max_workers_cms = asyncio.get_running_loop(), min(4, os.cpu_count() or 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_cms, thread_name_prefix="CourseFetchAttempt") as pool:
            try:
                content_future = loop.run_in_executor(pool, scrape_course_content, username_for_creds, password_for_creds, normalized_url)
                announcement_future = loop.run_in_executor(pool, scrape_course_announcements, username_for_creds, password_for_creds, normalized_url)
                content_list_result, announcement_result = await content_future, await announcement_future
                if content_list_result is not None or announcement_result is not None:
                    assembled_data = []
                    if announcement_result and isinstance(announcement_result, dict):
                        html_content = announcement_result.get("announcements_html")
                        if html_content and html_content.strip(): assembled_data.append({"course_announcement": html_content})
                        elif "error" in announcement_result: logger.warning(f"Ann. scrape error for {normalized_url}: {announcement_result['error']}")
                    if content_list_result and isinstance(content_list_result, list): assembled_data.extend(content_list_result)
                    elif content_list_result and "error" in content_list_result: logger.warning(f"Content scrape error for {normalized_url}: {content_list_result['error']}")
                    if assembled_data: combined_data_for_cache, fetch_success = assembled_data, True; break
                    else: logger.warning(f"Attempt {attempt+1}: Fetch successful but no data assembled for {course_name}.")
                else: logger.warning(f"Attempt {attempt+1}: Both content and announcement fetch returned None for {course_name}.")
            except Exception as fetch_exc: logger.error(f"Exception during fetch (Attempt {attempt+1}) for {course_name}: {fetch_exc}", exc_info=(attempt == max_retries - 1))
        if fetch_success: break
        if attempt < max_retries - 1: await asyncio.sleep(RETRY_DELAY_SECONDS)

    if not fetch_success or not combined_data_for_cache:
        return {"status": "failed_fetch_retries", "reason": "fetch failed or no data post-retries"}

    timeout, is_pickle_type = REFRESH_CONFIG["cms_content"]["timeout"], REFRESH_CONFIG["cms_content"]["is_pickle"]
    try:
        new_canonical_bytes = _get_canonical_bytes_for_hashing(combined_data_for_cache, is_pickle_type)
        new_hash_string = _generate_data_hash(new_canonical_bytes)
    except Exception as e_hash_new: return {"status": "failed_hash_new_data", "reason": f"error hashing new data: {e_hash_new}"}

    old_data_object, old_hash_string = get_data_and_stored_hash(key_base, is_pickle_type)
    new_data_is_substantial = _is_cms_content_substantial(combined_data_for_cache)

    if not new_data_is_substantial:
        logger.warning(f"Newly fetched CMS content for {course_name} ({key_base}) is not substantial.")
        if old_data_object and _is_cms_content_substantial(old_data_object):
            logger.info(f"Preserving existing substantial CMS content for {key_base}. Refreshing TTL.")
            expire_keys_pipelined(cms_pipeline, key_base, timeout)
            return {"status": "preserved_existing_ttl_refreshed", "refreshed_url_key": key_base}
        else: logger.info(f"No existing substantial cache for {key_base}, will proceed with new minimal data if changed.")

    if old_hash_string == new_hash_string:
        logger.info(f"CMS content for {key_base} has not changed (hash match). Refreshing TTL.")
        expire_keys_pipelined(cms_pipeline, key_base, timeout)
        return {"status": "unchanged_ttl_refreshed", "refreshed_url_key": key_base}
    else:
        logger.info(f"CMS content for {key_base} is new or changed. Adding to batch update.")
        set_data_and_hash_pipelined(cms_pipeline, key_base, combined_data_for_cache, new_hash_string, new_canonical_bytes, timeout, is_pickle_type)
        status_msg = "updated_batched" if new_data_is_substantial else "updated_minimal_batched"
        return {"status": status_msg, "refreshed_url_key": key_base}


async def run_refresh_for_user(username, password, data_types_to_run, refreshed_global_cms_keys_this_run: set, json_pipeline, cms_pipeline):
    user_results, max_concurrent_fetches = {}, config.MAX_CONCURRENT_FETCHES
    current_run_all_update_messages = []
    logger.info(f"Processing user: {username} for data types: {data_types_to_run}")

    if "cms_content" in data_types_to_run:
        # ... (CMS content handling remains unchanged from previous complete version) ...
        logger.info(f"Processing CMS content section for user: {username}")
        course_list_for_user = None
        try: course_list_for_user = await asyncio.get_running_loop().run_in_executor(None, scrape_cms_courses, username, password)
        except Exception as e: user_results["cms_content"] = f"failed: user course list fetch error: {e}"
        
        if isinstance(course_list_for_user, list) and course_list_for_user:
            cms_tasks_for_this_user, semaphore_cms = [], asyncio.Semaphore(max_concurrent_fetches)
            async def fetch_cms_with_semaphore(coro):
                async with semaphore_cms: return await coro
            for course_entry in course_list_for_user:
                course_url = course_entry.get("course_url")
                if not course_url: continue
                normalized_course_url = normalize_course_url(course_url)
                if not normalized_course_url: continue
                cms_key_base = generate_cms_content_key_base(normalized_course_url)
                if not cms_key_base or cms_key_base in refreshed_global_cms_keys_this_run: continue
                task_coro = _refresh_single_cms_course(username, password, course_entry, cms_pipeline)
                cms_tasks_for_this_user.append(asyncio.create_task(fetch_cms_with_semaphore(task_coro)))
            
            cms_op_summary = {"initiated_update": 0, "initiated_ttl_refresh": 0, "failed_initiation": 0, "skipped_no_url_norm": 0}
            if cms_tasks_for_this_user:
                course_initiation_results = await asyncio.gather(*cms_tasks_for_this_user, return_exceptions=True)
                for res in course_initiation_results:
                    if isinstance(res, Exception): cms_op_summary["failed_initiation"] += 1
                    elif isinstance(res, dict):
                        status, refreshed_key = res.get("status", "unknown_init_status"), res.get("refreshed_url_key")
                        if refreshed_key: refreshed_global_cms_keys_this_run.add(refreshed_key)
                        if status in ["updated_batched", "updated_minimal_batched"]: cms_op_summary["initiated_update"] +=1
                        elif status in ["unchanged_ttl_refreshed", "preserved_existing_ttl_refreshed"]: cms_op_summary["initiated_ttl_refresh"] +=1
                        elif status in ["skipped_no_url", "skipped_norm_fail"]: cms_op_summary["skipped_no_url_norm"] +=1
                        else: cms_op_summary["failed_initiation"] +=1
            user_results["cms_content"] = (f"Courses: {len(course_list_for_user) if course_list_for_user else 'Err/0'}. "
                                           f"Upd: {cms_op_summary['initiated_update']}, TTLs: {cms_op_summary['initiated_ttl_refresh']}, "
                                           f"Fails: {cms_op_summary['failed_initiation']}, Skip: {cms_op_summary['skipped_no_url_norm']}")
        elif isinstance(course_list_for_user, dict) and "error" in course_list_for_user: user_results["cms_content"] = f"failed: user course list error - {course_list_for_user['error']}"
        else: user_results["cms_content"] = "skipped: no courses or list error"
        data_types_to_run = [dt for dt in data_types_to_run if dt != "cms_content"]
        if not data_types_to_run: return user_results


    other_tasks, data_type_map, semaphore_other = [], {}, asyncio.Semaphore(max_concurrent_fetches)
    async def run_scrape_with_semaphore(func, args, dt_str):
        async with semaphore_other: 
            logger.debug(f"Starting scrape task for {username} - {dt_str}")
            res = await asyncio.get_running_loop().run_in_executor(None, func, *args)
            logger.debug(f"Finished scrape task for {username} - {dt_str}")
            return res
            
    for dt_str in data_types_to_run:
        if dt_str in REFRESH_CONFIG:
            cfg = REFRESH_CONFIG[dt_str]
            if cfg.get("func"):
                task = asyncio.create_task(run_scrape_with_semaphore(cfg["func"], [username, password] + cfg["args"], dt_str), name=f"{username}_{dt_str}")
                other_tasks.append(task); data_type_map[task] = dt_str
            else: user_results[dt_str] = "skipped: no function"
        else: user_results[dt_str] = "skipped: unknown type"

    if other_tasks:
        results_list = await asyncio.gather(*other_tasks, return_exceptions=True)
        for i, res_exc in enumerate(results_list):
            task, dt_str = other_tasks[i], data_type_map.get(other_tasks[i], f"unk_{i}")
            cfg = REFRESH_CONFIG.get(dt_str)
            if not cfg: user_results[dt_str] = "failed: internal config error"; logger.error(f"Internal: No REFRESH_CONFIG for {dt_str}"); continue
            
            fail, err_msg, scraped_obj = False, None, None
            if isinstance(res_exc, Exception): fail, err_msg = True, f"failed: Task exc - {res_exc!s}" ; logger.error(f"Task {task.get_name()} exc: {res_exc!s}", exc_info=False) # Keep exc_info False for brevity here
            elif isinstance(res_exc, dict) and "error" in res_exc: fail, err_msg = True, f"skipped: {res_exc['error']}"; logger.warning(f"Task {task.get_name()} returned error: {err_msg}")
            elif res_exc is None: fail, err_msg = True, "skipped: scraper returned None"; logger.warning(f"Task {task.get_name()} returned None")
            else: scraped_obj = res_exc
            if fail: user_results[dt_str] = err_msg; continue

            key_base, timeout, cmp_func, is_pkl = generate_cache_key(cfg["cache_prefix"], username), cfg["timeout"], cfg.get("compare_func"), cfg["is_pickle"]
            
            if dt_str == "schedule":
                try:
                    filtered = filter_schedule_details(scraped_obj)
                    scraped_obj = [] if is_schedule_empty(filtered) else (filtered, {"0":"8:15AM-9:45AM", "1":"10:00AM-11:30AM", "2":"11:45AM-1:15PM", "3":"1:45PM-3:15PM", "4":"3:45PM-5:15PM"})
                except Exception as e_f: user_results[dt_str] = f"failed: filter error: {e_f!s}"; logger.error(f"Filter error for {task.get_name()}: {e_f!s}"); continue
            
            # For grades, if it's a dict (as per user's JSON), OPT_SORT_KEYS will handle it.
            # No explicit list sorting needed here unless scrape_grades changes its output structure.
            if dt_str == "grades":
                logger.debug(f"Grades data for {key_base} (type: {type(scraped_obj)}) will rely on OPT_SORT_KEYS for dictionary key sorting during hashing.")
                # The critical fix for grades consistency (e.g. "NO_QUIZ_ASSIGN_NAME::discussion 1::0" vs "NO_QUIZ_ASSIGN_NAME::bonus::0")
                # must happen in scrape_grades.py to ensure the *keys* themselves are stable.

            try:
                new_canon_bytes = _get_canonical_bytes_for_hashing(scraped_obj, is_pkl)
                new_hash_str = _generate_data_hash(new_canon_bytes)
            except Exception as e_h: user_results[dt_str] = f"failed: hash new data error: {e_h!s}"; logger.error(f"Hash new data error for {key_base}: {e_h!s}"); continue
                
            old_data_obj, old_hash_str = get_data_and_stored_hash(key_base, is_pkl)

            if dt_str == "grades": # Enhanced Debugging for Grades
                logger.info(f"--- GRADES HASH DEBUG for {key_base} ---")
                logger.info(f"  New Scraped Object type: {type(scraped_obj)}")
                if isinstance(scraped_obj, dict) and "detailed_grades" in scraped_obj and isinstance(scraped_obj["detailed_grades"], dict):
                    num_courses = len(scraped_obj["detailed_grades"])
                    logger.info(f"  'detailed_grades' dictionary has {num_courses} courses (keys).")
                    # Example: log keys of the first course's detailed grades if structure matches
                    # first_course_name = next(iter(scraped_obj["detailed_grades"]), None)
                    # if first_course_name and isinstance(scraped_obj["detailed_grades"][first_course_name], dict):
                    #     logger.info(f"    Item keys for first course '{first_course_name}': {list(scraped_obj['detailed_grades'][first_course_name].keys())[:5]}")
                logger.info(f"  New Hash: {new_hash_str}")
                logger.info(f"  Old Hash: {old_hash_str}")
                if old_hash_str and old_hash_str != new_hash_str: # Only decode and compare strings if hashes differ
                    new_canon_str_dbg_cmp = new_canon_bytes.decode('utf-8', 'replace')
                    old_data_key_bytes = f"{key_base}:data".encode('utf-8')
                    old_canon_bytes_redis = redis_client.get(old_data_key_bytes) if redis_client else None
                    if old_canon_bytes_redis:
                        old_canon_str_dbg_cmp = old_canon_bytes_redis.decode('utf-8', 'replace')
                        if new_canon_str_dbg_cmp != old_canon_str_dbg_cmp:
                            logger.warning(f"  CANONICAL STRINGS DIFFER for {key_base}!")
                            len_min = min(len(new_canon_str_dbg_cmp), len(old_canon_str_dbg_cmp))
                            diff_idx = -1
                            for k_idx in range(len_min):
                                if new_canon_str_dbg_cmp[k_idx] != old_canon_str_dbg_cmp[k_idx]: diff_idx = k_idx; break
                            if diff_idx != -1:
                                ctx = 30 # Context characters around difference
                                logger.warning(f"    Diff@idx {diff_idx}:\n    New: ...{new_canon_str_dbg_cmp[max(0,diff_idx-ctx):diff_idx+ctx]}...\n    Old: ...{old_canon_str_dbg_cmp[max(0,diff_idx-ctx):diff_idx+ctx]}...")
                            elif len(new_canon_str_dbg_cmp) != len(old_canon_str_dbg_cmp): logger.warning(f"    Strings differ in length. New:{len(new_canon_str_dbg_cmp)}, Old:{len(old_canon_str_dbg_cmp)}")
                        else: logger.error(f"  HASHES DIFFER but canonical strings APPEAR identical for {key_base}. This is highly unusual (check for non-printing chars or subtle float representations).")
                    else: logger.info(f"  Old canonical data string not found in Redis for {key_base}:data (key likely expired or never set if this is not the first run).")
                elif not old_hash_str: logger.info(f"  No old hash for {key_base}. This is new data.")
                logger.info(f"--- END GRADES HASH DEBUG for {key_base} ---")

            if cmp_func and (old_hash_str != new_hash_str or not old_hash_str):
                if old_data_obj is not None:
                    try:
                        changes = cmp_func(username, old_data_obj, scraped_obj)
                        if changes and username in TARGET_NOTIFICATION_USERS:
                            for chg_item in changes:
                                if isinstance(chg_item, list) and len(chg_item) == 2: current_run_all_update_messages.append(f"[{dt_str.capitalize()}] {chg_item[1]}")
                                else: logger.warning(f"Unexpected change_item format from {dt_str} compare: {chg_item}")
                    except Exception as e_c: logger.error(f"Error in {dt_str} comparison for {username}: {e_c}", exc_info=True)
                elif not old_hash_str: logger.debug(f"First data for {key_base} ({dt_str}). No old data for notifications.")
            
            if old_hash_str == new_hash_str:
                logger.info(f"Data for {dt_str} ({key_base}) unchanged. Refreshing TTL.")
                expire_keys_pipelined(json_pipeline, key_base, timeout)
                user_results[dt_str] = "unchanged_ttl_refreshed"
            else:
                logger.info(f"Data for {dt_str} ({key_base}) new/changed. Adding to batch.")
                set_data_and_hash_pipelined(json_pipeline, key_base, scraped_obj, new_hash_str, new_canon_bytes, timeout, is_pkl)
                user_results[dt_str] = "updated_batched"

    if username in TARGET_NOTIFICATION_USERS and current_run_all_update_messages:
        notif_key = f"user_notifications_{username}"
        new_batch = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"), "messages": current_run_all_update_messages}
        existing_updates = get_json_from_cache_direct(notif_key) or []
        if not isinstance(existing_updates, list): existing_updates = []
        updated_notifs = ([new_batch] + existing_updates)[:MAX_NOTIFICATIONS_LIMIT]
        if set_json_in_cache_direct(notif_key, updated_notifs, timeout=365*24*60*60): logger.info(f"Cached {len(current_run_all_update_messages)} new updates for {username}.")
        else: logger.error(f"Failed to cache consolidated updates for {username} at {notif_key}")
    return user_results


async def main():
    start_time = datetime.now()
    logger.info(f"--- Cache Refresh Script Started: {start_time.isoformat()} ---")
    if not redis_client: logger.critical("Redis client unavailable. Aborting."); sys.exit(1)

    if len(sys.argv) < 2 or sys.argv[1] not in SECTION_MAP:
        print(f"Usage: python {sys.argv[0]} <section_number> [username]\nValid sections: {', '.join(SECTION_MAP.keys())}", file=sys.stderr); sys.exit(1)
    section, target_username = sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None
    data_types_to_run = SECTION_MAP[section]

    all_users_decrypted = get_all_stored_users_decrypted()
    if not all_users_decrypted: logger.warning("No stored users. Exiting."); return
    users_to_process = {}
    if target_username:
        pw = all_users_decrypted.get(target_username)
        if pw and pw != "DECRYPTION_ERROR": users_to_process = {target_username: pw}
        else: logger.error(f"Target user {target_username} not found or creds error."); return
    else: users_to_process = {u: p for u, p in all_users_decrypted.items() if p and p != "DECRYPTION_ERROR"}
    if not users_to_process: logger.warning("No valid users to process. Exiting."); return
    logger.info(f"Refreshing section {section} ({', '.join(data_types_to_run)}) for {len(users_to_process)} user(s).")

    json_pipeline, cms_pipeline = redis_client.pipeline(), redis_client.pipeline()
    overall_results, user_semaphore = {}, asyncio.Semaphore(config.MAX_CONCURRENT_USERS)
    refreshed_global_cms_keys = set() 

    async def process_user_with_sem(uname, pw, d_types, refreshed_cms_set, json_pipe, cms_pipe):
        async with user_semaphore: 
            return await run_refresh_for_user(uname, pw, d_types, refreshed_cms_set, json_pipe, cms_pipe)

    user_coroutines = [process_user_with_sem(u, p, list(data_types_to_run), refreshed_global_cms_keys, json_pipeline, cms_pipeline) for u,p in users_to_process.items()]
    results_list = await asyncio.gather(*user_coroutines, return_exceptions=True)

    pipeline_errors = False
    logger.info("Executing JSON data pipeline...")
    try: json_exec_res = json_pipeline.execute(); logger.info(f"JSON pipeline executed. Results count: {len(json_exec_res if json_exec_res else [])}")
    except redis.exceptions.RedisError as e: logger.error(f"CRITICAL: JSON Pipeline Error: {e}", exc_info=True); pipeline_errors = True
    except Exception as e_json_pipe: logger.error(f"CRITICAL: Generic JSON Pipeline Error: {e_json_pipe}", exc_info=True); pipeline_errors = True
    
    logger.info("Executing CMS (Pickle) data pipeline...")
    try: cms_exec_res = cms_pipeline.execute(); logger.info(f"CMS pipeline executed. Results count: {len(cms_exec_res if cms_exec_res else [])}")
    except redis.exceptions.RedisError as e: logger.error(f"CRITICAL: CMS Pipeline Error: {e}", exc_info=True); pipeline_errors = True
    except Exception as e_cms_pipe: logger.error(f"CRITICAL: Generic CMS Pipeline Error: {e_cms_pipe}", exc_info=True); pipeline_errors = True

    if pipeline_errors: logger.error("One or more Redis pipelines failed. Cached data may be inconsistent.")

    processed_usernames = list(users_to_process.keys())
    for i, user_res_exc in enumerate(results_list):
        uname = processed_usernames[i]
        if isinstance(user_res_exc, Exception): overall_results[uname] = {"error": f"User task group failed: {user_res_exc}"}; logger.error(f"User task for {uname} failed: {user_res_exc!r}", exc_info=True)
        elif isinstance(user_res_exc, dict): overall_results[uname] = user_res_exc
        else: overall_results[uname] = {"error": f"User task unexpected type: {type(user_res_exc)}"}

    logger.info("--- Cache Refresh Summary ---")
    stats = {"updated_batched": 0, "unchanged_ttl_refreshed": 0, "skipped": 0, "failed": 0, "preserved_existing_ttl_refreshed":0}
    for uname, res_dict in overall_results.items():
        user_summary = []
        if res_dict.get("error"):
            user_summary.append(f"USER_ERROR: {res_dict['error']}")
            stats["failed"] += len([dt for dt in data_types_to_run if dt != "cms_content"])
        else:
            for dt, status_str in res_dict.items():
                user_summary.append(f"{dt}: {status_str}")
                if dt != "cms_content":
                    if "updated_batched" in status_str: stats["updated_batched"] += 1
                    elif "unchanged_ttl_refreshed" in status_str: stats["unchanged_ttl_refreshed"] += 1
                    elif "preserved_existing_ttl_refreshed" in status_str : stats["preserved_existing_ttl_refreshed"] +=1
                    elif "skipped" in status_str: stats["skipped"] += 1
                    elif "failed" in status_str: stats["failed"] += 1
                    else: logger.warning(f"Unknown status '{status_str}' for {dt} of {uname}"); stats["failed"] +=1
        logger.info(f"User: {uname} -> {'; '.join(user_summary)}")

    end_time, duration = datetime.now(), datetime.now() - start_time
    logger.info(f"--- Cache Refresh Script Finished: {end_time.isoformat()} (Duration: {duration}) ---")
    if "cms_content" in data_types_to_run: logger.info(f"CMS Content Global Summary: Unique course keys processed/initiated = {len(refreshed_global_cms_keys)}.")
    logger.info(f"Overall Items Summary (non-CMS global): Updated (Batched): {stats['updated_batched']}, Unchanged (TTL Refreshed): {stats['unchanged_ttl_refreshed']}, Skipped: {stats['skipped']}, Failed: {stats['failed']}")
    if pipeline_errors: logger.critical("PIPELINE EXECUTION ERRORS OCCURRED. Review logs immediately.")

if __name__ == "__main__":
    if not hasattr(config, "NOTIFICATION_ENABLED_USERS"): config.NOTIFICATION_ENABLED_USERS = "ALL"
    if not hasattr(config, "MAX_CONCURRENT_FETCHES"): config.MAX_CONCURRENT_FETCHES = 5
    if not hasattr(config, "MAX_CONCURRENT_USERS"): config.MAX_CONCURRENT_USERS = 10
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Refresh script interrupted.")
    except Exception as e: logger.critical(f"Critical script error: {e}", exc_info=True); sys.exit(1)