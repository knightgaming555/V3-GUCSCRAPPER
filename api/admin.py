# api/admin.py
import logging
import json
import asyncio
import traceback
from flask import Blueprint, request, jsonify, g
import redis

from config import config
from scraping.guc_data import scrape_guc_data_fast
from utils.auth import (
    get_all_stored_usernames,
    get_all_stored_users_decrypted,
    delete_user_credentials,
    get_whitelist,
    set_whitelist,
    AuthError,
    get_stored_password,  # Need this to get password for refresh
)
from utils.cache import (
    redis_client,
    delete_from_cache,
    generate_cache_key,
    set_in_cache,
)
from utils.helpers import get_country_from_ip  # For stats potentially
from utils.log import API_LOG_KEY, MAX_LOG_ENTRIES  # Import log constants

# Import *specific* scraping functions needed for targeted refresh
# This avoids importing the entire scraping module if not needed
try:
    from scraping import (
        scrape_guc_data,
        scrape_schedule,
        scrape_cms_courses,
        scrape_grades,
        scrape_attendance,
        scrape_exam_seats,
        scrape_course_content,
        scrape_course_announcements,  # For potential CMS content refresh
        filter_schedule_details,  # Needed for schedule caching structure
    )

    # If cms_scraper is still used for combined course data fetching:
    from scraping.cms import cms_scraper as scrape_combined_cms
except ImportError:
    logging.critical(
        "Failed to import scraping functions for admin refresh.", exc_info=True
    )
    # Define dummies or raise error? Raising error is safer.
    raise ImportError("Admin blueprint requires scraping functions.")


logger = logging.getLogger(__name__)
admin_bp = Blueprint("admin_bp", __name__)


# --- Helper: Admin Authentication ---
def check_admin_secret():
    """Checks if the correct admin secret is provided in headers or args."""
    secret = request.headers.get("Admin-Secret") or request.args.get("secret")
    if not secret or secret != config.ADMIN_SECRET:
        g.log_outcome = "auth_error_admin"
        g.log_error_message = "Missing or invalid admin secret"
        return False
    return True


# --- Helper: Get Decrypted Password ---
def _get_decrypted_password_for_user(username):
    """Safely retrieves and decrypts password. Returns None on error."""
    try:
        return get_stored_password(username)
    except Exception as e:
        logger.error(
            f"Failed to get/decrypt password for {username} during admin operation: {e}"
        )
        return None


# --- Admin Endpoints ---


@admin_bp.route("/admin/status", methods=["GET"])
def admin_status():
    """Basic status check for the admin endpoint."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "success"
    return jsonify({"status": "ok", "message": "Admin endpoint is active."}), 200


@admin_bp.route("/admin/config", methods=["GET"])
def admin_config_view():
    """View core configuration details."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "success"

    stored_usernames = get_all_stored_usernames()
    whitelist = get_whitelist()

    try:
        # Use string client for these keys
        str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        version_num_raw = str_redis_client.get("VERSION_NUMBER")
        version_num = version_num_raw if version_num_raw else "Not Set"
    except Exception as e:
        logger.error(f"Failed to get version number from Redis: {e}")
        version_num = "Error Fetching"

    config_data = {
        "api_version": version_num,
        "whitelist_enabled": bool(whitelist),  # Check if whitelist is non-empty
        "whitelisted_users": whitelist,
        "stored_user_count": len(stored_usernames),
        "stored_usernames": stored_usernames,
        # Add other relevant config values read from config object
        "guc_index_url": config.GUC_INDEX_URL,
        "base_schedule_url": config.BASE_SCHEDULE_URL,
        "base_attendance_url": config.BASE_ATTENDANCE_URL,
        "base_grades_url": config.BASE_GRADES_URL,
        "base_exam_seats_url": config.BASE_EXAM_SEATS_URL,
        "cms_home_url": config.CMS_HOME_URL,
        "ssl_verification": config.VERIFY_SSL,
        "cache_default_timeout": config.CACHE_DEFAULT_TIMEOUT,
        "cache_long_timeout": config.CACHE_LONG_TIMEOUT,
        "proxy_cache_expiry": config.PROXY_CACHE_EXPIRY,
        "log_level": config.LOG_LEVEL,
    }
    return jsonify(config_data), 200


@admin_bp.route("/admin/redis_info", methods=["GET"])
def admin_redis_info():
    """Return Redis INFO command output."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "success"
    if not redis_client:
        return jsonify({"error": "Redis client not available"}), 503

    try:
        # Use the raw client (decode_responses=False) for INFO
        raw_redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        redis_info_raw = raw_redis_client.info()
        # Decode bytes to string for JSON serialization
        redis_info = {
            k.decode("utf-8", "ignore"): v.decode("utf-8", "ignore")
            for k, v in redis_info_raw.items()
        }
        return jsonify(redis_info), 200
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error getting INFO: {e}")
        return jsonify({"error": f"Failed to connect to Redis: {e}"}), 503
    except Exception as e:
        logger.exception(f"Error retrieving Redis info: {e}")
        return jsonify({"error": f"Failed to retrieve Redis info: {str(e)}"}), 500


@admin_bp.route("/admin/logs", methods=["GET"])
def admin_view_logs():
    """Retrieves the last N API logs from Redis."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "success"  # Log the attempt to view logs

    if not redis_client:
        return jsonify({"error": "Redis client not available for logs"}), 503

    try:
        # Use raw client to get bytes, then decode
        raw_redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        log_entries_bytes = raw_redis_client.lrange(
            API_LOG_KEY.encode("utf-8"), 0, MAX_LOG_ENTRIES - 1
        )
        logs = []
        for entry_bytes in log_entries_bytes:
            try:
                entry_json = entry_bytes.decode("utf-8")
                logs.append(json.loads(entry_json))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(
                    f"Error decoding/parsing log entry from Redis: {e}. Entry preview: {entry_bytes[:100]!r}"
                )
                logs.append(
                    {
                        "error": "Failed to parse log entry",
                        "raw_preview": repr(entry_bytes[:100]),
                    }
                )

        return jsonify(logs), 200
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Error retrieving logs from Redis (connection): {e}")
        return jsonify({"error": "Failed to connect to log storage"}), 503
    except Exception as e:
        logger.exception(f"Error retrieving logs from Redis: {e}")
        return jsonify({"error": "Failed to retrieve logs from storage"}), 500


# --- User Management ---


@admin_bp.route("/admin/users", methods=["GET"])
def admin_list_users():
    """Lists usernames with stored credentials."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "success"
    stored_usernames = get_all_stored_usernames()
    return jsonify({"usernames_with_credentials": stored_usernames}), 200


@admin_bp.route("/admin/users/<username>", methods=["DELETE"])
def admin_delete_user(username):
    """Deletes stored credentials for a specific user."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.username = username  # Log which user is being targeted
    g.log_outcome = "attempt"

    if not username:
        g.log_outcome = "validation_error"
        return jsonify({"error": "Username parameter is required"}), 400

    deleted = delete_user_credentials(username)

    if deleted:
        g.log_outcome = "success"
        # Also clear associated caches
        # Note: This is a basic pattern match, might be slow on large Redis. Consider specific keys.
        try:
            keys_to_delete = redis_client.keys(f"*:{username}:*")  # CMS content keys
            keys_to_delete.extend(redis_client.keys(f"*:{username}"))  # Standard keys
            if keys_to_delete:
                deleted_cache_count = redis_client.delete(*keys_to_delete)
                logger.info(
                    f"Deleted {deleted_cache_count} cache keys associated with user {username}."
                )
            else:
                logger.info(f"No cache keys found to delete for user {username}.")
        except Exception as e:
            logger.error(f"Error deleting cache keys for user {username}: {e}")

        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"Credentials and associated cache deleted for user: {username}",
                }
            ),
            200,
        )
    else:
        g.log_outcome = "fail_not_found"
        # Check if user existed before deciding between 404 and 500
        # delete_user_credentials logs errors, assume 404 if it returns False cleanly
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"Credentials not found or deletion failed for user: {username}",
                }
            ),
            404,
        )


# --- Whitelist Management ---


@admin_bp.route("/admin/whitelist", methods=["GET", "POST"])
def admin_manage_whitelist():
    """GET to view whitelist, POST to update it."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403

    if request.method == "GET":
        g.log_outcome = "success"
        whitelist = get_whitelist()
        return jsonify({"whitelist": whitelist}), 200

    elif request.method == "POST":
        g.log_outcome = "attempt"
        data = request.get_json()
        if (
            data is None
            or "whitelist" not in data
            or not isinstance(data["whitelist"], list)
        ):
            g.log_outcome = "validation_error"
            g.log_error_message = "Invalid request body. Expected JSON with 'whitelist' key (list of strings)."
            return (
                jsonify(
                    {
                        "error": "Invalid request body. Expected JSON: {'whitelist': ['user1', 'user2', ...]}"
                    }
                ),
                400,
            )

        new_whitelist = [
            str(user).strip() for user in data["whitelist"] if str(user).strip()
        ]  # Clean input
        success = set_whitelist(new_whitelist)

        if success:
            g.log_outcome = "success"
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Whitelist updated.",
                        "current_whitelist": new_whitelist,
                    }
                ),
                200,
            )
        else:
            g.log_outcome = "fail"
            g.log_error_message = "Failed to update whitelist in Redis."
            return jsonify({"error": "Failed to update whitelist"}), 500


# --- Dev Announcement Management ---


@admin_bp.route("/admin/dev_announcement", methods=["GET", "POST"])
def admin_dev_announcement():
    """GET to view, POST to update the developer announcement."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403

    # Re-use functions defined in api/guc.py for consistency
    # Need to import them or move them to utils
    try:
        # Assuming functions are accessible (e.g., moved to utils or imported)
        from api.guc import (
            get_dev_announcement,
            set_dev_announcement,
        )  # Adjust import if moved
    except ImportError:
        logger.error("Dev announcement functions not found.")
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Internal configuration error (announcement functions missing)",
                }
            ),
            500,
        )

    if request.method == "GET":
        g.log_outcome = "success"
        announcement = get_dev_announcement()
        return jsonify({"status": "success", "announcement": announcement}), 200

    elif request.method == "POST":
        g.log_outcome = "attempt"
        data = request.get_json()
        if not data or "announcement" not in data:
            g.log_outcome = "validation_error"
            g.log_error_message = "Missing 'announcement' in JSON body"
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Missing 'announcement' object in request body",
                    }
                ),
                400,
            )

        # Add basic validation for the announcement structure if needed
        new_announcement = data["announcement"]
        if not isinstance(new_announcement, dict) or not all(
            k in new_announcement for k in ["title", "body", "date"]
        ):
            g.log_outcome = "validation_error"
            g.log_error_message = "Invalid announcement structure"
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Invalid announcement structure. Required keys: title, body, date, etc.",
                    }
                ),
                400,
            )

        success = set_dev_announcement(new_announcement)

        if success:
            g.log_outcome = "success"
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Dev announcement updated.",
                        "current_announcement": new_announcement,
                    }
                ),
                200,
            )
        else:
            g.log_outcome = "fail"
            g.log_error_message = "Failed to update dev announcement in Redis."
            return jsonify({"error": "Failed to update dev announcement"}), 500


# --- Cache Management ---


@admin_bp.route("/admin/cache/keys", methods=["GET"])
def admin_list_cache_keys():
    """Lists cache keys matching a pattern (Use with caution!)."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "success"
    if not redis_client:
        return jsonify({"error": "Redis client not available"}), 503

    pattern = request.args.get(
        "pattern", "*:*"
    )  # Default to keys with a colon (common format)
    max_keys_str = request.args.get("limit", "1000")
    try:
        max_keys = int(max_keys_str)
    except ValueError:
        return jsonify({"error": "Invalid limit parameter, must be an integer."}), 400

    logger.warning(
        f"Admin requesting cache keys with pattern: '{pattern}' (limit: {max_keys})"
    )
    try:
        # Use SCAN for safer iteration in production compared to KEYS
        cursor = "0"
        all_keys = []
        while True:
            # Use raw client for SCAN bytes response
            raw_redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
            cursor, keys_batch_bytes = raw_redis_client.scan(
                cursor=cursor, match=pattern.encode("utf-8"), count=100
            )  # Scan in batches
            all_keys.extend([key.decode("utf-8", "ignore") for key in keys_batch_bytes])
            if len(all_keys) >= max_keys:
                logger.info(f"Reached key limit ({max_keys}) while scanning.")
                all_keys = all_keys[:max_keys]  # Truncate
                break
            if cursor == 0:  # Scan finished
                break
        logger.info(f"Found {len(all_keys)} cache keys matching '{pattern}'.")
        return (
            jsonify(
                {
                    "pattern": pattern,
                    "limit": max_keys,
                    "keys_found": len(all_keys),
                    "keys": all_keys,
                }
            ),
            200,
        )
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error listing keys: {e}")
        return jsonify({"error": f"Failed to connect to Redis: {e}"}), 503
    except Exception as e:
        logger.exception(f"Error listing cache keys with pattern '{pattern}': {e}")
        return jsonify({"error": f"Error listing keys: {e}"}), 500


@admin_bp.route("/admin/cache/delete", methods=["POST"])
def admin_delete_cache_key():
    """Deletes a specific cache key."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "attempt"

    data = request.get_json()
    key_to_delete = data.get("key") if data else None

    if not key_to_delete:
        g.log_outcome = "validation_error"
        return jsonify({"error": "Missing 'key' in JSON request body"}), 400

    logger.warning(f"Admin requesting deletion of cache key: {key_to_delete}")
    deleted_count = delete_from_cache(key_to_delete)  # Use helper

    if deleted_count > 0:
        g.log_outcome = "success"
        return (
            jsonify(
                {"status": "success", "message": f"Deleted cache key: {key_to_delete}"}
            ),
            200,
        )
    else:
        g.log_outcome = "fail_not_found"
        # Could be key didn't exist or Redis error (logged by helper)
        return (
            jsonify(
                {
                    "status": "warning",
                    "message": f"Cache key not found or deletion failed: {key_to_delete}",
                }
            ),
            404,
        )


# --- Refresh Endpoints (Placeholder - Full logic in refresh_cache.py script) ---
# These endpoints trigger the standalone script or run refresh logic directly (choose one approach)
# Option 1: Trigger external script (simpler, separates concerns, but requires script execution mechanism)
# Option 2: Run logic directly here (more complex, keeps it within API, uses asyncio)

# --- Option 2: Run Refresh Logic Directly (using asyncio) ---
# This requires importing and running the scraping functions


async def _run_refresh_task(username, password, section):
    """Async helper to run refresh logic for a single user and section."""
    tasks = []
    results = {}
    logger.info(f"Starting refresh task for user {username}, section {section}")

    # --- Section 1: guc_data, schedule ---
    if section == "1":
        # guc_data (already async)
        tasks.append(
            asyncio.create_task(
                scrape_guc_data(username, password),
                name=f"{username}_guc_data",
            )
        )
        # schedule (sync, run in thread)
        tasks.append(
            asyncio.to_thread(
                scrape_schedule, username, password, name=f"{username}_schedule"
            )
        )  # Pass name if supported

    # --- Section 2: cms (courses list or specific?), grades ---
    elif section == "2":
        # Refreshing *all* course content isn't feasible here.
        # Maybe refresh just the course list?
        tasks.append(
            asyncio.to_thread(
                scrape_cms_courses, username, password, name=f"{username}_cms_courses"
            )
        )
        # grades (sync, run in thread)
        tasks.append(
            asyncio.to_thread(
                scrape_grades, username, password, name=f"{username}_grades"
            )
        )

    # --- Section 3: attendance, exam_seats ---
    elif section == "3":
        # attendance (sync, run in thread)
        tasks.append(
            asyncio.to_thread(
                scrape_attendance, username, password, name=f"{username}_attendance"
            )
        )
        # exam_seats (sync, run in thread)
        tasks.append(
            asyncio.to_thread(
                scrape_exam_seats, username, password, name=f"{username}_exam_seats"
            )
        )

    else:
        return {"error": "Invalid section"}

    # Run tasks concurrently
    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results and update cache
    for i, result_or_exc in enumerate(task_results):
        task_name = tasks[i].get_name()  # Get name assigned above
        data_type = task_name.split("_")[-1]  # Extract type (guc_data, schedule, etc.)
        cache_key_base = data_type  # e.g., 'guc_data', 'schedule'

        if isinstance(result_or_exc, Exception):
            logger.error(
                f"Refresh task {task_name} failed: {result_or_exc}", exc_info=False
            )
            results[data_type] = f"failed: {result_or_exc}"
        elif isinstance(result_or_exc, dict) and "error" in result_or_exc:
            logger.warning(
                f"Refresh task {task_name} returned error: {result_or_exc['error']}"
            )
            results[data_type] = f"skipped: {result_or_exc['error']}"
        elif result_or_exc is None and data_type not in [
            "cms_courses"
        ]:  # Allow None for cms_courses? Check scraper return. Assume None is failure for others.
            logger.warning(f"Refresh task {task_name} returned None.")
            results[data_type] = "skipped: scraper returned None"
        else:
            # --- Success - Update Cache ---
            cache_key = generate_cache_key(cache_key_base, username)
            data_to_cache = result_or_exc
            timeout = config.CACHE_DEFAULT_TIMEOUT

            # Handle specific structuring/timeouts for certain types
            if data_type == "schedule":
                filtered = filter_schedule_details(result_or_exc)
                #Normal Timings timings = {  # Get timings from config or define here
                #    "0": "8:15AM-9:45AM",
                #    "1": "10:00AM-11:30AM",
                #    "2": "11:45AM-1:15PM",
                #    "3": "1:45PM-3:15PM",
                #    "4": "3:45PM-5:15PM",
                #}
                timings = {  # Ramadan Timings (example, adjust as needed)
                    "0": "8:30AM-9:40AM",
                    "1": "9:45AM-10:55AM",
                    "2": "11:00AM-12:10PM",
                    "3": "12:20PM-1:30PM",
                    "4": "1:35PM-2:45PM",
                }
                data_to_cache = (filtered, timings)
                timeout = config.CACHE_LONG_TIMEOUT
            elif data_type == "cms_courses":
                timeout = config.CACHE_LONG_TIMEOUT

            # Set cache
            set_success = set_in_cache(cache_key, data_to_cache, timeout=timeout)
            if set_success:
                results[data_type] = "updated"
                logger.info(f"Refreshed cache for {task_name}")
            else:
                results[data_type] = "failed: cache set error"
                logger.error(f"Failed to set cache for {task_name}")

    return results


@admin_bp.route("/admin/refresh_user/<username>/<section>", methods=["POST"])
async def admin_refresh_user(username, section):
    """Refreshes cache for a specific user and section."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.username = username  # Log target user
    g.log_outcome = "attempt"

    if section not in ["1", "2", "3"]:
        g.log_outcome = "validation_error"
        g.log_error_message = f"Invalid section: {section}"
        return jsonify({"error": "Invalid section. Use '1', '2', or '3'."}), 400

    password = _get_decrypted_password_for_user(username)
    if not password:
        g.log_outcome = "fail_no_user_pw"
        g.log_error_message = f"Could not get password for user {username}"
        return (
            jsonify(
                {"error": f"Could not find or decrypt password for user: {username}"}
            ),
            404,
        )

    logger.info(f"Admin trigger: Refreshing section {section} for user {username}")
    try:
        refresh_results = await _run_refresh_task(username, password, section)
        g.log_outcome = "success"  # Assume partial success if task runs
        return (
            jsonify(
                {
                    "status": "done",
                    "username": username,
                    "section": section,
                    "results": refresh_results,
                }
            ),
            200,
        )
    except Exception as e:
        logger.exception(
            f"Error running refresh task for user {username}, section {section}: {e}"
        )
        g.log_outcome = "fail"
        g.log_error_message = f"Refresh task execution failed: {e}"
        return jsonify({"status": "error", "message": f"Refresh task failed: {e}"}), 500


@admin_bp.route("/admin/refresh_all/<section>", methods=["POST"])
async def admin_refresh_all(section):
    """Refreshes cache for ALL users for a specific section."""
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "attempt"

    if section not in ["1", "2", "3"]:
        g.log_outcome = "validation_error"
        g.log_error_message = f"Invalid section: {section}"
        return jsonify({"error": "Invalid section. Use '1', '2', or '3'."}), 400

    logger.warning(f"Admin trigger: Refreshing section {section} for ALL users.")
    stored_users = get_all_stored_users_decrypted()  # Get user:pass map

    if not stored_users:
        g.log_outcome = "fail_no_users"
        return (
            jsonify(
                {"status": "warning", "message": "No stored users found to refresh."}
            ),
            404,
        )

    # Run refresh tasks for all users concurrently
    all_results = {}
    async_tasks = []
    for username, password in stored_users.items():
        if password == "DECRYPTION_ERROR":
            all_results[username] = {"error": "Decryption failed"}
            continue
        async_tasks.append(_run_refresh_task(username, password, section))

    # Gather results from all user refresh tasks
    results_list = await asyncio.gather(*async_tasks, return_exceptions=True)

    # Map results back to usernames
    usernames_processed = [
        u for u, p in stored_users.items() if p != "DECRYPTION_ERROR"
    ]
    for i, user_result in enumerate(results_list):
        username = usernames_processed[i]
        if isinstance(user_result, Exception):
            all_results[username] = {"error": f"Task exception: {user_result}"}
            logger.error(
                f"Refresh task for user {username} raised exception: {user_result}"
            )
        else:
            all_results[username] = user_result

    logger.info(
        f"Finished refresh_all for section {section}. Processed {len(stored_users)} users."
    )
    g.log_outcome = (
        "success"  # Mark overall attempt as success (individual errors are in results)
    )
    return jsonify({"status": "done", "section": section, "results": all_results}), 200
