# utils/log.py
import logging
import json
import threading
import concurrent.futures
import sys
import atexit
from time import perf_counter
from datetime import datetime, timezone
from flask import g, request # For accessing g and request context

from config import config
# Use the Redis client initialized in cache.py for consistency
try:
    from .cache import redis_client
    if not redis_client:
        logging.warning("Redis client from utils.cache is None. Logging to Redis will fail.")
except ImportError:
    logging.critical("Could not import redis_client from utils.cache. Logging may fail.")
    redis_client = None

logger = logging.getLogger(__name__) # Use module-specific logger

# --- Logging Constants ---
API_LOG_KEY = config.API_LOG_KEY
MAX_LOG_ENTRIES = config.MAX_LOG_ENTRIES

# --- Thread Pool for Background Logging ---
log_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5, thread_name_prefix='LogThread')

def _log_to_redis_task(log_entry_dict):
    """Internal task to write logs to Redis asynchronously."""
    if not redis_client:
        # Fallback to standard logger if Redis is down
        logger.warning(f"Redis unavailable for logging. Stdout log: {log_entry_dict}")
        return
    try:
        # Ensure all values are JSON serializable, default to string representation
        log_entry_json = json.dumps(log_entry_dict, default=str, ensure_ascii=False)
        log_entry_bytes = log_entry_json.encode('utf-8')
        log_key_bytes = API_LOG_KEY.encode('utf-8')

        # Use pipeline for atomic LPUSH and LTRIM
        pipe = redis_client.pipeline()
        pipe.lpush(log_key_bytes, log_entry_bytes)
        pipe.ltrim(log_key_bytes, 0, MAX_LOG_ENTRIES - 1)
        results = pipe.execute()
        # Optional: Check results for errors if needed
        # if not all(results): logger.error("Error in Redis logging pipeline execution.")

    except redis.exceptions.TimeoutError:
        logger.error("Redis timeout during async logging.")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error during async logging: {e}")
    except TypeError as e:
        # Log the original dict for easier debugging if serialization fails
        logger.error(f"Log serialization error: {e}. Log entry: {log_entry_dict}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected async log error: {e}", exc_info=True)


def log_api_request(response):
    """
    Gathers log info from Flask's g and request/response objects
    and submits the logging task asynchronously.
    Designed to be called from Flask's @app.after_request.
    """
    # Avoid logging OPTIONS requests or specific utility paths
    if request.method == 'OPTIONS' or request.path in ['/favicon.ico', '/api/logs']: # Add paths to skip
        return response # Return original response without logging

    elapsed_ms = (perf_counter() - getattr(g, 'start_time', perf_counter())) * 1000
    request_time = getattr(g, 'request_time', datetime.now(timezone.utc))

    # --- Robust User-Agent Handling ---
    ua_string_from_parsed = None
    ua_parse_error = False
    raw_ua_header = request.headers.get('User-Agent')
    try:
        if request.user_agent:
            ua_string_from_parsed = request.user_agent.string
    except Exception as e:
        ua_parse_error = True
        logger.error(f"UA Parsing Error: {e}", exc_info=False) # Keep log level lower

    final_user_agent = ua_string_from_parsed if ua_string_from_parsed else raw_ua_header if raw_ua_header else "Unknown"
    if ua_parse_error and not raw_ua_header:
        final_user_agent = "Unknown (Parsing Error)"
    # Limit UA length
    final_user_agent = final_user_agent[:250]
    # --- End User-Agent Handling ---

    # Prepare Log Entry
    username = getattr(g, 'username', None)
    outcome = getattr(g, 'log_outcome', 'unknown')
    error_message = getattr(g, 'log_error_message', None)

    # Mask sensitive data in request args/body
    request_args = {}
    try:
        request_args = request.args.to_dict()
        if 'password' in request_args:
            request_args['password'] = '********'
    except Exception: pass # Ignore errors getting args

    request_data = {}
    if request.is_json:
        try:
            # Use force=True cautiously, might raise errors on bad JSON
            request_data = request.get_json(silent=True) if request.content_length else {}
            if request_data and 'password' in request_data:
                request_data['password'] = '********'
        except Exception as e:
            logger.warning(f"Could not parse request JSON for logging: {e}")
            request_data = {"error": "Could not parse JSON body"}
    elif request.form:
         request_data = request.form.to_dict()
         if 'password' in request_data:
                request_data['password'] = '********'

    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr) or "Unknown"

    log_entry = {
        "endpoint": request.path,
        "method": request.method,
        "status_code": response.status_code,
        "username": username,
        "outcome": outcome,
        "error_message": error_message,
        "time_elapsed_ms": round(elapsed_ms, 2),
        "request_timestamp_utc": request_time.isoformat(),
        "response_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "ip_address": ip_address,
        "user_agent": final_user_agent,
        "request_args": request_args or None, # Use None if empty
        "request_data": request_data or None, # Use None if empty
        "response_size_bytes": response.content_length,
    }

    # Submit logging task
    try:
        log_executor.submit(_log_to_redis_task, log_entry)
    except Exception as e:
        logger.exception(f"CRITICAL: Failed to submit log task to executor: {e}")

    return response # Return the original response


def setup_logging():
    """Configures the root logger."""
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s",
        # filename='app.log', # Uncomment to log to a file
        # filemode='a'
    )
    # Silence excessively verbose libraries if needed
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING) # Example: Pillow library

    logger.info(f"Logging configured with level {config.LOG_LEVEL}")


def shutdown_log_executor(wait=True):
    """Shuts down the background logging thread pool."""
    logger.info(f"Attempting to shut down log executor (wait={wait})...")
    log_executor.shutdown(wait=wait)
    logger.info("Log executor shut down complete.")

# Register the shutdown function to be called on exit
atexit.register(shutdown_log_executor)