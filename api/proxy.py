# api/proxy.py
import logging
from io import BytesIO
from flask import Blueprint, request, jsonify, g, Response, stream_with_context
import redis
from werkzeug.http import parse_range_header
import time  # Added for timing
import base64  # Needed for PyPDF2 checks maybe
import concurrent.futures
import threading
import requests
from requests_ntlm import HttpNtlmAuth  # Keep NTLM auth
import os
import PyPDF2  # Keep for extraction
import json  # Keep for JSON logging/responses

# Keep these imports if needed by extraction functions
try:
    import docx
except ImportError:
    docx = None
try:
    from pptx import Presentation
except ImportError:
    Presentation = None
try:
    from pdfminer.high_level import extract_text as pdfminer_extract
except ImportError:
    pdfminer_extract = None

from config import config
from utils.auth import validate_credentials_flow, AuthError

# ---> Use the simple binary cache functions and correct cache key generator <---
from utils.cache import (
    generate_cache_key,
    save_binary_simple,
    get_binary_simple,
    get_from_cache,
    set_in_cache,
)  # Also need standard cache for /extract
from utils.helpers import guess_content_type

# Import file fetching and extraction from scraping module
from scraping.files import fetch_file_content, extract_text_from_file

# Import session creation for HEAD request and original proxy download session
from scraping.core import create_session, make_request

logger = logging.getLogger(__name__)
proxy_bp = Blueprint("proxy_bp", __name__)

CACHE_PREFIX_PROXY = "proxy_file"  # Cache prefix for proxied file content
CACHE_PREFIX_EXTRACT = "extract_text"  # Cache prefix for extracted text

# --- Logging setup from original file (keep if not handled globally) ---
# This might be redundant if app.py sets up logging, but safe to keep if running standalone
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
# )

# --- Background logging task and executor (keep from original) ---
API_LOG_KEY = "api_logs"  # Specific Redis key for proxy/extractor logs
MAX_LOG_ENTRIES = 5000
log_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=5, thread_name_prefix="LogThread"
)
# Use the redis client from utils.cache
from utils.cache import redis_client


def _log_to_redis_task(log_entry_dict):
    """Internal task to write logs to Redis asynchronously."""
    if not redis_client:
        # Use logger if Redis fails
        logger.warning(f"Redis unavailable for logging. Stdout log: {log_entry_dict}")
        return
    try:
        # Use string client for logging
        str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        log_entry_json = json.dumps(log_entry_dict, default=str)
        pipe = str_redis_client.pipeline()
        pipe.lpush(API_LOG_KEY, log_entry_json)
        pipe.ltrim(API_LOG_KEY, 0, MAX_LOG_ENTRIES - 1)
        pipe.execute()
    except redis.exceptions.ConnectionError as e:
        logger.error(
            f"[{threading.current_thread().name}] Log Error: Redis connection error: {e}"
        )
    except TypeError as e:
        logger.error(
            f"[{threading.current_thread().name}] Log Error: Failed to serialize log entry to JSON: {e}",
            exc_info=True,
        )
    except Exception as e:
        logger.error(
            f"[{threading.current_thread().name}] Log Error: Failed to write log to Redis: {e}",
            exc_info=True,
        )


# --- Session from original file (keep if scraping.core doesn't provide suitable one) ---
# Or better: Ensure scraping.core.create_session is sufficient
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=10, pool_maxsize=20, max_retries=3, pool_block=False
)
session.mount("http://", adapter)
session.mount("https://", adapter)

# --- Request Hooks (keep from original file - should be integrated in app.py) ---
# These might be redundant if app.py handles before/after request globally
# Commenting out for now, assuming app.py handles it
# @app.before_request ...
# @app.after_request def after_request_logger(response): ...
# @app.after_request def add_cors_headers(response): ...


# --- Helper for Streaming ---
def generate_chunks(content: bytes, chunk_size: int = config.PROXY_CHUNK_SIZE):
    """Yields chunks of byte content."""
    logger.debug(
        f"Generating chunks of size {chunk_size} from content length {len(content)}"
    )
    bytes_yielded = 0
    try:
        for i in range(0, len(content), chunk_size):
            chunk = content[i : i + chunk_size]
            yield chunk
            bytes_yielded += len(chunk)
        logger.debug(
            f"Finished generating chunks. Total bytes yielded: {bytes_yielded}"
        )
    except GeneratorExit:
        logger.warning(
            "Chunk generator exited prematurely (client likely disconnected)."
        )
    except Exception as e:
        logger.error(f"Error during chunk generation: {e}", exc_info=True)


# --- API Endpoints ---


@proxy_bp.route("/file-info", methods=["GET"])
def file_info():
    """
    Endpoint to get file metadata (size, type, etc.) using a HEAD request.
    Requires query params: username, password, fileUrl
    """
    username = request.args.get("username")
    password = request.args.get("password")
    file_url = request.args.get("fileUrl")
    g.username = username  # Set for logging

    # Validation moved inside try block for cleaner AuthError handling
    try:
        if not file_url:
            raise AuthError("Missing fileUrl parameter", 400, "validation_error")

        # Use validate_credentials_flow for consistency, create session separately
        password_to_use = validate_credentials_flow(username, password)
        # Use create_session from scraping.core for consistency
        head_session = create_session(username, password_to_use)

        logger.info(f"Fetching HEAD for file info: {file_url}")
        # Use make_request from scraping.core
        response = make_request(head_session, file_url, method="HEAD", timeout=(10, 15))

        if not response:
            g.log_outcome = "fetch_error_head"
            g.log_error_message = f"HEAD request failed for {file_url}"
            return (
                jsonify(
                    {"error": f"Failed to get file info from server for {file_url}"}
                ),
                502,
            )

        content_type = response.headers.get("Content-Type", "application/octet-stream")
        content_length_str = response.headers.get("Content-Length")
        last_modified = response.headers.get("Last-Modified")
        accept_ranges = response.headers.get("Accept-Ranges", "none")
        filename = file_url.split("/")[-1].split("?")[0] or "unknown_file"

        g.log_outcome = "success"
        logger.info(f"Successfully retrieved file info for: {file_url}")
        return jsonify(
            {
                "contentType": content_type,
                "contentLength": (
                    content_length_str if content_length_str else "unknown"
                ),
                "lastModified": last_modified if last_modified else "unknown",
                "filename": filename,
                "acceptRanges": accept_ranges,
            }
        )

    except AuthError as e:
        logger.warning(
            f"AuthError during file info request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(f"Unexpected error getting file info for {file_url}: {e}")
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unexpected error during file info: {e}"
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


@proxy_bp.route("/proxy", methods=["GET"])
def proxy_file():
    """
    Proxies a file download from an NTLM-protected source.
    Uses simple Base64 caching. Supports basic Range requests from cache.
    Requires query params: username, password, fileUrl
    """
    req_start_time = time.perf_counter()  # Start timing req processing

    if request.args.get("bot", "").lower() == "true":  # Bot check
        # ... (same as before) ...
        logger.info("Received bot health check request for Proxy API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {"status": "Success", "message": "Proxy API route is up!", "data": None}
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    file_url = request.args.get("fileUrl")
    g.username = username

    try:
        if not file_url:
            raise AuthError("Missing fileUrl parameter", 400, "validation_error")

        auth_start = time.perf_counter()
        password_to_use = validate_credentials_flow(username, password)
        auth_duration = (time.perf_counter() - auth_start) * 1000
        logger.info(f"TIMING: Proxy Auth flow took {auth_duration:.2f} ms")

        cache_key = generate_cache_key(CACHE_PREFIX_PROXY, username, file_url)
        file_name = file_url.split("/")[-1].split("?")[0] or "downloaded_file"
        content_type = guess_content_type(file_name)

        # --- Range Request Handling ---
        range_header = request.headers.get("Range")
        start, end = None, None
        is_range_request = False
        if range_header:
            try:
                range_obj = parse_range_header(range_header)
                if (
                    range_obj
                    and range_obj.units == "bytes"
                    and len(range_obj.ranges) == 1
                ):
                    start, end = range_obj.ranges[0]
                    is_range_request = True
                    logger.info(
                        f"Range requested: bytes={start}-{end if end is not None else ''}"
                    )
                else:
                    logger.warning(f"Unsupported range header format: {range_header}")
            except ValueError:
                logger.warning(f"Invalid range header: {range_header}")

        # --- Cache Check ---
        cache_check_start = time.perf_counter()
        cached_content = get_binary_simple(cache_key)
        cache_check_duration = (time.perf_counter() - cache_check_start) * 1000
        logger.info(
            f"TIMING: Proxy Redis cache check took {cache_check_duration:.2f} ms"
        )

        if cached_content:
            logger.info(f"Proxy cache hit for: {file_url}")
            g.log_outcome = "cache_hit"
            total_size = len(cached_content)

            if (
                is_range_request and start is not None
            ):  # Process range request from cache
                if end is None or end >= total_size:
                    end = total_size - 1
                if (
                    start >= total_size
                    or start < 0
                    or (end is not None and start > end)
                ):
                    logger.warning(
                        f"Invalid range {start}-{end} for cached size {total_size}"
                    )
                    return Response(
                        "Range Not Satisfiable",
                        status=416,
                        headers={"Content-Range": f"bytes */{total_size}"},
                    )

                content_to_serve = cached_content[start : end + 1]
                resp_length = len(content_to_serve)
                headers = {
                    "Content-Type": content_type,
                    "Content-Length": str(resp_length),
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                    "Cache-Control": "public, max-age=86400",
                    "X-Source": "redis-cache (partial)",
                }
                logger.info(
                    f"Serving partial content from cache ({resp_length} bytes) for {file_url}"
                )
                resp = Response(
                    stream_with_context(generate_chunks(content_to_serve)),
                    headers=headers,
                    status=206,
                )
            else:  # Serve full file from cache
                headers = {
                    "Content-Disposition": f'attachment; filename="{file_name}"',
                    "Content-Type": content_type,
                    "Content-Length": str(total_size),
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "public, max-age=86400",
                    "X-Source": "redis-cache (full)",
                }
                logger.info(
                    f"Serving full file from cache ({total_size} bytes) for {file_url}"
                )
                resp = Response(
                    stream_with_context(generate_chunks(cached_content)),
                    headers=headers,
                    status=200,
                )

            req_end_time = time.perf_counter()
            logger.info(
                f"TIMING: Proxy Cache Hit request processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
            )
            return resp

        # --- Cache Miss -> Fetch and Stream ---
        logger.info(f"Proxy cache miss. Fetching and streaming: {file_url}")
        g.log_outcome = "fetch_stream_attempt"

        fetch_start = time.perf_counter()
        file_content = fetch_file_content(username, password_to_use, file_url)
        fetch_duration = (time.perf_counter() - fetch_start) * 1000
        logger.info(f"TIMING: Proxy file fetch took {fetch_duration:.2f} ms")

        if file_content is None:
            g.log_outcome = "fetch_error"
            g.log_error_message = f"Failed to fetch file content for proxy: {file_url}"
            return (
                jsonify({"error": f"Failed to fetch file from source: {file_url}"}),
                502,
            )
        else:
            g.log_outcome = "fetch_stream_success"
            total_size = len(file_content)
            logger.info(
                f"Successfully fetched {total_size} bytes for {file_url}. Streaming to client."
            )

            # Save synchronously before starting stream
            cache_save_start = time.perf_counter()
            cache_success = save_binary_simple(
                cache_key, file_content
            )  # Use simple save
            cache_save_duration = (time.perf_counter() - cache_save_start) * 1000
            logger.info(
                f"TIMING: Proxy cache save (simple SETEX) took {cache_save_duration:.2f} ms"
            )
            if not cache_success:
                logger.warning(f"Failed to cache file content (simple) for {cache_key}")

            # --- Range handling for LIVE fetch (Optional but good practice) ---
            # If a range was requested but we had a cache miss, we fetched the WHOLE file.
            # We *could* now serve just the requested range from the fetched content.
            if is_range_request and start is not None:
                logger.info(
                    f"Serving range request from live fetch result for {file_url}"
                )
                if end is None or end >= total_size:
                    end = total_size - 1
                if (
                    start >= total_size
                    or start < 0
                    or (end is not None and start > end)
                ):
                    logger.warning(
                        f"Invalid range {start}-{end} for live fetched size {total_size}"
                    )
                    return Response(
                        "Range Not Satisfiable",
                        status=416,
                        headers={"Content-Range": f"bytes */{total_size}"},
                    )

                content_to_serve = file_content[start : end + 1]
                resp_length = len(content_to_serve)
                headers = {
                    "Content-Type": content_type,
                    "Content-Length": str(resp_length),
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                    "Cache-Control": "public, max-age=86400",
                    "X-Source": "live-fetch (partial)",
                }
                resp = Response(
                    stream_with_context(generate_chunks(content_to_serve)),
                    headers=headers,
                    status=206,
                )
            else:  # Serve full live fetch response
                headers = {
                    "Content-Disposition": f'attachment; filename="{file_name}"',
                    "Content-Type": content_type,
                    "Content-Length": str(total_size),
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "public, max-age=86400",
                    "X-Source": "live-fetch (full)",
                }
                resp = Response(
                    stream_with_context(generate_chunks(file_content)),
                    headers=headers,
                    status=200,
                )

            req_end_time = time.perf_counter()
            logger.info(
                f"TIMING: Proxy Cache Miss request processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
            )
            return resp

    except AuthError as e:
        logger.warning(
            f"AuthError during proxy request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        resp = jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/proxy for {g.username or 'unknown user'}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        resp = (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )

    # Log timing even on error paths
    req_end_time_err = time.perf_counter()
    logger.info(
        f"TIMING: Proxy Error request processed in {(req_end_time_err - req_start_time) * 1000:.2f} ms"
    )
    return resp


@proxy_bp.route("/extract", methods=["GET"])
def extract_text():
    """
    Fetches a file (PDF, DOCX, PPTX, TXT) and extracts its text content.
    Supports caching of extracted text.
    Requires query params: username, password, fileUrl
    """
    req_start_time = time.perf_counter()

    if request.args.get("bot", "").lower() == "true":  # Bot check
        # ... (same as before) ...
        logger.info("Received bot health check request for Extract API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "Extract API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    username = request.args.get("username")
    password = request.args.get("password")
    file_url = request.args.get("fileUrl")
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    g.username = username

    try:
        if not file_url:
            raise AuthError("Missing fileUrl parameter", 400, "validation_error")

        auth_start = time.perf_counter()
        password_to_use = validate_credentials_flow(username, password)
        auth_duration = (time.perf_counter() - auth_start) * 1000
        logger.info(f"TIMING: Extract Auth flow took {auth_duration:.2f} ms")

        file_name = file_url.split("/")[-1].split("?")[0] or "unknown_file"

        # --- Cache Check (for extracted text) ---
        extract_cache_key = generate_cache_key(CACHE_PREFIX_EXTRACT, username, file_url)
        if not force_refresh:
            cache_check_start = time.perf_counter()
            cached_text_data = get_from_cache(
                extract_cache_key
            )  # Standard JSON cache check
            cache_check_duration = (time.perf_counter() - cache_check_start) * 1000
            logger.info(
                f"TIMING: Extract text cache check took {cache_check_duration:.2f} ms"
            )
            if (
                cached_text_data is not None
                and isinstance(cached_text_data, dict)
                and "text" in cached_text_data
            ):
                logger.info(f"Serving extracted text from cache for: {file_url}")
                g.log_outcome = "cache_hit"
                req_end_time = time.perf_counter()
                logger.info(
                    f"TIMING: Extract Cache Hit request processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
                )
                return jsonify(cached_text_data), 200

        # --- Cache Miss -> Fetch File Content (uses binary cache) ---
        logger.info(
            f"Cache miss for extracted text. Getting file content for: {file_url}"
        )
        g.log_outcome = "fetch_attempt"

        # Check simple binary cache first
        bin_cache_check_start = time.perf_counter()
        binary_cache_key = generate_cache_key(CACHE_PREFIX_PROXY, username, file_url)
        file_content = get_binary_simple(binary_cache_key)  # Use simple getter
        bin_cache_check_duration = (time.perf_counter() - bin_cache_check_start) * 1000
        logger.info(
            f"TIMING: Extract binary cache check took {bin_cache_check_duration:.2f} ms"
        )
        source = "redis-cache"

        if not file_content:
            logger.info(f"Binary cache miss for {binary_cache_key}. Fetching live.")
            source = "live-fetch"
            fetch_start = time.perf_counter()
            file_content = fetch_file_content(username, password_to_use, file_url)
            fetch_duration = (time.perf_counter() - fetch_start) * 1000
            logger.info(f"TIMING: Extract file fetch took {fetch_duration:.2f} ms")

            if file_content:
                # Save fetched content to simple binary cache
                save_start = time.perf_counter()
                save_binary_simple(binary_cache_key, file_content)  # Use simple saver
                save_duration = (time.perf_counter() - save_start) * 1000
                logger.info(
                    f"TIMING: Extract binary cache save took {save_duration:.2f} ms"
                )

        if file_content is None:
            g.log_outcome = "fetch_error"
            g.log_error_message = (
                f"Failed to fetch file content for extraction: {file_url}"
            )
            return (
                jsonify({"error": f"Failed to fetch file from source: {file_url}"}),
                502,
            )

        # --- Extract Text ---
        extract_start = time.perf_counter()
        logger.info(
            f"Starting text extraction ({len(file_content)} bytes, source: {source}) for: {file_name}"
        )
        g.log_outcome = "extract_attempt"
        extracted_text = extract_text_from_file(file_content, file_name)
        extract_duration = (time.perf_counter() - extract_start) * 1000
        logger.info(f"TIMING: Text extraction took {extract_duration:.2f} ms")

        if extracted_text.startswith("Error:") or "Unsupported" in extracted_text:
            logger.warning(f"Text extraction failed for {file_name}: {extracted_text}")
            g.log_outcome = "extract_error"
            g.log_error_message = extracted_text
            status = 415 if "Unsupported" in extracted_text else 500
            resp = (
                jsonify({"status": "error", "message": extracted_text, "text": ""}),
                status,
            )
        else:
            g.log_outcome = "extract_success"
            logger.info(
                f"Successfully extracted text ({len(extracted_text)} chars) for: {file_name}"
            )
            result_data = {"text": extracted_text}

            # Cache the extracted text using standard JSON cache
            cache_set_start = time.perf_counter()
            set_in_cache(
                extract_cache_key, result_data, timeout=config.CACHE_LONG_TIMEOUT
            )
            cache_set_duration = (time.perf_counter() - cache_set_start) * 1000
            logger.info(
                f"TIMING: Extract text cache save took {cache_set_duration:.2f} ms"
            )
            logger.info(f"Cached extracted text for {extract_cache_key}")

            resp = jsonify(result_data), 200

        req_end_time = time.perf_counter()
        logger.info(
            f"TIMING: Extract request processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
        )
        return resp

    except AuthError as e:
        # ... (same error handling) ...
        logger.warning(
            f"AuthError during extract request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        resp = jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        # ... (same error handling) ...
        logger.exception(
            f"Unhandled exception during /api/extract for {g.username or 'unknown user'}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        resp = (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )

    # Log timing even on error paths
    req_end_time_err = time.perf_counter()
    logger.info(
        f"TIMING: Extract Error request processed in {(req_end_time_err - req_start_time) * 1000:.2f} ms"
    )
    return resp


# --- Main Execution (keep from original for standalone testing if needed) ---
# if __name__ == "__main__":
#     def shutdown_log_executor(): ...
#     atexit.register(shutdown_log_executor)
#     app.run(host="0.0.0.0", port=5000, debug=True) # Use app.debug from config?
