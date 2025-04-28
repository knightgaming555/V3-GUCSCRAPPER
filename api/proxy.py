# api/proxy.py
import logging
from io import BytesIO
from flask import Blueprint, request, jsonify, g, Response, stream_with_context
import redis
from werkzeug.http import parse_range_header
import time
import base64
import concurrent.futures
import threading
import requests
from requests_ntlm import HttpNtlmAuth
import os
import PyPDF2
import json

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
from utils.cache import (
    generate_cache_key,
    save_binary_simple,
    get_binary_simple,
    get_from_cache,
    set_in_cache,
    redis_client,  # Import redis_client from cache utils
)
from utils.helpers import guess_content_type
from scraping.files import fetch_file_content, extract_text_from_file
from scraping.core import create_session, make_request

logger = logging.getLogger(__name__)
proxy_bp = Blueprint("proxy_bp", __name__)

CACHE_PREFIX_PROXY = "proxy_file"
CACHE_PREFIX_EXTRACT = "extract_text"
API_LOG_KEY = "api_logs"
MAX_LOG_ENTRIES = 5000
log_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=5, thread_name_prefix="LogThread"
)

# ---> Define Mock User Credentials and Real Credentials to Use <---
MOCK_USERNAME = "google.user"
MOCK_PASSWORD = (
    "google@3569"  # Only used for initial check if needed by validate_credentials_flow
)
REAL_USERNAME_FOR_MOCK = "mohamed.elsaadi"
REAL_PASSWORD_FOR_MOCK = (
    "Messo-1245"  # IMPORTANT: Store this securely, e.g., env variables
)


# --- Helper Function to Get Credentials for Upstream Request ---
def get_upstream_credentials(request_username, request_password):
    """
    Determines the credentials to use for the upstream request.
    If the request uses the mock credentials, it bypasses validation
    for those and returns the predefined real credentials.
    Otherwise, it validates the incoming credentials using validate_credentials_flow.

    Returns:
        tuple[str, str]: (username_for_upstream, password_for_upstream)
    Raises:
        AuthError: If validation fails for a non-mock user.
    """
    # Step 1: Check if the incoming user is the mock user FIRST
    if request_username == MOCK_USERNAME:
        # Optional: You could add a basic check for the mock password here if desired,
        # but it's often sufficient just to check the username for mock scenarios.
        # if request_password == MOCK_PASSWORD:
        logger.info(
            f"Mock user '{MOCK_USERNAME}' detected. Bypassing validation and using real credentials for upstream request."
        )
        # Return the predefined real credentials directly
        return REAL_USERNAME_FOR_MOCK, REAL_PASSWORD_FOR_MOCK
        # else:
        #     # If you want to enforce the mock password match
        #     raise AuthError("Invalid mock password provided", 401, "mock_auth_fail")

    else:
        # Step 2: For any other user, validate their credentials as normal
        logger.info(f"Non-mock user '{request_username}'. Validating credentials.")
        # This will raise AuthError if validation fails for the real user
        password_to_use_validated = validate_credentials_flow(
            request_username, request_password
        )
        return request_username, password_to_use_validated


# --- Logging Task (remains the same) ---
def _log_to_redis_task(log_entry_dict):
    # ... (logging implementation remains the same) ...
    if not redis_client:
        logger.warning(f"Redis unavailable for logging. Stdout log: {log_entry_dict}")
        return
    try:
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


# --- Helper for Streaming (remains the same) ---
def generate_chunks(content: bytes, chunk_size: int = config.PROXY_CHUNK_SIZE):
    # ... (chunk generation remains the same) ...
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
    Uses real credentials if mock user is provided.
    """
    request_username = request.args.get("username")
    request_password = request.args.get("password")
    file_url = request.args.get("fileUrl")
    g.username = request_username  # Log the original request username

    try:
        if not file_url:
            raise AuthError("Missing fileUrl parameter", 400, "validation_error")

        # ---> Get credentials to use for the actual HEAD request <---
        username_for_upstream, password_for_upstream = get_upstream_credentials(
            request_username, request_password
        )
        # ----------------------------------------------------------

        # Use create_session with the potentially overridden credentials
        head_session = create_session(username_for_upstream, password_for_upstream)

        logger.info(
            f"Fetching HEAD for file info: {file_url} (using upstream user: {username_for_upstream})"
        )
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

        # ... (rest of the file info processing remains the same) ...
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
        # This will catch validation errors from get_upstream_credentials too
        logger.warning(
            f"AuthError during file info request for {request_username}: {e.log_message}"
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
    Proxies a file download. Uses real credentials if mock user is provided.
    Uses simple Base64 caching. Supports basic Range requests from cache.
    Requires query params: username, password, fileUrl
    """
    req_start_time = time.perf_counter()

    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for Proxy API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {"status": "Success", "message": "Proxy API route is up!", "data": None}
            ),
            200,
        )

    request_username = request.args.get("username")
    request_password = request.args.get("password")
    file_url = request.args.get("fileUrl")
    g.username = request_username  # Log original request username

    try:
        if not file_url:
            raise AuthError("Missing fileUrl parameter", 400, "validation_error")

        auth_start = time.perf_counter()
        # ---> Get credentials for potential upstream fetch <---
        username_for_upstream, password_for_upstream = get_upstream_credentials(
            request_username, request_password
        )
        auth_duration = (time.perf_counter() - auth_start) * 1000
        logger.info(
            f"TIMING: Proxy Auth flow (incl. potential override) took {auth_duration:.2f} ms"
        )
        # -----------------------------------------------------

        # ---> Cache key uses the *original* requesting user, not the upstream one <---
        # This ensures the mock user gets their own cache entry if needed,
        # separate from the real user's cache.
        cache_key = generate_cache_key(CACHE_PREFIX_PROXY, request_username, file_url)
        # ----------------------------------------------------------------------------

        file_name = file_url.split("/")[-1].split("?")[0] or "downloaded_file"
        content_type = guess_content_type(file_name)

        # --- Range Request Handling (remains the same) ---
        range_header = request.headers.get("Range")
        start, end = None, None
        is_range_request = False
        if range_header:
            # ... (range parsing logic is unchanged) ...
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
                        f"Range requested: bytes={start}-{end if end is not None else ''} for user {request_username}"
                    )
                else:
                    logger.warning(f"Unsupported range header format: {range_header}")
            except ValueError:
                logger.warning(f"Invalid range header: {range_header}")

        # --- Cache Check (using original user's cache key) ---
        cache_check_start = time.perf_counter()
        cached_content = get_binary_simple(cache_key)
        cache_check_duration = (time.perf_counter() - cache_check_start) * 1000
        logger.info(
            f"TIMING: Proxy Redis cache check for key {cache_key} took {cache_check_duration:.2f} ms"
        )

        if cached_content:
            # ... (cache hit logic, including range handling, remains the same) ...
            logger.info(f"Proxy cache hit for key: {cache_key}")
            g.log_outcome = "cache_hit"
            total_size = len(cached_content)

            if is_range_request and start is not None:
                # ... serve partial from cache ...
                if end is None or end >= total_size:
                    end = total_size - 1
                if (
                    start >= total_size
                    or start < 0
                    or (end is not None and start > end)
                ):
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
                    f"Serving partial content from cache ({resp_length} bytes) for key {cache_key}"
                )
                resp = Response(
                    stream_with_context(generate_chunks(content_to_serve)),
                    headers=headers,
                    status=206,
                )
            else:
                # ... serve full from cache ...
                headers = {
                    "Content-Disposition": f'attachment; filename="{file_name}"',
                    "Content-Type": content_type,
                    "Content-Length": str(total_size),
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "public, max-age=86400",
                    "X-Source": "redis-cache (full)",
                }
                logger.info(
                    f"Serving full file from cache ({total_size} bytes) for key {cache_key}"
                )
                resp = Response(
                    stream_with_context(generate_chunks(cached_content)),
                    headers=headers,
                    status=200,
                )

            req_end_time = time.perf_counter()
            logger.info(
                f"TIMING: Proxy Cache Hit request (user {request_username}) processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
            )
            return resp

        # --- Cache Miss -> Fetch and Stream ---
        logger.info(f"Proxy cache miss for key {cache_key}. Fetching: {file_url}")
        g.log_outcome = "fetch_stream_attempt"

        fetch_start = time.perf_counter()
        # ---> Use the potentially overridden credentials for fetching <---
        file_content = fetch_file_content(
            username_for_upstream, password_for_upstream, file_url
        )
        # -------------------------------------------------------------
        fetch_duration = (time.perf_counter() - fetch_start) * 1000
        logger.info(
            f"TIMING: Proxy file fetch (upstream user {username_for_upstream}) took {fetch_duration:.2f} ms"
        )

        if file_content is None:
            # ... (fetch error handling remains the same) ...
            g.log_outcome = "fetch_error"
            g.log_error_message = f"Failed to fetch file content for proxy: {file_url}"
            return (
                jsonify({"error": f"Failed to fetch file from source: {file_url}"}),
                502,
            )
        else:
            # ... (fetch success handling remains the same) ...
            g.log_outcome = "fetch_stream_success"
            total_size = len(file_content)
            logger.info(
                f"Successfully fetched {total_size} bytes for {file_url} (upstream user {username_for_upstream}). Streaming to client {request_username}."
            )

            # ---> Save to cache using the *original* user's cache key <---
            cache_save_start = time.perf_counter()
            cache_success = save_binary_simple(cache_key, file_content)
            cache_save_duration = (time.perf_counter() - cache_save_start) * 1000
            logger.info(
                f"TIMING: Proxy cache save to key {cache_key} took {cache_save_duration:.2f} ms"
            )
            if not cache_success:
                logger.warning(f"Failed to cache file content (simple) for {cache_key}")
            # -----------------------------------------------------------

            # --- Range handling for LIVE fetch (remains the same logic, uses fetched content) ---
            if is_range_request and start is not None:
                # ... serve partial from live fetch ...
                logger.info(
                    f"Serving range request from live fetch result for key {cache_key}"
                )
                if end is None or end >= total_size:
                    end = total_size - 1
                if (
                    start >= total_size
                    or start < 0
                    or (end is not None and start > end)
                ):
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
            else:
                # ... serve full live fetch ...
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
                f"TIMING: Proxy Cache Miss request (user {request_username}) processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
            )
            return resp

    except AuthError as e:
        logger.warning(
            f"AuthError during proxy request for {request_username}: {e.log_message}"
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

    req_end_time_err = time.perf_counter()
    logger.info(
        f"TIMING: Proxy Error request (user {request_username}) processed in {(req_end_time_err - req_start_time) * 1000:.2f} ms"
    )
    return resp


@proxy_bp.route("/extract", methods=["GET"])
def extract_text():
    """
    Fetches a file and extracts text. Uses real credentials if mock user is provided.
    Supports caching of extracted text and underlying binary file.
    Requires query params: username, password, fileUrl
    """
    req_start_time = time.perf_counter()

    if request.args.get("bot", "").lower() == "true":
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

    request_username = request.args.get("username")
    request_password = request.args.get("password")
    file_url = request.args.get("fileUrl")
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    g.username = request_username  # Log original request username

    try:
        if not file_url:
            raise AuthError("Missing fileUrl parameter", 400, "validation_error")

        auth_start = time.perf_counter()
        # ---> Get credentials for potential upstream fetch <---
        username_for_upstream, password_for_upstream = get_upstream_credentials(
            request_username, request_password
        )
        auth_duration = (time.perf_counter() - auth_start) * 1000
        logger.info(
            f"TIMING: Extract Auth flow (incl. potential override) took {auth_duration:.2f} ms"
        )
        # -----------------------------------------------------

        file_name = file_url.split("/")[-1].split("?")[0] or "unknown_file"

        # --- Cache Check (for *extracted text*, uses original user's key) ---
        extract_cache_key = generate_cache_key(
            CACHE_PREFIX_EXTRACT, request_username, file_url
        )
        if not force_refresh:
            cache_check_start = time.perf_counter()
            cached_text_data = get_from_cache(extract_cache_key)
            cache_check_duration = (time.perf_counter() - cache_check_start) * 1000
            logger.info(
                f"TIMING: Extract text cache check for key {extract_cache_key} took {cache_check_duration:.2f} ms"
            )
            if (
                cached_text_data is not None
                and isinstance(cached_text_data, dict)
                and "text" in cached_text_data
            ):
                logger.info(
                    f"Serving extracted text from cache for key: {extract_cache_key}"
                )
                g.log_outcome = "cache_hit"
                req_end_time = time.perf_counter()
                logger.info(
                    f"TIMING: Extract Cache Hit request (user {request_username}) processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
                )
                return jsonify(cached_text_data), 200

        # --- Cache Miss for Text -> Get File Content ---
        logger.info(
            f"Cache miss for extracted text ({extract_cache_key}). Getting file content for: {file_url}"
        )
        g.log_outcome = "fetch_attempt"

        # ---> Check simple binary cache (using *original* user's key) <---
        bin_cache_check_start = time.perf_counter()
        binary_cache_key = generate_cache_key(
            CACHE_PREFIX_PROXY, request_username, file_url
        )
        file_content = get_binary_simple(binary_cache_key)
        bin_cache_check_duration = (time.perf_counter() - bin_cache_check_start) * 1000
        logger.info(
            f"TIMING: Extract binary cache check for key {binary_cache_key} took {bin_cache_check_duration:.2f} ms"
        )
        source = "redis-cache"
        # --------------------------------------------------------------------

        if not file_content:
            logger.info(f"Binary cache miss for {binary_cache_key}. Fetching live.")
            source = "live-fetch"
            fetch_start = time.perf_counter()
            # ---> Use potentially overridden credentials for live fetch <---
            file_content = fetch_file_content(
                username_for_upstream, password_for_upstream, file_url
            )
            # ------------------------------------------------------------
            fetch_duration = (time.perf_counter() - fetch_start) * 1000
            logger.info(
                f"TIMING: Extract file fetch (upstream user {username_for_upstream}) took {fetch_duration:.2f} ms"
            )

            if file_content:
                # ---> Save fetched content to binary cache (using *original* user's key) <---
                save_start = time.perf_counter()
                save_binary_simple(binary_cache_key, file_content)
                save_duration = (time.perf_counter() - save_start) * 1000
                logger.info(
                    f"TIMING: Extract binary cache save to key {binary_cache_key} took {save_duration:.2f} ms"
                )
                # ---------------------------------------------------------------------------

        if file_content is None:
            g.log_outcome = "fetch_error"
            g.log_error_message = (
                f"Failed to fetch file content for extraction: {file_url}"
            )
            return (
                jsonify({"error": f"Failed to fetch file from source: {file_url}"}),
                502,
            )

        # --- Extract Text (logic remains the same) ---
        extract_start = time.perf_counter()
        logger.info(
            f"Starting text extraction ({len(file_content)} bytes, source: {source}) for: {file_name} (request user: {request_username})"
        )
        g.log_outcome = "extract_attempt"
        extracted_text = extract_text_from_file(file_content, file_name)
        extract_duration = (time.perf_counter() - extract_start) * 1000
        logger.info(f"TIMING: Text extraction took {extract_duration:.2f} ms")

        if extracted_text.startswith("Error:") or "Unsupported" in extracted_text:
            # ... (error handling for extraction remains the same) ...
            logger.warning(f"Text extraction failed for {file_name}: {extracted_text}")
            g.log_outcome = "extract_error"
            g.log_error_message = extracted_text
            status = 415 if "Unsupported" in extracted_text else 500
            resp = (
                jsonify({"status": "error", "message": extracted_text, "text": ""}),
                status,
            )
        else:
            # ... (success handling for extraction remains the same) ...
            g.log_outcome = "extract_success"
            logger.info(
                f"Successfully extracted text ({len(extracted_text)} chars) for: {file_name}"
            )
            result_data = {"text": extracted_text}

            # ---> Cache the extracted text (using *original* user's key) <---
            cache_set_start = time.perf_counter()
            set_in_cache(
                extract_cache_key, result_data, timeout=config.CACHE_LONG_TIMEOUT
            )
            cache_set_duration = (time.perf_counter() - cache_set_start) * 1000
            logger.info(
                f"TIMING: Extract text cache save to key {extract_cache_key} took {cache_set_duration:.2f} ms"
            )
            logger.info(f"Cached extracted text for {extract_cache_key}")
            # ----------------------------------------------------------------

            resp = jsonify(result_data), 200

        req_end_time = time.perf_counter()
        logger.info(
            f"TIMING: Extract request (user {request_username}) processed in {(req_end_time - req_start_time) * 1000:.2f} ms"
        )
        return resp

    except AuthError as e:
        logger.warning(
            f"AuthError during extract request for {request_username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        resp = jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
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

    req_end_time_err = time.perf_counter()
    logger.info(
        f"TIMING: Extract Error request (user {request_username}) processed in {(req_end_time_err - req_start_time) * 1000:.2f} ms"
    )
    return resp


# --- Main Execution (if needed for standalone testing) ---
# if __name__ == "__main__":
#     import atexit
#     def shutdown_log_executor():
#         print("Shutting down log executor...")
#         log_executor.shutdown(wait=True)
#         print("Log executor shut down.")
#     atexit.register(shutdown_log_executor)
#     # Need a Flask app instance to run this
#     # from flask import Flask
#     # app = Flask(__name__)
#     # app.register_blueprint(proxy_bp, url_prefix='/api')
#     # app.run(host="0.0.0.0", port=5000, debug=True)
