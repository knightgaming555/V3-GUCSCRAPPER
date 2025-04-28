# api/auth.py
import logging
from flask import Blueprint, request, jsonify, g
import redis

from config import config
from utils.auth import (
    validate_credentials_flow,
    store_user_credentials,
    AuthError,
    get_whitelist,
    set_whitelist,
)
from utils.helpers import get_country_from_ip
from utils.cache import (
    redis_client,
)  # Direct access for version check/whitelist if needed

logger = logging.getLogger(__name__)
auth_bp = Blueprint("auth_bp", __name__)


@auth_bp.route("/login", methods=["POST"])
def api_login():
    """
    Handles user login attempts. Authenticates against GUC, stores/updates credentials on success.
    Expects JSON body: {"username": "...", "password": "..."}
    Optional query param: ?version_number=...
    """
    data = request.get_json()
    if not data:
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing JSON request body"
        return jsonify({"status": "error", "message": "Missing JSON request body"}), 400

    username = data.get("username")
    password = data.get("password")
    g.username = username  # Set for logging context

    if not username or not password:
        logger.warning("Login attempt missing username or password in JSON body")
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing username or password"
        return (
            jsonify({"status": "error", "message": "Missing username or password"}),
            400,
        )

    # --- Version Check ---
    # Fetch current version from Redis (cache this check if it becomes expensive)
    current_version = "1.0"  # Default
    try:
        if redis_client:  # Check if redis_client is available
            str_redis_client = redis.from_url(
                config.REDIS_URL, decode_responses=True
            )  # Client for strings
            version_raw = str_redis_client.get("VERSION_NUMBER")
            if version_raw:
                current_version = version_raw
        else:
            logger.warning(
                "Redis client not available for version check, using default."
            )
    except Exception as e:
        logger.error(f"Error getting VERSION_NUMBER from Redis: {e}", exc_info=True)
        # Proceed with default version, or return error? Let's proceed.

    req_version = request.args.get("version_number")  # Get from query parameters
    if req_version != current_version:
        logger.warning(
            f"Incorrect version number for {username}. Required: {current_version}, Got: {req_version}"
        )
        g.log_outcome = "version_error"
        g.log_error_message = (
            f"Incorrect version. Required: {current_version}, Got: {req_version}"
        )
        # Return 426 Upgrade Required might be semantically better than 403
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"Incorrect version number. Please update the app to version {current_version}.",
                    "data": None,
                }
            ),
            426,
        )

    try:
        # Use the centralized validation flow - it handles storing internally on success
        # It raises AuthError on failure
        valid_password = validate_credentials_flow(
            username, password, first_time=False
        )  # Assume not first time for basic login endpoint

        # If validate_credentials_flow returns without error, login is successful
        g.log_outcome = "login_success"
        logger.info(f"Login successful for {username}")

        # Track country (best effort)
        try:
            ip_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
            country = get_country_from_ip(ip_addr)
            if country not in (
                "Lookup Failed (Network)",
                "Lookup Failed (JSON)",
                "Lookup Failed (Unknown)",
                "Lookup Timeout",
                "API Error",
                "Localhost",
                "Unknown",
                "Invalid IP Format",
                "Reserved Range",
            ):
                if redis_client:
                    str_redis_client = redis.from_url(
                        config.REDIS_URL, decode_responses=True
                    )
                    str_redis_client.hset("user_countries", username, country)
                    logger.info(
                        f"Stored country '{country}' for {username} from IP {ip_addr}"
                    )
                else:
                    logger.warning(
                        f"Cannot store country for {username}, Redis client unavailable."
                    )
            else:
                logger.warning(
                    f"Could not determine/store valid country for {username} from IP {ip_addr}, result was: {country}"
                )
        except Exception as country_err:
            logger.error(
                f"Error storing country for {username}: {country_err}", exc_info=True
            )

        # Determine if password was updated during the flow (less direct now, maybe add flag to AuthError or return tuple?)
        # For simplicity, we just return success here. The flow logs the update.
        return jsonify({"status": "success", "username": username}), 200

    except AuthError as e:
        logger.warning(f"AuthError during login for {username}: {e.log_message}")
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        # Catch unexpected errors during the process
        logger.exception(f"Unexpected error during login for {username}: {e}")
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unexpected login error: {e}"
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "An internal server error occurred during login.",
                }
            ),
            500,
        )


@auth_bp.route("/test-login", methods=["POST"])
def api_test_login():
    """
    Tests credentials against GUC without storing them.
    Expects JSON body: {"username": "...", "password": "..."}
    """
    data = request.get_json()
    if not data:
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing JSON request body"
        return jsonify({"status": "error", "message": "Missing JSON request body"}), 400

    username = data.get("username")
    password = data.get("password")
    g.username = username  # Set for logging

    if not username or not password:
        g.log_outcome = "validation_error"
        g.log_error_message = "Missing username or password"
        return (
            jsonify({"status": "error", "message": "Missing username or password"}),
            400,
        )

    logger.info(f"Test login attempt for {username}")
    try:
        # Directly call the core authentication function
        from scraping.authenticate import authenticate_user

        auth_success = authenticate_user(username, password)

        if auth_success:
            g.log_outcome = "test_login_success"
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Credentials are valid (Test Only)",
                    }
                ),
                200,
            )
        else:
            g.log_outcome = "test_login_fail"
            g.log_error_message = "Invalid credentials (Test Only)"
            return (
                jsonify(
                    {"status": "error", "message": "Invalid credentials (Test Only)"}
                ),
                401,
            )

    except Exception as e:
        logger.exception(f"Error during test authentication for {username}: {e}")
        g.log_outcome = "internal_error_test_auth"
        g.log_error_message = f"Test authentication failed: {e}"
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Test authentication failed due to an internal error",
                }
            ),
            500,
        )


# Example endpoint to view whitelist (consider adding admin auth later)
@auth_bp.route("/debug/whitelist", methods=["GET"])
def debug_whitelist():
    # Basic check if needed, or remove for public debug
    # if request.headers.get("Admin-Secret") != config.ADMIN_SECRET:
    #    return jsonify({"error": "Unauthorized"}), 403
    whitelist = get_whitelist()
    return jsonify({"whitelist": whitelist}), 200
