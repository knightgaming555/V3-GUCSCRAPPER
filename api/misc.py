# api/misc.py
import logging
import json
from flask import Blueprint, request, jsonify, g
import redis

from config import config

# Assuming redis_client is accessible via utils.cache
try:
    from utils.cache import redis_client

    if not redis_client:
        logging.warning(
            "Redis client not available via utils.cache for misc endpoints."
        )
except ImportError:
    logging.critical(
        "Could not import redis_client from utils.cache for misc endpoints."
    )
    redis_client = None

# Import admin secret check if needed for specific endpoints
from .admin import check_admin_secret

logger = logging.getLogger(__name__)
misc_bp = Blueprint("misc_bp", __name__)

# --- User Activity ---


def _get_user_activity(username: str) -> dict | None:
    """Retrieves activity data for a specific user."""
    if not username or not redis_client:
        return None
    user_key = f"user_activity:{username}"
    try:
        # Use string client for activity data
        str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        activity_data = str_redis_client.hgetall(user_key)
        # Convert counts to integers if needed
        if activity_data and "access_count" in activity_data:
            try:
                activity_data["access_count"] = int(activity_data["access_count"])
            except ValueError:
                pass  # Keep as string if not integer
        return activity_data if activity_data else None
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error getting activity for {username}: {e}")
    except Exception as e:
        logger.error(f"Error getting activity for {username}: {e}", exc_info=True)
    return None


def _get_all_user_activity() -> dict:
    """Retrieves activity data for ALL users."""
    all_activity = {}
    if not redis_client:
        return all_activity
    try:
        str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        # Use SCAN to avoid blocking with KEYS on large datasets
        cursor = "0"
        while True:
            cursor, keys = str_redis_client.scan(
                cursor=cursor, match="user_activity:*", count=100
            )
            for key in keys:
                username = key.split(":", 1)[1]
                activity_data = str_redis_client.hgetall(key)
                if activity_data:
                    if "access_count" in activity_data:
                        try:
                            activity_data["access_count"] = int(
                                activity_data["access_count"]
                            )
                        except ValueError:
                            pass
                    all_activity[username] = activity_data
            if cursor == 0:
                break
        return all_activity
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error getting all user activity: {e}")
    except Exception as e:
        logger.error(f"Error getting all user activity: {e}", exc_info=True)
    return all_activity  # Return what was gathered before error


@misc_bp.route("/user-activity", methods=["GET"])
def api_user_activity():
    """
    API endpoint to retrieve user activity data.
    Requires admin secret for security.
    Query param 'username' (optional) to get specific user.
    """
    # Requires Admin Auth
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403

    username_param = request.args.get("username")
    g.log_outcome = "success"  # Assume success unless specific user not found

    if username_param:
        g.username = username_param  # Log target user
        activity = _get_user_activity(username_param)
        if activity:
            return jsonify({username_param: activity}), 200
        else:
            g.log_outcome = "not_found"
            g.log_error_message = f"No activity data found for user: {username_param}"
            return (
                jsonify(
                    {"error": f"No activity data found for user: {username_param}"}
                ),
                404,
            )
    else:
        # Get activity for all users
        all_activity = _get_all_user_activity()
        return jsonify(all_activity), 200


# --- Country Stats ---
@misc_bp.route("/country_stats", methods=["GET"])
def api_country_stats():
    """
    Returns the number of users per country based on stored data.
    Requires admin secret.
    """
    # Requires Admin Auth
    if not check_admin_secret():
        return jsonify({"error": "Unauthorized"}), 403
    g.log_outcome = "success"

    if not redis_client:
        g.log_outcome = "redis_error"
        return jsonify({"error": "Redis client not available"}), 503

    stats = {}
    try:
        # Use string client for this hash
        str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        user_countries = str_redis_client.hgetall("user_countries")
        for _username, country in user_countries.items():
            stats[country] = stats.get(country, 0) + 1
        return jsonify(stats), 200
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error getting country stats: {e}")
        g.log_outcome = "redis_error"
        return jsonify({"error": f"Failed to connect to Redis: {e}"}), 503
    except Exception as e:
        logger.exception(f"Error calculating country stats: {e}")
        g.log_outcome = "internal_error"
        return jsonify({"error": f"Failed to calculate stats: {e}"}), 500


# --- Debug/Version Endpoint ---
@misc_bp.route("/version", methods=["GET"])
def api_version():
    """Returns the current API version stored in Redis."""
    # Publicly accessible? Or requires auth? Assume public for now.
    g.log_outcome = "success"
    version_number = "N/A"
    try:
        if redis_client:
            str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
            version_raw = str_redis_client.get("VERSION_NUMBER")
            version_number = version_raw if version_raw else "Not Set"
        else:
            version_number = "Redis Unavailable"
    except Exception as e:
        logger.error(f"Error getting version number: {e}")
        version_number = "Error Fetching"

    return jsonify({"current_api_version": version_number}), 200
