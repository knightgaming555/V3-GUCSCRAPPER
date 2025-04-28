# api/notifications.py
import logging
import json
from flask import Blueprint, request, jsonify, g

import redis

from utils.auth import validate_credentials_flow, AuthError
from utils.cache import get_from_cache, set_in_cache, generate_cache_key, redis_client

logger = logging.getLogger(__name__)
notifications_bp = Blueprint("notifications_bp", __name__)

# Constants
NOTIFICATIONS_CACHE_PREFIX = "notifications"
# ****** CENTRALIZED LIMIT ******
MAX_NOTIFICATIONS = 2  # Maximum number of notifications to STORE and RETURN


@notifications_bp.route("/notifications", methods=["GET"])
def api_notifications():
    """
    Endpoint to fetch user notifications about changes in grades, attendance, and GUC data.

    Returns a 2D array of notifications limited by MAX_NOTIFICATIONS.
    Requires query params: username, password
    """
    # --- Bot Health Check ---
    if request.args.get("bot", "").lower() == "true":
        logger.info("Received bot health check request for Notifications API.")
        g.log_outcome = "bot_check_success"
        return (
            jsonify(
                {
                    "status": "Success",
                    "message": "Notifications API route is up!",
                    "data": None,
                }
            ),
            200,
        )

    # --- Parameter Extraction & Validation ---
    username = request.args.get("username")
    password = request.args.get("password")
    g.username = username

    try:
        # Validate credentials
        validate_credentials_flow(username, password)

        # Get notifications from cache
        cache_key = generate_cache_key(NOTIFICATIONS_CACHE_PREFIX, username)
        notifications = get_from_cache(cache_key) or []

        # Limit to MAX_NOTIFICATIONS
        notifications_to_return = notifications[:MAX_NOTIFICATIONS]

        logger.info(
            f"Retrieved {len(notifications_to_return)} notifications for {username} (Cache holds {len(notifications)})"
        )

        # Clear the notifications cache by setting it to an empty array
        # Use a very long timeout to ensure it doesn't expire
        set_in_cache(cache_key, [], timeout=31536000)  # 1 year in seconds
        logger.info(f"Cleared notifications cache for {username}")

        g.log_outcome = "success"

        return jsonify(notifications_to_return), 200

    except AuthError as e:
        logger.warning(
            f"AuthError during notifications request for {username}: {e.log_message}"
        )
        g.log_outcome = e.log_outcome
        g.log_error_message = e.log_message
        return jsonify({"status": "error", "message": str(e)}), e.status_code
    except Exception as e:
        logger.exception(
            f"Unhandled exception during /api/notifications for {username}: {e}"
        )
        g.log_outcome = "internal_error_unhandled"
        g.log_error_message = f"Unhandled exception: {e}"
        return (
            jsonify(
                {"status": "error", "message": "An internal server error occurred"}
            ),
            500,
        )


# Helper functions for notification generation
def add_notification(username, notification_type, description):
    """
    Add a notification to the user's notification cache, respecting MAX_NOTIFICATIONS.

    Args:
        username (str): The username to add the notification for
        notification_type (str): The type of notification (e.g., "New grade", "Attendance update")
        description (str): The description of the notification

    Returns:
        bool: True if the notification was added successfully, False otherwise
    """
    if not username or not notification_type or not description:
        logger.warning("Attempted to add notification with missing info.")
        return False
    try:
        cache_key = generate_cache_key(NOTIFICATIONS_CACHE_PREFIX, username)
        # Use Redis transaction for atomicity (optional but good practice)
        with redis_client.pipeline() as pipe:
            try:
                pipe.watch(cache_key)  # Watch for changes between read and write
                current_value = pipe.get(cache_key)
                notifications = []
                if current_value:
                    try:
                        notifications = json.loads(current_value.decode("utf-8"))
                        if not isinstance(notifications, list):
                            logger.warning(
                                f"Invalid data format in notification cache for {username}. Resetting."
                            )
                            notifications = []
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(
                            f"Could not decode notification cache for {username}: {e}. Resetting."
                        )
                        notifications = []

                # Check if this exact notification [type, description] already exists
                new_notification_entry = [notification_type, description]
                if new_notification_entry in notifications:
                    logger.debug(
                        f"Notification '{description}' already exists for {username}. Skipping add."
                    )
                    return False  # Indicate it wasn't newly added

                # Start transaction
                pipe.multi()

                # Add new notification at the beginning
                notifications.insert(0, new_notification_entry)

                # Limit to MAX_NOTIFICATIONS (Use the constant)
                if len(notifications) > MAX_NOTIFICATIONS:
                    notifications = notifications[:MAX_NOTIFICATIONS]

                # Update cache within the transaction
                # Use a very long timeout to ensure it doesn't expire
                timeout = 31536000  # 1 year in seconds
                pipe.setex(cache_key, timeout, json.dumps(notifications))

                # Execute the transaction
                pipe.execute()
                logger.info(
                    f"Successfully added notification via transaction for {username}: {notification_type} - {description}"
                )
                return True  # Successfully added

            except redis.exceptions.WatchError:
                logger.warning(
                    f"WatchError on notification cache for {username}. Retrying add might be needed if concurrent writes are frequent."
                )
                # For this script, likely not an issue, but good to know.
                return False  # Indicate addition failed due to contention
            except redis.exceptions.ConnectionError as e:
                logger.error(
                    f"Redis connection error adding notification for {username}: {e}"
                )
                return False
            except Exception as e:
                logger.error(
                    f"Error during Redis pipeline for adding notification ({username}): {e}",
                    exc_info=True,
                )
                return False  # Indicate generic failure

    except Exception as e:
        logger.error(
            f"General error adding notification for {username}: {e}", exc_info=True
        )
        return False
