# utils/auth.py
import logging
import os
import sys
from cryptography.fernet import Fernet, InvalidToken
import redis

from config import config

# Assuming the core authentication function is in scraping.authenticate
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from scraping.authenticate import authenticate_user


logger = logging.getLogger(__name__)

# Initialize Fernet
try:
    fernet = Fernet(config.ENCRYPTION_KEY.encode())
except Exception as e:
    logger.critical(f"Failed to initialize Fernet: {e}", exc_info=True)
    # Depending on requirements, you might raise an error here to prevent startup
    fernet = None  # Indicate failure

# Use the Redis client initialized in cache.py for consistency
# This avoids multiple connections if cache.py is imported first
try:
    from .cache import redis_client

    if not redis_client:
        logger.warning(
            "Redis client from utils.cache is None. Auth features needing Redis will fail."
        )
except ImportError:
    logger.critical("Could not import redis_client from utils.cache. Auth may fail.")
    redis_client = None

USER_CREDENTIALS_HASH = "user_credentials"


class AuthError(Exception):
    """Custom exception for authentication related errors."""

    def __init__(
        self, message, status_code=401, log_outcome="auth_error", log_message=None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.log_outcome = log_outcome
        self.log_message = log_message or message


def get_stored_password(username: str) -> str | None:
    """Retrieves and decrypts a stored password. Returns None if not found or on error."""
    if not redis_client:
        logger.error("Redis client unavailable for get_stored_password.")
        return None
    if not fernet:
        logger.error("Fernet encryption not available.")
        return None

    try:
        encrypted_pw_bytes = redis_client.hget(USER_CREDENTIALS_HASH, username)
        if encrypted_pw_bytes:
            try:
                decrypted_bytes = fernet.decrypt(encrypted_pw_bytes)
                return decrypted_bytes.decode("utf-8")
            except InvalidToken:
                logger.error(
                    f"InvalidToken: Failed to decrypt password for {username}. Stored data might be corrupted or using wrong key."
                )
                return None
            except Exception as e:
                logger.error(f"Decryption failed for {username}: {e}", exc_info=True)
                return None
        else:
            return None  # User not found
    except redis.exceptions.ConnectionError as e:
        logger.error(
            f"[Redis] Connection error getting stored password for {username}: {e}"
        )
        return None
    except Exception as e:
        logger.error(
            f"Error retrieving stored password for {username}: {e}", exc_info=True
        )
        return None


def store_user_credentials(username: str, password: str) -> bool:
    """Stores encrypted user credentials. Returns True on success, False on failure."""
    if not redis_client:
        logger.error("Redis client unavailable for store_user_credentials.")
        return False
    if not fernet:
        logger.error("Fernet encryption not available.")
        return False

    try:
        # Ensure password is str before encoding
        if isinstance(password, bytes):
            password = password.decode("utf-8", errors="ignore")

        encrypted_pw_bytes = fernet.encrypt(password.encode("utf-8"))
        # Store bytes directly in Redis hash
        redis_client.hset(USER_CREDENTIALS_HASH, username, encrypted_pw_bytes)
        logger.info(f"Stored/Updated credentials for user: {username}")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(
            f"[Redis] Connection error storing credentials for {username}: {e}"
        )
    except Exception as e:
        logger.error(
            f"Error storing credentials for user '{username}': {e}", exc_info=True
        )
    return False


def user_has_stored_credentials(username: str) -> bool:
    """
    Checks if a user has stored credentials.
    Returns True if credentials exist, False otherwise.
    """
    if not redis_client:
        logger.error("Redis client unavailable for user_has_stored_credentials.")
        return False

    try:
        exists = redis_client.hexists(USER_CREDENTIALS_HASH, username)
        return bool(exists)
    except redis.exceptions.ConnectionError as e:
        logger.error(
            f"[Redis] Connection error checking credentials existence for {username}: {e}"
        )
        return False
    except Exception as e:
        logger.error(
            f"Error checking credentials existence for user '{username}': {e}", exc_info=True
        )
        return False


def validate_credentials_flow(
    username: str, password: str, first_time: bool = False
) -> str:
    """
    Handles the user authentication flow, checking stored creds or GUC.
    Returns the valid password to use for scraping.
    Raises AuthError on failure.
    """
    if not username or not password:
        raise AuthError("Missing username or password", 400, "validation_error")

    password_to_use = None
    stored_password = get_stored_password(username)
    logger.debug(
        f"AUTH_FLOW_DEBUG: User='{username}', Provided='{password!r}', Stored='{stored_password!r}'"
    )

    if first_time:
        logger.info(f"First time login flow for {username}")
        try:
            auth_success = authenticate_user(username, password)  # Call core GUC auth
            if not auth_success:
                raise AuthError("Invalid credentials", 401, "first_time_auth_fail")

            logger.info(f"First time GUC auth successful for {username}. Storing.")
            store_user_credentials(username, password)
            password_to_use = password
        except Exception as e:
            logger.error(
                f"Error during first time auth check for {username}: {e}", exc_info=True
            )
            # Check if the exception is related to authentication
            error_msg = str(e).lower()
            if "auth" in error_msg or "login failed" in error_msg or "credentials" in error_msg or "password" in error_msg or "invalid" in error_msg or "401" in error_msg:
                raise AuthError("Invalid credentials", 401, "first_time_auth_fail") from e
            else:
                # Only use 503 for actual server/connection issues
                raise AuthError(
                    f"Authentication check failed: {e}", 503, "first_time_auth_exception"
                ) from e
    elif stored_password:
        # Not first time, user exists in store
        if stored_password.strip() == password.strip():
            logger.debug(f"Stored credentials match for {username}")
            password_to_use = stored_password  # Use the known good password
        else:
            # Password mismatch - try GUC auth with the *new* password
            logger.warning(
                f"Stored password mismatch for {username}. Attempting GUC auth with provided password."
            )
            try:
                auth_success = authenticate_user(username, password)
                if auth_success:
                    logger.info(
                        f"GUC accepted new password for {username}. Updating stored credentials."
                    )
                    store_user_credentials(username, password)
                    password_to_use = password  # Use the newly verified password
                else:
                    # GUC rejected the new password, stored one was likely correct but user provided wrong one
                    raise AuthError(
                        "Invalid credentials", 401, "stored_auth_fail_mismatch"
                    )
            except Exception as e:
                logger.error(
                    f"Error during GUC re-auth check for {username}: {e}", exc_info=True
                )
                # Check if the exception is related to authentication
                error_msg = str(e).lower()
                if "auth" in error_msg or "login failed" in error_msg or "credentials" in error_msg or "password" in error_msg or "invalid" in error_msg or "401" in error_msg:
                    raise AuthError("Invalid credentials", 401, "stored_auth_fail_mismatch") from e
                else:
                    # Only use 503 for actual server/connection issues
                    raise AuthError(
                        f"Authentication check failed: {e}", 503, "stored_auth_exception"
                    ) from e
    else:
        # Not first time, but user *not* found in store (e.g., cache cleared, or never logged in before despite first_time=false)
        logger.info(
            f"Credentials not stored for {username} (non-first-time flow). Attempting GUC auth."
        )
        try:
            auth_success = authenticate_user(username, password)  # Call core GUC auth
            if not auth_success:
                raise AuthError(
                    "Invalid credentials or user not found",
                    401,
                    "non_first_time_auth_fail",
                )

            logger.info(f"GUC auth successful for non-stored user {username}. Storing.")
            store_user_credentials(username, password)
            password_to_use = password
        except Exception as e:
            logger.error(
                f"Error during GUC auth for non-stored user {username}: {e}",
                exc_info=True,
            )
            # Check if the exception is related to authentication
            error_msg = str(e).lower()
            if "auth" in error_msg or "login failed" in error_msg or "credentials" in error_msg or "password" in error_msg or "invalid" in error_msg or "401" in error_msg:
                raise AuthError("Invalid credentials", 401, "non_first_time_auth_fail") from e
            else:
                # Only use 503 for actual server/connection issues
                raise AuthError(
                    f"Authentication check failed: {e}",
                    503,
                    "non_first_time_auth_exception",
                ) from e

    if not password_to_use:
        # This should theoretically not be reached if logic is correct
        logger.error(
            f"Internal Auth Logic Error: password_to_use not set for {username}"
        )
        raise AuthError("Internal Server Error", 500, "internal_error_auth_logic")

    return password_to_use


def get_all_stored_users_decrypted():
    """Retrieves all usernames and their *decrypted* passwords. Use with extreme caution."""
    if not redis_client or not fernet:
        logger.error(
            "Redis or Fernet not available for get_all_stored_users_decrypted."
        )
        return {}

    decrypted_users = {}
    try:
        stored_users_bytes = redis_client.hgetall(USER_CREDENTIALS_HASH)
        for username_bytes, encrypted_pw_bytes in stored_users_bytes.items():
            username = username_bytes.decode("utf-8", "ignore")
            try:
                password = fernet.decrypt(encrypted_pw_bytes).decode("utf-8")
                decrypted_users[username] = password
            except Exception as e:
                logger.error(
                    f"Failed to decrypt password for user {username} in bulk retrieval: {e}"
                )
                decrypted_users[username] = "DECRYPTION_ERROR"
        return decrypted_users
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Redis] Connection error getting all stored users: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error getting all stored users decrypted: {e}", exc_info=True)
        return {}


def get_all_stored_usernames() -> list[str]:
    """Retrieves only the usernames of users with stored credentials."""
    if not redis_client:
        logger.error("Redis client unavailable for get_all_stored_usernames.")
        return []
    try:
        username_bytes = redis_client.hkeys(USER_CREDENTIALS_HASH)
        return [uname.decode("utf-8", "ignore") for uname in username_bytes]
    except redis.exceptions.ConnectionError as e:
        logger.error(f"[Redis] Connection error getting stored usernames: {e}")
        return []
    except Exception as e:
        logger.error(f"Error retrieving stored usernames: {e}", exc_info=True)
        return []


def delete_user_credentials(username: str) -> bool:
    """Deletes credentials for a specific user."""
    if not redis_client:
        logger.error("Redis client unavailable for delete_user_credentials.")
        return False
    try:
        deleted_count = redis_client.hdel(USER_CREDENTIALS_HASH, username)
        if deleted_count > 0:
            logger.info(f"Deleted credentials for user: {username}")
            return True
        else:
            logger.warning(
                f"Attempted to delete credentials for non-existent user: {username}"
            )
            return False
    except redis.exceptions.ConnectionError as e:
        logger.error(
            f"[Redis] Connection error deleting credentials for {username}: {e}"
        )
    except Exception as e:
        logger.error(
            f"Error deleting credentials for user '{username}': {e}", exc_info=True
        )
    return False


# --- Whitelist Function (Consider if needed, uses simple Redis key) ---
WHITELIST_KEY = "WHITELIST"


def get_whitelist() -> list[str]:
    """Retrieves the list of whitelisted users."""
    if not redis_client:
        logger.error("Redis client unavailable for get_whitelist.")
        return []  # Default to empty list if Redis fails
    try:
        # Assuming decode_responses=True for redis_client where it's used
        # Need consistent client setup or handle bytes here. Let's assume string client.
        # Re-getting client specific for strings here if needed:
        str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        whitelist_raw = str_redis_client.get(WHITELIST_KEY)
        if whitelist_raw:
            return [user.strip() for user in whitelist_raw.split(",") if user.strip()]
        else:
            return []  # No whitelist set or empty
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error getting whitelist: {e}")
    except Exception as e:
        logger.error(f"Error getting whitelist: {e}", exc_info=True)
    return []  # Return empty list on error


def set_whitelist(users: list[str]) -> bool:
    """Sets the whitelist in Redis."""
    if not redis_client:
        logger.error("Redis client unavailable for set_whitelist.")
        return False
    try:
        str_redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        whitelist_str = ",".join(users)
        str_redis_client.set(WHITELIST_KEY, whitelist_str)
        logger.info(f"Whitelist updated with {len(users)} users.")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error setting whitelist: {e}")
    except Exception as e:
        logger.error(f"Error setting whitelist: {e}", exc_info=True)
    return False


def get_password_for_readonly_session(username: str, password_provided: str) -> str:
    """
    Authenticates a user for a read-only session.
    Checks against stored password if available, or GUC if not.
    NEVER stores or updates credentials.
    Returns the password to use for the session if auth is successful.
    Raises AuthError on failure.
    """
    if not username or not password_provided: # Basic check
        logger.warning("Readonly Auth: Username or password not provided.")
        raise AuthError("Username or password not provided", 400, "readonly_auth_missing_params")

    stored_password = get_stored_password(username)

    if stored_password:
        if stored_password == password_provided:
            logger.debug(f"Readonly Auth: Provided password matches stored for {username}.")
            return stored_password  # Use the validated stored password
        else:
            logger.warning(f"Readonly Auth: Provided password for {username} does not match stored. Access denied.")
            raise AuthError("Invalid credentials", 401, "readonly_stored_pw_mismatch")
    else:
        # No stored password, GUC check for session only, no storing.
        logger.info(f"Readonly Auth: No stored credentials for {username}. Attempting direct GUC auth for session only.")
        # Ensure authenticate_user is available in this scope
        # It's imported at the top of utils/auth.py as: from scraping.authenticate import authenticate_user
        auth_success = authenticate_user(username, password_provided)
        if auth_success:
            logger.info(f"Readonly Auth: Direct GUC authentication successful for {username}.")
            return password_provided # Use the GUC-validated provided password
        else:
            logger.warning(f"Readonly Auth: Direct GUC authentication failed for {username}.")
            raise AuthError("Invalid credentials", 401, "readonly_direct_auth_fail")
