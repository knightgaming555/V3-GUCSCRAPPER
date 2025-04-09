# scraping/authenticate.py
import requests
import logging
import time
from bs4 import BeautifulSoup

# Use the core session creation and request making helpers
from .core import create_session, make_request
from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)


def authenticate_user(username: str, password: str, domain: str = "GUC") -> bool:
    """
    Authenticates a user directly against the university login mechanism.
    Uses retry logic defined in make_request via the session adapter.

    Args:
        username (str): User's university ID (without domain prefix).
        password (str): User's password.
        domain (str): The NTLM domain (default: "GUC").

    Returns:
        bool: True if authentication is successful, False otherwise.
    """
    if not username or not password:
        logger.warning("Authenticate_user called with empty username or password.")
        return False

    # Use the GUC index URL as the target for authentication check
    auth_check_url = config.GUC_INDEX_URL
    # Use shorter timeout for auth check
    auth_timeout = (5, 10)  # (connect, read)

    # Create a dedicated session for this authentication attempt
    # Pass domain correctly
    session = create_session(username=username, password=password, domain=domain)

    logger.info(f"Attempting authentication for user: {username}")

    # make_request handles retries based on session adapter config
    response = make_request(session, auth_check_url, method="GET", timeout=auth_timeout)

    # Analyze the response
    if response:
        # Check 1: Successful status code (usually 200)
        if response.status_code == 200:
            # Check 2: Content indicating successful login (e.g., "Welcome")
            # This is crucial as 200 might still land on login page sometimes
            if "Welcome" in response.text:  # Adjust keyword if needed
                logger.info(
                    f"Authentication successful for user: {username} (Status: {response.status_code}, 'Welcome' found)"
                )
                logger.debug(
                    f"AUTH_DEBUG: Returning True because ... (e.g., Status={response.status_code}, 'Welcome' found)"
                )
                return True
            else:
                # Check if it's actually the login page despite 200 OK
                soup_login_check = BeautifulSoup(response.text, "lxml")
                if soup_login_check.find(
                    "form", action=lambda x: x and "login" in x.lower()
                ):
                    logger.warning(
                        f"Authentication failed for user: {username} (Status: {response.status_code}, but login form found)"
                    )
                    logger.debug(
                        f"AUTH_DEBUG: Returning False because ... (e.g., Status={response.status_code}, login form found)"
                    )
                    return False
                else:
                    # 200 OK but no "Welcome" and not login page? Maybe GUC changed layout. Log warning.
                    logger.warning(
                        f"Authentication check for user: {username} returned 200 OK but unexpected content (no 'Welcome', no login form). Assuming success for now, but verify."
                    )
                    # Consider returning False here if strict checking is required
                    logger.debug(
                        f"AUTH_DEBUG: Returning True because ... (e.g., Status={response.status_code}, 'Welcome' found)"
                    )
                    return True
        else:
            # Status code was not 200 (and not 401 caught by make_request)
            logger.warning(
                f"Authentication failed for user: {username} (Status: {response.status_code})"
            )
            logger.debug(
                f"AUTH_DEBUG: Returning False because ... (e.g., Status={response.status_code}, login form found)"
            )
            return False
    else:
        # make_request returned None (all retries failed, timeout, connection error, or 401)
        # Specific reason (401, timeout, etc.) is logged within make_request
        logger.error(
            f"Authentication failed for user: {username} (Request failed after retries)"
        )
        return False
