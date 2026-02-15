# scraping/core.py
from bs4 import BeautifulSoup
import requests
import ssl
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Try importing NTLM auth, handle if not installed
try:
    from requests_ntlm import HttpNtlmAuth
except ImportError:
    HttpNtlmAuth = None
    logging.warning("requests_ntlm not installed. NTLM authentication will not work.")

from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

# Global session for potential reuse (use with caution in multi-threaded envs without thread-local storage)
# Consider creating sessions per request or using thread-local storage if needed.
# For simplicity here, we'll create sessions as needed in helper functions.


def create_session(
    username: str = None, password: str = None, domain: str = "GUC"
) -> requests.Session:
    """Creates a requests session with NTLM auth and retry logic."""
    session = requests.Session()

    # Configure NTLM authentication if credentials provided
    if username and password:
        if HttpNtlmAuth:
            # Ensure username format is correct (e.g., DOMAIN\\username)
            ntlm_user = f"{domain}\\{username}" if domain else username
            session.auth = HttpNtlmAuth(ntlm_user, password)
            logger.debug(f"NTLM Auth configured for user: {ntlm_user}")
        else:
            logger.error(
                "NTLM auth requested but requests_ntlm library is not installed."
            )
            # Decide behaviour: raise error, or continue without auth? Continuing silently is risky.
            # raise ImportError("requests_ntlm is required for NTLM authentication but not installed.")

    # Configure retry strategy
    # Increase backoff_factor for more delay between retries
    retry_strategy = Retry(
        total=config.DEFAULT_MAX_RETRIES,
        backoff_factor=1,  # Increase delay: 1s, 2s, 4s...
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these server/rate errors
        allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],  # Retry on relevant methods
    )

    # Mount HTTPAdapter with retry strategy to session
    if config.VERIFY_SSL is False:
        adapter = UnsafeTLSAdapter(
            max_retries=retry_strategy, pool_connections=10, pool_maxsize=20
        )
    else:
        adapter = HTTPAdapter(
            max_retries=retry_strategy, pool_connections=10, pool_maxsize=20
        )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Set default headers
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Unisight/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )

    # Set default timeout (connect, read)
    session.timeout = (
        config.DEFAULT_REQUEST_TIMEOUT,
        config.DEFAULT_REQUEST_TIMEOUT * 2,
    )

    # Handle SSL Verification based on config (bool or custom CA bundle path)
    session.verify = config.VERIFY_SSL
    if config.VERIFY_SSL is False:
        # Suppress InsecureRequestWarning if verification is disabled
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        logger.warning("SSL verification is disabled. Requests may be insecure.")
    elif isinstance(config.VERIFY_SSL, str):
        logger.info(
            f"Using custom CA bundle for SSL verification: {config.VERIFY_SSL}"
        )

    return session


def make_request(
    session: requests.Session, url: str, method: str = "GET", **kwargs
) -> requests.Response | None:
    """
    Makes a request using the provided session.
    Relies on the retry logic configured within the session adapter.

    Args:
        session: The requests.Session object to use.
        url: The URL to request.
        method: HTTP method (GET, POST, etc.).
        **kwargs: Additional arguments to pass to session.request (e.g., data, json, headers, timeout).

    Returns:
        requests.Response object on success, None on failure after retries.
    """
    # Merge default timeout from session if not overridden in kwargs
    req_timeout = kwargs.pop("timeout", session.timeout)

    try:
        if "verify" not in kwargs:
            kwargs["verify"] = config.VERIFY_SSL
        response = session.request(method, url, timeout=req_timeout, **kwargs)

        # Check for specific auth failure status codes
        if response.status_code == 401:
            logger.warning(f"Request failed: 401 Unauthorized for {method} {url}")
            # No need to raise_for_status, just return None or the response itself
            # Returning None indicates failure to the caller more clearly than response object
            return None

        # Check for redirect to login page as another sign of auth failure
        if response.history:  # Check if redirection occurred
            for resp_hist in response.history:
                if "login" in resp_hist.url.lower():
                    logger.warning(
                        f"Request redirected to login page during history for {method} {url}"
                    )
                    return None  # Treat login redirect as failure
        if (
            "login" in response.url.lower() and response.status_code != 401
        ):  # Check final URL
            # Sometimes redirects happen with 200 OK but land on login
            temp_soup = BeautifulSoup(response.text, "lxml")
            if temp_soup.find("form", action=lambda x: x and "login" in x.lower()):
                logger.warning(
                    f"Request landed on login page (form detected) for {method} {url}"
                )
                return None  # Treat login page content as failure

        # Raise HTTPError for other bad responses (4xx client error, 5xx server error)
        # The session adapter's retry logic should have handled retries for status_forcelist codes.
        # This will catch errors like 404 Not Found, 403 Forbidden, etc.
        response.raise_for_status()

        logger.debug(
            f"Request successful: {method} {url} (Status: {response.status_code})"
        )
        return response

    except requests.exceptions.RetryError as e:
        # This exception is raised by urllib3/requests adapter after max retries are exhausted
        logger.error(f"Request failed after max retries for {method} {url}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        logger.error(f"Request timed out for {method} {url}: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error for {method} {url}: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        # Errors not covered by status_forcelist in retry or 401/login checks above
        logger.error(
            f"HTTP error for {method} {url}: {e.response.status_code} {e.response.reason}"
        )
        return None
    except requests.exceptions.RequestException as e:
        # Catch other potential requests exceptions
        logger.error(f"Request exception for {method} {url}: {e}", exc_info=True)
        return None
    except Exception as e:
        # Catch unexpected errors
        logger.error(
            f"Unexpected error during request for {method} {url}: {e}", exc_info=True
        )
        return None
# Force-disable TLS verification at the urllib3 layer when VERIFY_SSL is False.
# This is more robust than relying only on session.verify in some environments.
class UnsafeTLSAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self._ssl_context = ssl.create_default_context()
        self._ssl_context.check_hostname = False
        self._ssl_context.verify_mode = ssl.CERT_NONE
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["ssl_context"] = self._ssl_context
        pool_kwargs["assert_hostname"] = False
        return super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["ssl_context"] = self._ssl_context
        proxy_kwargs["assert_hostname"] = False
        return super().proxy_manager_for(proxy, **proxy_kwargs)
