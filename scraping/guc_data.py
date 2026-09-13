# scraping/guc_data.py
import logging
import traceback
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import time  # For perf_counter
from time import perf_counter  # Explicit import
import json  # For error dicts

from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)


# --- Concurrent fetching via requests (replaces pycurl) ---
# NOTE: pycurl was removed here because its libcurl build is fragile on
# Windows/Vercel (CURLE_NOT_BUILT_IN / error 4 on setopt, e.g. missing NTLM
# or SSL backend mismatch). Every other scraper already uses
# scraping.core.create_session/make_request (requests + NTLM), and auth via
# that path succeeds, so guc_data now uses the same path.
import concurrent.futures

try:
    from .core import create_session, make_request
except ImportError:  # pragma: no cover - fallback for odd import contexts
    from scraping.core import create_session, make_request


def _parse_userpwd(userpwd: str) -> tuple[str, str, str]:
    """Parses 'DOMAIN\\username:password' into (domain, username, password)."""
    domain, username, password = "GUC", "", ""
    try:
        creds, _, password = (userpwd or "").partition(":")
        # password itself may contain ':'; partition above only splits on the
        # first one, but that breaks if... actually user part never contains
        # ':' so first ':' is the separator. Rejoin the rest correctly:
        # partition already keeps the remainder in `password`, which is right.
        if "\\" in creds:
            domain, _, username = creds.partition("\\")
            username = username.lstrip("\\")
        else:
            username = creds
    except Exception:
        pass
    return domain or "GUC", username, password


def _fetch_single(url: str, username: str, password: str, domain: str) -> tuple[str, str, str | None]:
    """Fetches one URL with a dedicated requests session. Returns (url, text, error)."""
    try:
        session = create_session(username=username, password=password, domain=domain)
        resp = make_request(
            session,
            url,
            method="GET",
            timeout=(config.DEFAULT_REQUEST_TIMEOUT, config.DEFAULT_REQUEST_TIMEOUT * 2),
        )
        if resp is None:
            return url, "", "request failed after retries (see logs: 401/timeout/connection/login-redirect)"
        try:
            content = resp.text
        except Exception as decode_err:
            logger.error(f"Error decoding response for {url}: {decode_err}")
            return url, "", f"Decode error: {decode_err}"
        if "Login Failed!" in content or "Object moved" in content:
            logger.warning(f"Auth failure detected in content for {url}")
            return url, content, "Authentication failed (content check)"
        logger.debug(f"Fetch success for {url} (Status: {resp.status_code})")
        return url, content, None
    except Exception as e:
        logger.error(f"Fetch exception for {url}: {e}", exc_info=True)
        return url, "", f"fetch exception: {e}"


def multi_fetch(urls: list[str], userpwd: str) -> tuple[dict, dict]:
    """Fetches multiple URLs concurrently using requests + NTLM (thread per URL).

    Keeps the original (results, errors) return shape so callers are unchanged.
    """
    start_time = perf_counter()
    results: dict = {}
    errors: dict = {}
    domain, username, password = _parse_userpwd(userpwd)

    if not urls:
        return results, errors

    max_workers = max(1, min(len(urls), 4))
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="GucDataFetch"
    ) as executor:
        future_to_url = {
            executor.submit(_fetch_single, url, username, password, domain): url
            for url in urls
        }
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                f_url, content, err = future.result()
                results[f_url] = content
                if err:
                    errors[f_url] = err
            except Exception as e:
                logger.error(f"Exception fetching {url}: {e}", exc_info=True)
                results[url] = ""
                errors[url] = f"fetch exception: {e}"

    # Ensure results dict contains entries for all original URLs
    for url in set(urls):
        if url not in results:
            results[url] = ""
            if url not in errors:
                errors[url] = "Fetch failed (unknown reason)"

    duration = perf_counter() - start_time
    logger.debug(f"requests multi_fetch completed in {duration:.3f}s")
    return results, errors


# --- HTML Parsing Functions (Keep identical from previous scraping/guc_data.py) ---


def parse_student_info(html: str) -> dict:
    """Parses student information from the index page HTML."""
    info = {}
    if not html:
        return info
    try:
        soup = BeautifulSoup(html, "lxml")
        prefix = "ContentPlaceHolderright_ContentPlaceHoldercontent_Label"
        mapping = {
            "FullName": "fullname",
            "UniqAppNo": "uniqappno",
            "UserCode": "usercode",
            "Mail": "mail",
            "sg": "sg",
        }
        found_any = False
        for label, key in mapping.items():
            element = soup.find(id=f"{prefix}{label}")
            if element:
                info[key] = element.get_text(" ", strip=True).replace("\r", "")
                found_any = True
            else:
                info[key] = ""
        if not found_any:
            logger.warning("Failed to parse any student info fields.")
    except Exception as e:
        logger.error(f"Error parsing student info: {e}", exc_info=True)
    return info


def parse_notifications(html: str) -> list:
    """Parses notifications from the notifications page HTML."""
    notifications = []
    if not html:
        return notifications
    try:
        soup = BeautifulSoup(html, "lxml")
        table = soup.find(
            id="ContentPlaceHolderright_ContentPlaceHoldercontent_GridViewdata"
        )
        if not table:
            if "Login Failed!" not in html and "Object moved" not in html:
                logger.warning(
                    "Notifications table '...GridViewdata' not found in HTML."
                )
            return notifications

        rows = table.find_all("tr")[1:]
        for idx, row in enumerate(rows):
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            try:
                notif = {
                    "id": cells[0].get_text(strip=True).replace("\r", ""),
                    "title": cells[2].get_text(" ", strip=True).replace("\r", ""),
                    "date": cells[3].get_text(strip=True).replace("\r", ""),
                    "staff": cells[4].get_text(strip=True).replace("\r", ""),
                    "importance": cells[5].get_text(strip=True).replace("\r", ""),
                }
                button = cells[1].find("button")
                email_time_iso = datetime.now(timezone.utc).isoformat()
                if button:
                    email_time_str = button.get("data-email_time", "")
                    if email_time_str:
                        try:
                            email_time_iso = datetime.strptime(
                                email_time_str, "%m/%d/%Y"
                            ).isoformat()
                        except ValueError:
                            logger.warning(
                                f"Error parsing email_time '{email_time_str}'."
                            )
                    notif["subject"] = (
                        button.get("data-subject_text", "")
                        .replace("Notification System:", "")
                        .strip()
                        .replace("\r", "")
                    )
                    notif["body"] = (
                        button.get("data-body_text", "")
                        .replace("------------------------------", "")
                        .strip()
                        .replace("\r", "")
                    )
                else:
                    notif["subject"] = ""
                    notif["body"] = ""
                notif["email_time"] = email_time_iso
                notifications.append(notif)
            except Exception as e_row:
                logger.error(
                    f"Error processing notification row {idx+1}: {e_row}", exc_info=True
                )
    except Exception as e_table:
        logger.error(f"Error parsing notifications table: {e_table}", exc_info=True)

    try:
        notifications.sort(key=lambda x: x.get("email_time", ""), reverse=True)
    except Exception as sort_err:
        logger.error(f"Failed to sort notifications: {sort_err}")
    return notifications


# --- Synchronous Scraping Function ---


def scrape_guc_data_fast(
    username: str, password: str, domain: str = "GUC"
) -> dict | None:
    """
    Synchronously scrapes student info and notifications using requests + NTLM.

    Args:
        username (str): User's university ID.
        password (str): User's password.
        domain (str): NTLM domain (default: "GUC").

    Returns:
        dict: A dictionary containing 'student_info' and 'notifications',
              or dict with 'error' on failure. Returns None on critical setup issues.
    """
    urls = config.GUC_DATA_URLS
    if len(urls) != 2:
        logger.error("Configuration error: Expected 2 GUC_DATA_URLS.")
        return {"error": "Invalid URL configuration"}

    index_url, notif_url = urls
    ntlm_user = f"{domain}\\{username}"
    userpwd = f"{ntlm_user}:{password}"  # Kept for multi_fetch signature compat

    try:
        start_scrape_time = perf_counter()
        logger.info(f"Starting requests scrape for {username}")
        results, errors = multi_fetch(urls, userpwd)
        duration = perf_counter() - start_scrape_time
        logger.info(
            f"Requests multi_fetch part finished in {duration:.3f}s for {username}"
        )

        # --- Check for critical failures ---
        if len(errors) == len(urls):
            # All fetches failed
            error_summary = "; ".join(f"{k}: {v}" for k, v in errors.items())
            logger.error(
                f"GUC data scrape failed for {username}. All fetches failed: {error_summary}"
            )
            # Check if *all* errors indicate auth failure
            if all(
                "Auth" in msg or "401" in msg or "login" in msg
                for msg in errors.values()
            ):
                return {"error": "Authentication failed"}
            else:
                return {"error": f"All URL fetches failed: {error_summary}"}

        # --- Process results (even if some errors occurred) ---
        student_html = results.get(index_url, "")
        notif_html = results.get(notif_url, "")

        # Double-check content for auth failures if fetch status seemed ok
        if index_url not in errors and (
            "Login Failed!" in student_html or "Object moved" in student_html
        ):
            logger.warning(
                f"Auth failure detected in fetched index content for {username}"
            )
            errors[index_url] = "Authentication failed (content check)"
            # If index failed auth, consider it a total failure
            return {"error": "Authentication failed"}
        if notif_url not in errors and (
            "Login Failed!" in notif_html or "Object moved" in notif_html
        ):
            logger.warning(
                f"Auth failure detected in fetched notifications content for {username}"
            )
            errors[notif_url] = "Authentication failed (content check)"
            # Maybe allow partial success if index worked but notif failed auth? For now, fail hard if index fails.

        # --- Parsing ---
        parse_start_time = perf_counter()
        student_info = parse_student_info(student_html)
        notifications = parse_notifications(notif_html)
        parse_duration = perf_counter() - parse_start_time
        logger.info(f"Parsing finished in {parse_duration:.3f}s for {username}")

        # --- Check parsing results ---
        student_info_valid = any(v for k, v in student_info.items())
        if not student_info_valid and not notifications:
            # Parsing failed to find anything, even if fetches seemed ok (or partially ok)
            if not errors:  # Fetches were ok, parsing failed
                logger.warning(
                    f"Parsing failed to extract any data for {username}, despite successful fetch."
                )
                return {"error": "Parsing failed to extract any data"}
            else:  # Some fetches failed AND parsing yielded nothing from the successful ones
                error_summary = "; ".join(f"{k}: {v}" for k, v in errors.items())
                logger.warning(
                    f"Parsing extracted no data for {username}. Fetch errors were: {error_summary}"
                )
                return {
                    "error": f"Fetching failed for some URLs and no data extracted: {error_summary}"
                }

        # --- Success or Partial Success ---
        final_data = {"student_info": student_info, "notifications": notifications}
        if errors:
            final_data["fetch_warnings"] = errors  # Include non-critical errors
            logger.warning(
                f"GUC data scrape for {username} completed with fetch warnings: {errors}"
            )

        logger.info(f"Successfully scraped GUC data (sync) for {username}")
        return final_data

    except Exception as e:
        logger.error(
            f"Unexpected error in scrape_guc_data_fast for {username}: {e}",
            exc_info=True,
        )
        return {"error": f"An unexpected error occurred during scraping: {e}"}


# Keep alias for consistency if needed, points to the sync function now
scrape_guc_data = scrape_guc_data_fast
