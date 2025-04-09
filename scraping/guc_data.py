# scraping/guc_data.py
import logging
import traceback
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import time  # For perf_counter
from time import perf_counter  # Explicit import
import pycurl  # Use pycurl
from io import BytesIO
import json  # For error dicts

from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)


# --- PycURL Fetching ---
def multi_fetch(urls: list[str], userpwd: str) -> tuple[dict, dict]:
    """Fetches multiple URLs concurrently using pycurl.CurlMulti."""
    multi = pycurl.CurlMulti()
    handles = []
    buffers = {}
    results = {}
    errors = {}
    start_time = perf_counter()

    # Prepare handles
    for url in urls:
        buffer = BytesIO()
        try:
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.setopt(c.HTTPAUTH, pycurl.HTTPAUTH_NTLM)
            c.setopt(c.USERPWD, userpwd)
            c.setopt(c.WRITEDATA, buffer)
            c.setopt(c.FOLLOWLOCATION, True)
            c.setopt(c.TIMEOUT, config.DEFAULT_REQUEST_TIMEOUT)  # Use config timeout
            # Disable SSL verification if configured
            c.setopt(c.SSL_VERIFYPEER, 1 if config.VERIFY_SSL else 0)
            c.setopt(c.SSL_VERIFYHOST, 2 if config.VERIFY_SSL else 0)
            c.setopt(
                c.USERAGENT, "UnisightApp/Client (Python-PycURL/Sync)"
            )  # Identify client
            multi.add_handle(c)
            handles.append(c)
            buffers[c] = buffer  # Use handle as key for easy lookup later
        except pycurl.error as e:
            logger.error(f"Error setting up pycurl handle for {url}: {e}")
            errors[url] = f"pycurl setup error: {e}"
            results[url] = ""  # Ensure result entry exists even on setup failure

    # Perform requests
    num_handles = len(handles)
    while num_handles:
        try:
            ret, num_handles_active = multi.perform()
            # Check for errors during perform
            # ret might be pycurl.E_OK even if some transfers failed, need checkinfo later
            if ret != pycurl.E_OK and ret != pycurl.E_CALL_MULTI_PERFORM:
                logger.warning(f"multi.perform() returned error code: {ret}")

            num_handles = num_handles_active
            if num_handles_active:
                # Wait for activity or timeout
                multi.select(1.0)  # Wait up to 1 second
        except Exception as e_perform:
            logger.error(
                f"Exception during multi.perform/select: {e_perform}", exc_info=True
            )
            # Mark remaining handles as error? Difficult to know which one caused it.
            # Best effort: try checkinfo below.
            break  # Exit loop on perform error

    duration = perf_counter() - start_time
    logger.debug(f"pycurl multi_fetch completed in {duration:.3f}s")

    # Process results
    while True:
        try:
            num_q, ok_list, err_list = multi.info_read()
            for handle in ok_list:
                url = handle.getinfo(
                    pycurl.EFFECTIVE_URL
                )  # Get URL associated with this handle
                http_code = handle.getinfo(pycurl.HTTP_CODE)
                buffer = buffers.get(handle)
                if buffer:
                    try:
                        content = buffer.getvalue().decode("utf-8", errors="replace")
                        results[url] = content
                        logger.debug(f"Fetch success for {url} (Status: {http_code})")
                        # Check for GUC application errors even if status is 200
                        if "Login Failed!" in content or "Object moved" in content:
                            logger.warning(
                                f"Auth failure detected in content for {url}"
                            )
                            errors[url] = "Authentication failed (content check)"
                    except Exception as decode_err:
                        logger.error(f"Error decoding response for {url}: {decode_err}")
                        errors[url] = f"Decode error: {decode_err}"
                        results[url] = ""  # Store empty on decode error
                else:
                    logger.error(f"Buffer not found for successful handle: {url}")
                    errors[url] = "Internal buffer error"
                    results[url] = ""

                multi.remove_handle(handle)
                handle.close()

            for handle, err_no, err_msg in err_list:
                url = handle.getinfo(pycurl.EFFECTIVE_URL)
                http_code = handle.getinfo(
                    pycurl.HTTP_CODE
                )  # Get code even on error if possible
                logger.error(
                    f"Fetch error for {url} (Status: {http_code}): {err_no} - {err_msg}"
                )
                errors[url] = f"pycurl error {err_no}: {err_msg}"
                results[url] = ""  # Store empty on error
                multi.remove_handle(handle)
                handle.close()

            if num_q == 0:
                break
        except Exception as e_info:
            logger.error(f"Exception during multi.info_read: {e_info}", exc_info=True)
            break  # Exit loop on info_read error

    # Cleanup remaining handles (shouldn't be needed if info_read worked)
    for handle in handles:
        try:
            multi.remove_handle(handle)
        except:
            pass
        try:
            handle.close()
        except:
            pass
    try:
        multi.close()
    except:
        pass

    # Ensure results dict contains entries for all original URLs
    original_urls_set = set(urls)
    for url in original_urls_set:
        if url not in results:
            results[url] = ""  # Ensure entry exists if fetch failed badly
            if url not in errors:
                errors[url] = "Fetch failed (unknown reason)"

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
    Synchronously scrapes student info and notifications using pycurl.

    Args:
        username (str): User's university ID.
        password (str): User's password.
        domain (str): NTLM domain (default: "GUC").

    Returns:
        dict: A dictionary containing 'student_info' and 'notifications',
              or dict with 'error' on failure. Returns None on critical pycurl setup issues.
    """
    urls = config.GUC_DATA_URLS
    if len(urls) != 2:
        logger.error("Configuration error: Expected 2 GUC_DATA_URLS.")
        return {"error": "Invalid URL configuration"}

    index_url, notif_url = urls
    ntlm_user = f"{domain}\\{username}"
    userpwd = f"{ntlm_user}:{password}"  # Format for pycurl

    try:
        start_scrape_time = perf_counter()
        logger.info(f"Starting pycurl scrape for {username}")
        results, errors = multi_fetch(urls, userpwd)
        duration = perf_counter() - start_scrape_time
        logger.info(
            f"Pycurl multi_fetch part finished in {duration:.3f}s for {username}"
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

    except pycurl.error as e:
        # Errors during pycurl setup or critical multi.perform issues
        error_code, error_msg = e.args
        logger.error(
            f"Critical PycURL error during scraping for {username}: Code {error_code} - {error_msg}",
            exc_info=True,
        )
        return {"error": f"Network layer error during scraping: {error_msg}"}
    except Exception as e:
        logger.error(
            f"Unexpected error in scrape_guc_data_fast for {username}: {e}",
            exc_info=True,
        )
        return {"error": f"An unexpected error occurred during scraping: {e}"}


# Keep alias for consistency if needed, points to the sync function now
scrape_guc_data = scrape_guc_data_fast
