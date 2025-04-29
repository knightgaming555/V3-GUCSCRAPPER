# scraping/attendance.py
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests
import time
import re
import os
from datetime import datetime

# Use core session creation and request making helpers
from .core import create_session, make_request

# Use helper for v_param extraction
from utils.helpers import extract_v_param

# Import config singleton
from config import config

logger = logging.getLogger(__name__)

# --- Attendance Parsing Functions ---


# _parse_attendance_for_course remains unchanged from previous version
def _parse_attendance_for_course(soup: BeautifulSoup) -> list | None:
    """
    Extracts the attendance table rows for a single, selected course.
    Returns a list of attendance records [{status: str, session: str}] or None on failure.
    """
    if not soup:
        logger.warning("_parse_attendance_for_course received None soup object.")
        return None
    try:
        attendance_table = soup.find("table", id="DG_StudentCourseAttendance")
        if not attendance_table:
            logger.info(
                "Attendance detail table 'DG_StudentCourseAttendance' not found."
            )
            return []

        course_attendance = []
        rows = attendance_table.find_all("tr")
        if len(rows) <= 1:
            logger.info("Attendance detail table found but is empty.")
            return []

        for row_idx, row in enumerate(rows[1:]):  # Skip header row
            cells = row.find_all("td")
            if len(cells) >= 3:
                try:
                    status_text = cells[1].get_text(strip=True)
                    status = status_text if status_text else None
                    session_desc = cells[2].get_text(strip=True)
                    session_desc = session_desc if session_desc else None
                    course_attendance.append(
                        {"status": status, "session": session_desc}
                    )
                except IndexError:
                    logger.warning(
                        f"Skipping attendance row (Row {row_idx+1}) due to IndexError. Cells found: {len(cells)}. Row HTML: {row}"
                    )
                except Exception as e_cell:
                    logger.error(
                        f"Error extracting attendance row cells (Row {row_idx+1}): {e_cell}. Row HTML: {row}",
                        exc_info=False,
                    )
            else:
                logger.warning(
                    f"Skipping attendance row (Row {row_idx+1}) - insufficient cells ({len(cells)} < 3). Row HTML: {row}"
                )
        return course_attendance
    except Exception as e:
        logger.error(
            f"Error parsing attendance detail table 'DG_StudentCourseAttendance': {e}",
            exc_info=True,
        )
        return None


# _parse_absence_summary remains unchanged from previous version
def _parse_absence_summary(soup: BeautifulSoup) -> dict:
    """
    Parses the DG_AbsenceReport table to get absence levels per course.
    Uses Course Code as the primary key if available, otherwise Course Name.
    Formats level as 'Level X' or 'No Warning Level'.
    """
    absence_summary = {}  # {course_code_or_name: absence_level}
    try:
        summary_table = soup.find("table", id="DG_AbsenceReport")
        if not summary_table:
            logger.info("Absence summary table 'DG_AbsenceReport' not found.")
            return absence_summary  # Return empty dict

        rows = summary_table.find_all("tr")
        if len(rows) <= 1:  # Check if only header row exists or table is empty
            logger.info("Absence summary table found but contains no data rows.")
            return absence_summary  # Return empty dict

        # --- Header Parsing ---
        headers = [
            th.get_text(strip=True).lower().replace(" ", "")
            for th in rows[0].find_all(["th", "td"])  # Handles both th and td headers
        ]
        logger.debug(f"Found absence summary headers: {headers}")

        code_index, level_index, name_index = -1, -1, -1
        try:
            code_index = headers.index("code")
        except ValueError:
            logger.warning("Could not find 'code' header by text in absence summary.")
        try:
            level_index = headers.index("absencelevel")
        except ValueError:
            logger.warning(
                "Could not find 'absencelevel' header by text in absence summary."
            )
        try:
            name_index = headers.index("name")
        except ValueError:
            logger.warning("Could not find 'name' header by text in absence summary.")

        # --- Fallback Indices based on observed HTML structure ---
        if code_index == -1 or level_index == -1 or name_index == -1:
            logger.warning(
                "One or more headers not found by text. Applying default indices based on "
                "observed structure: Code=1, Name=2, AbsenceLevel=3."
            )
            if len(headers) > 3:
                if code_index == -1:
                    code_index = 1
                if name_index == -1:
                    name_index = 2
                if level_index == -1:
                    level_index = 3
            else:
                logger.error(
                    f"Cannot apply fallback indices, table has only {len(headers)} columns. Headers: {headers}"
                )
                return absence_summary

        if code_index == -1 or level_index == -1 or name_index == -1:
            logger.error(
                f"Failed to determine all required column indices (code, name, level). Cannot parse absence summary. Indices found: C={code_index}, N={name_index}, L={level_index}"
            )
            return absence_summary

        logger.info(
            f"Using indices: Code={code_index}, Name={name_index}, Level={level_index}"
        )

        # --- Data Row Parsing ---
        for row_idx, row in enumerate(rows[1:]):  # Iterate through data rows
            cells = row.find_all("td")
            max_needed_index = max(code_index, level_index, name_index)
            if len(cells) > max_needed_index:
                try:
                    course_code = cells[code_index].get_text(
                        strip=True
                    )  # e.g., "CSEN 202"
                    absence_level_str = cells[level_index].get_text(
                        strip=True
                    )  # e.g., "1"
                    course_name = cells[name_index].get_text(
                        strip=True
                    )  # e.g., "Introduction to Computer Programming"

                    level_match = re.search(r"^\d+$", absence_level_str)
                    absence_level = (
                        f"Level {level_match.group(0)}"
                        if level_match
                        else "No Warning Level"
                    )
                    if not level_match and absence_level_str:
                        logger.warning(
                            f"Absence level cell contained non-digit text: '{absence_level_str}'. Setting to 'No Warning Level'."
                        )

                    key = None
                    if course_code:
                        key = course_code  # Use code directly, preserving space: 'CSEN 202'
                    elif course_name:
                        key = re.sub(r"\s+", " ", course_name).strip()
                        logger.debug(
                            f"Using normalized course name '{key}' as key (code was missing)."
                        )
                    else:
                        logger.warning(
                            f"Skipping absence summary row {row_idx+1} - missing both Code and Name. Row: {row}"
                        )
                        continue

                    absence_summary[key] = absence_level
                    logger.debug(
                        f"Stored absence level for key '{key}': '{absence_level}'"
                    )

                except IndexError:
                    logger.warning(
                        f"Index out of bounds parsing absence summary row {row_idx+1}. Indices: C={code_index}, L={level_index}, N={name_index}. Cells={len(cells)}. Row: {row}"
                    )
                except Exception as e_cell:
                    logger.error(
                        f"Error parsing absence summary row {row_idx+1}: {e_cell}. Row: {row}",
                        exc_info=False,
                    )
            else:
                logger.warning(
                    f"Skipping absence summary row {row_idx+1} - insufficient cells ({len(cells)} <= {max_needed_index}). Row: {row}"
                )

    except Exception as e:
        logger.error(f"General error parsing absence summary table: {e}", exc_info=True)
        return absence_summary

    logger.info(f"Parsed absence summary: {absence_summary}")
    return absence_summary


# --- MODIFIED _get_attendance_details_for_all_courses ---
def _get_attendance_details_for_all_courses(
    session: requests.Session, attendance_url_with_v: str
) -> dict | None:
    """
    Fetches the main attendance page, parses absence summary, then POSTs for
    each course to get detailed attendance and combines the results.
    **MODIFIED to prioritize Course Code lookup.**

    Returns:
        dict: Keys are course names from dropdown. Values are dicts:
              {'absence_level': str, 'sessions': list}.
              Returns None on critical failure.
              Returns empty dict {} if dropdown missing or initial fetch fails.
    """
    final_attendance_data = {}
    try:
        logger.info(
            f"Fetching initial attendance page for details: {attendance_url_with_v}"
        )
        response_initial = make_request(
            session, attendance_url_with_v, method="GET", timeout=(10, 20)
        )
        if not response_initial:
            logger.error(
                "Failed to fetch initial attendance page (with v param or base URL)."
            )
            return None

        initial_html = response_initial.text
        if (
            "Login Failed!" in initial_html
            or "Object moved" in initial_html
            or "login.aspx" in response_initial.url.lower()
        ):
            logger.warning(
                "Attendance details failed: Authentication likely failed (detected on detail page fetch/redirect)."
            )
            return None

        soup_initial = BeautifulSoup(initial_html, "lxml")

        # Parse Absence Summary (assumed correct from previous step)
        # Expecting absence_summary = {'CSEN 202': 'Level 1', ...}
        absence_summary = _parse_absence_summary(soup_initial)

        course_dropdown = soup_initial.find(
            "select", id="ContentPlaceHolderright_ContentPlaceHoldercontent_DDL_Courses"
        )
        if not course_dropdown:
            logger.warning("Course dropdown '...DDL_Courses' not found on the page.")
            if absence_summary:
                logger.info(
                    "Absence summary was found, but no course dropdown to iterate."
                )
            return {}

        options = course_dropdown.find_all("option")
        if not options or len(options) <= 1:
            logger.info("Course dropdown found but contains no actual course options.")
            return {}

        viewstate = soup_initial.find("input", {"name": "__VIEWSTATE"})
        viewstate_gen = soup_initial.find("input", {"name": "__VIEWSTATEGENERATOR"})
        event_validation = soup_initial.find("input", {"name": "__EVENTVALIDATION"})

        if not (viewstate and viewstate_gen and event_validation):
            logger.error(
                "Missing essential ASP.NET form elements (__VIEWSTATE*, __EVENTVALIDATION) on attendance page. Cannot proceed with POST requests."
            )
            return None

        base_form_data = {
            "__EVENTTARGET": "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$DDL_Courses",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": viewstate["value"],
            "__VIEWSTATEGENERATOR": viewstate_gen["value"],
            "__EVENTVALIDATION": event_validation["value"],
            "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$H_AlertText": "",
            "ctl00$ctl00$div_position": "0",
        }

        for option in options:
            course_value = option.get("value")
            dropdown_course_name = (
                option.text.strip()
            )  # e.g., "Spring 2025 - CSEN 202 - Introduction to Computer Programming"

            if not course_value or course_value == "0" or not dropdown_course_name:
                continue

            logger.debug(
                f"Processing course from dropdown: '{dropdown_course_name}' (Value: {course_value})"
            )

            # --- Absence Level Lookup (REVISED LOGIC) ---
            matched_level = "No Warning Level"  # Default
            lookup_key_code = None

            # 1. Attempt to extract Course Code from dropdown text
            #    Regex captures patterns like XXXX NNN or XXX NNNN (e.g., CSEN 202, DE 202)
            code_match = re.search(r"\b([A-Z]{2,4}\s?\d{3,4})\b", dropdown_course_name)
            if code_match:
                # Use the exact captured code (preserving space) as the primary lookup key
                lookup_key_code = code_match.group(1)  # e.g., "CSEN 202"
                logger.debug(f"Extracted potential code key: '{lookup_key_code}'")
                # Try looking up using the extracted code
                matched_level = absence_summary.get(lookup_key_code, "No Warning Level")
                if matched_level != "No Warning Level":
                    logger.info(
                        f"Found absence level '{matched_level}' using code key '{lookup_key_code}'."
                    )
                else:
                    logger.warning(
                        f"Code key '{lookup_key_code}' extracted, but not found in absence summary dict. Keys: {list(absence_summary.keys())}"
                    )
            else:
                logger.warning(
                    f"Could not extract course code pattern from dropdown text: '{dropdown_course_name}'"
                )

            # 2. Fallback: If code extraction or lookup failed, try matching the full normalized dropdown name
            #    (Less reliable, but keeps original fallback path)
            if matched_level == "No Warning Level":
                normalized_dropdown_name = re.sub(
                    r"\s+", " ", dropdown_course_name
                ).strip()
                logger.debug(
                    f"Code lookup failed. Trying full normalized dropdown name: '{normalized_dropdown_name}'"
                )
                matched_level = absence_summary.get(
                    normalized_dropdown_name, "No Warning Level"
                )
                if matched_level != "No Warning Level":
                    logger.info(
                        f"Found absence level '{matched_level}' using full dropdown name key."
                    )
                # else: # No need for another log here, the final fallback will handle it

            # 3. Final Fallback: Try matching just the name part (Least reliable)
            if matched_level == "No Warning Level":
                potential_match_name = dropdown_course_name
                # Try splitting by ' - ' to isolate name part
                if " - " in dropdown_course_name:
                    parts = dropdown_course_name.split(" - ", 2)
                    if len(parts) > 2:  # Semester - Code - Name
                        potential_match_name = parts[2].strip()
                    elif (
                        len(parts) > 1 and not lookup_key_code
                    ):  # Code - Name or Semester - Name (and code wasn't found earlier)
                        potential_match_name = parts[1].strip()

                if potential_match_name != dropdown_course_name:
                    normalized_potential_name = re.sub(
                        r"\s+", " ", potential_match_name
                    ).strip()
                    logger.debug(
                        f"Full name lookup failed. Trying potential name part: '{normalized_potential_name}'"
                    )
                    matched_level = absence_summary.get(
                        normalized_potential_name, "No Warning Level"
                    )
                    if matched_level != "No Warning Level":
                        logger.info(
                            f"Found absence level '{matched_level}' using name part key."
                        )
                    else:
                        logger.warning(
                            f"All lookup attempts failed for '{dropdown_course_name}'. Using default 'No Warning Level'."
                        )
            # --- End of REVISED Absence Level Lookup ---

            # Prepare form data for POST
            form_data = base_form_data.copy()
            form_data[
                "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$DDL_Courses"
            ] = course_value

            # Make POST request
            logger.debug(f"POSTing to select course '{dropdown_course_name}'")
            response_course = make_request(
                session,
                attendance_url_with_v,
                method="POST",
                data=form_data,
                timeout=(10, 25),
            )

            course_attendance_list = []
            if response_course:
                if (
                    "Login Failed!" in response_course.text
                    or "Object moved" in response_course.text
                    or "login.aspx" in response_course.url.lower()
                ):
                    logger.warning(
                        f"POST for course '{dropdown_course_name}' resulted in login page. Aborting further requests."
                    )
                    return None  # Auth lost

                soup_course = BeautifulSoup(response_course.content, "lxml")
                parsed_list = _parse_attendance_for_course(soup_course)
                if parsed_list is not None:
                    course_attendance_list = parsed_list
                    logger.info(
                        f"Successfully parsed {len(course_attendance_list)} session records for '{dropdown_course_name}'."
                    )
                else:
                    logger.error(
                        f"Failed to parse attendance details table for course '{dropdown_course_name}' after POST. Storing empty session list."
                    )
            else:
                logger.error(
                    f"POST request failed for course '{dropdown_course_name}'. Cannot get session details."
                )

            # Store results with the determined absence level
            final_attendance_data[dropdown_course_name] = {
                "absence_level": matched_level,  # Use the level found via revised lookup
                "sessions": course_attendance_list,
            }
            time.sleep(0.2)

        return final_attendance_data

    except requests.exceptions.RequestException as req_e:
        logger.error(
            f"Network error during attendance detail fetching: {req_e}", exc_info=True
        )
        return None
    except Exception as e:
        logger.exception(
            f"Unexpected error getting attendance details for all courses: {e}"
        )
        return None


# --- End MODIFIED _get_attendance_details_for_all_courses ---


# scrape_attendance remains unchanged from previous version
def scrape_attendance(username: str, password: str) -> dict | None:
    """
    Scrapes attendance data for all courses for a user. Fetches summary and details.

    Handles fetching the base page, extracting the 'v' parameter,
    and then retrieving details for each course via POST requests.

    Returns:
        dict: Dictionary with course names as keys. Each value is a dict containing
              'absence_level' (str) and 'sessions' (list of dicts).
        None: On critical failure (e.g., auth, network, missing form elements).
              Returns {} if attendance page is accessible but no courses found in dropdown.
    """
    base_url = config.BASE_ATTENDANCE_URL
    session = None  # Initialize session to None
    attendance_data = None

    logger.info(
        f"Starting attendance scraping for {username} from base URL: {base_url}"
    )

    try:
        # 0. Create Session
        session = create_session(username, password)
        if not session:
            return None

        # 1. Fetch base attendance page
        logger.debug(f"Attempting to fetch base attendance page: {base_url}")
        response_base = make_request(session, base_url, method="GET", timeout=(15, 30))
        if not response_base:
            logger.error(
                f"Failed to fetch base attendance page for {username}. Check connection or base URL."
            )
            return None

        base_html = response_base.text
        logger.debug(f"Base attendance page URL after fetch: {response_base.url}")
        logger.debug(f"Base response status code: {response_base.status_code}")

        # Check for login page indicators
        soup_login_check = BeautifulSoup(base_html, "lxml")
        is_login_page = (
            "Login Failed!" in base_html
            or "Object moved" in base_html
            or "login.aspx" in response_base.url.lower()
            or soup_login_check.find(
                "input", {"id": lambda x: x and "password" in x.lower()}
            )
            is not None
            or soup_login_check.find(
                "form", action=lambda x: x and "login" in x.lower()
            )
            is not None
        )

        if is_login_page:
            logger.warning(
                f"Attendance scraping failed: Auth failed or redirected to login page (detected on base page fetch)."
            )
            return None

        # 2. Extract 'v' parameter OR check for dropdown
        v_param = extract_v_param(base_html)
        attendance_url_final = None

        if v_param:
            attendance_url_final = urljoin(response_base.url, f"?v={v_param}")
            logger.info(f"Found 'v' parameter. Using URL: {attendance_url_final}")
        else:
            logger.warning("No 'v' parameter found in base attendance page response.")
            # Save HTML for debugging if 'v' is missing
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_username = re.sub(r'[\\/*?:"<>|]', "_", username)
                filename = f"debug_attendance_no_v_{safe_username}_{timestamp}.html"
                project_root_dir = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..")
                )
                debug_dir = os.path.join(project_root_dir, "debug_html")
                os.makedirs(debug_dir, exist_ok=True)
                filepath = os.path.join(debug_dir, filename)
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(f"<!-- URL: {response_base.url} -->\n")
                    f.write(f"<!-- Status Code: {response_base.status_code} -->\n")
                    f.write(f"<!-- History: {response_base.history} -->\n")
                    f.write(base_html)
                logger.info(
                    f"Saved attendance HTML (no 'v' param) for debugging to: {filepath}"
                )
            except Exception as log_err:
                logger.error(f"Failed to save debug HTML: {log_err}")

            dropdown = soup_login_check.find(
                "select",
                id="ContentPlaceHolderright_ContentPlaceHoldercontent_DDL_Courses",
            )

            if dropdown:
                logger.info(
                    "Dropdown found on initial page. Proceeding without 'v' parameter using the page's final URL."
                )
                attendance_url_final = response_base.url
            else:
                logger.error(
                    f"Failed to extract 'v' parameter AND course dropdown not found for {username} on page {response_base.url}. Cannot proceed."
                )
                return None

        if not attendance_url_final:
            logger.error(
                f"Internal logic error: attendance_url_final was not set for {username} despite checks."
            )
            return None

        # 3. Fetch details for all courses using the final URL
        logger.info(f"Proceeding to fetch details using URL: {attendance_url_final}")
        # Call the MODIFIED function
        attendance_data = _get_attendance_details_for_all_courses(
            session, attendance_url_final
        )

        if attendance_data is None:
            logger.error(
                f"Failed to get combined attendance details for {username} (_get_attendance_details_for_all_courses returned None)."
            )
            return None
        elif not attendance_data:
            logger.info(
                f"Attendance scraping finished for {username}, but no course details were found (e.g., empty dropdown)."
            )
            return {}
        else:
            logger.info(
                f"Successfully finished attendance scraping for {username}. Processed {len(attendance_data)} courses."
            )
            return attendance_data

    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout occurred during attendance scraping for {username}.",
            exc_info=True,
        )
        return None
    except requests.exceptions.RequestException as req_e:
        logger.error(
            f"A network request exception occurred during attendance scraping for {username}: {req_e}",
            exc_info=True,
        )
        return None
    except Exception as e:
        logger.exception(
            f"Unexpected error during attendance scraping for {username}: {e}"
        )
        return None
    finally:
        if session:
            session.close()
            logger.debug("Requests session closed.")
