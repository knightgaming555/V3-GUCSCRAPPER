# scraping/attendance.py
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests
import time
import re  # Added for potential course code extraction
import os  # Added for path joining
from datetime import datetime  # Added for debug filename

# Use core session creation and request making helpers
from .core import create_session, make_request

# Use helper for v_param extraction
from utils.helpers import extract_v_param

# Import config singleton
from config import config

logger = logging.getLogger(__name__)

# --- Attendance Parsing Functions ---


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
            return []  # Return empty list

        course_attendance = []
        rows = attendance_table.find_all("tr")
        if len(rows) <= 1:
            logger.info("Attendance detail table found but is empty.")
            return []  # Return empty list

        for row_idx, row in enumerate(rows[1:]):
            cells = row.find_all("td")
            if len(cells) >= 3:
                try:
                    status_text = cells[1].get_text(strip=True)
                    status = status_text if status_text else None
                    session_desc = (
                        cells[2].get_text(strip=True)
                        if cells[2].get_text(strip=True)
                        else None
                    )
                    course_attendance.append(
                        {"status": status, "session": session_desc}
                    )
                except Exception as e_cell:
                    logger.error(
                        f"Error extracting attendance row cells (Row {row_idx+1}): {e_cell}. Row HTML: {row}",
                        exc_info=False,
                    )
            else:
                logger.warning(
                    f"Skipping attendance row (Row {row_idx+1}) - insufficient cells ({len(cells)}). Row HTML: {row}"
                )

        return course_attendance
    except Exception as e:
        logger.error(f"Error parsing attendance detail table: {e}", exc_info=True)
        return None


def _parse_absence_summary(soup: BeautifulSoup) -> dict:
    """Parses the DG_AbsenceReport table to get absence levels per course."""
    absence_summary = {}  # {normalized_course_name: absence_level}
    try:
        summary_table = soup.find("table", id="DG_AbsenceReport")
        if not summary_table:
            logger.info("Absence summary table 'DG_AbsenceReport' not found.")
            return absence_summary

        rows = summary_table.find_all("tr")
        if len(rows) <= 1:
            logger.info("Absence summary table found but is empty.")
            return absence_summary

        headers = [
            th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])
        ]
        code_index, level_index, name_index = -1, -1, -1

        try:
            code_index = headers.index("code")
        except ValueError:
            logger.warning("Could not find 'code' header in absence summary.")
        try:
            level_index = headers.index("absencelevel")
        except ValueError:
            logger.warning("Could not find 'absencelevel' header in absence summary.")
        try:
            name_index = headers.index("name")
        except ValueError:
            logger.warning("Could not find 'name' header in absence summary.")

        if code_index == -1 or level_index == -1 or name_index == -1:
            logger.warning("Using default indices for absence summary table.")
            code_index = 1
            name_index = 2
            level_index = 3

        for row in rows[1:]:
            cells = row.find_all("td")
            max_needed_index = max(code_index, level_index, name_index)
            if max_needed_index == -1:
                max_needed_index = 3
            if len(cells) > max_needed_index:
                try:
                    course_code = (
                        cells[code_index].get_text(strip=True)
                        if code_index != -1
                        else ""
                    )
                    absence_level_str = (
                        cells[level_index].get_text(strip=True)
                        if level_index != -1
                        else ""
                    )
                    course_name = (
                        cells[name_index].get_text(strip=True)
                        if name_index != -1
                        else ""
                    )

                    level_match = re.search(r"\d+", absence_level_str)
                    absence_level = (
                        f"Level {level_match.group(0)}"
                        if level_match
                        else "No Warning Level"
                    )

                    if course_name:
                        normalized_summary_name = re.sub(
                            r"\s+", " ", course_name
                        ).strip()
                        absence_summary[normalized_summary_name] = absence_level
                    elif course_code:
                        absence_summary[course_code] = absence_level
                except IndexError:
                    logger.warning(
                        f"Index out of bounds parsing absence summary row. Indices: code={code_index}, level={level_index}, name={name_index}. Cells={len(cells)}"
                    )
                except Exception as e_cell:
                    logger.error(
                        f"Error parsing absence summary row: {e_cell}. Row: {row}",
                        exc_info=False,
                    )
            else:
                logger.warning(
                    f"Skipping absence summary row - insufficient cells ({len(cells)})."
                )

    except Exception as e:
        logger.error(f"Error parsing absence summary table: {e}", exc_info=True)

    logger.info(f"Parsed absence summary: {absence_summary}")
    return absence_summary


def _get_attendance_details_for_all_courses(
    session: requests.Session, attendance_url_with_v: str
) -> dict | None:
    """
    Fetches the main attendance page, parses absence summary, then POSTs for
    each course to get detailed attendance and combines the results.

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
        if "Login Failed!" in initial_html or "Object moved" in initial_html:
            logger.warning(
                "Attendance details failed: Authentication failed (detected on detail page)."
            )
            return None

        soup_initial = BeautifulSoup(initial_html, "lxml")

        absence_summary = _parse_absence_summary(soup_initial)

        course_dropdown = soup_initial.find(
            "select", id="ContentPlaceHolderright_ContentPlaceHoldercontent_DDL_Courses"
        )
        if not course_dropdown:
            logger.warning("Course dropdown '...DDL_Courses' not found.")
            if absence_summary:
                logger.warning("Absence summary found, but no course dropdown.")
            return {}

        options = course_dropdown.find_all("option")
        if not options or len(options) <= 1:
            logger.info("Course dropdown found but contains no actual courses.")
            return {}

        viewstate = soup_initial.find("input", {"name": "__VIEWSTATE"})
        viewstate_gen = soup_initial.find("input", {"name": "__VIEWSTATEGENERATOR"})
        event_validation = soup_initial.find("input", {"name": "__EVENTVALIDATION"})

        if not (viewstate and viewstate_gen and event_validation):
            logger.error("Missing essential ASP.NET form elements on attendance page.")
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
            dropdown_course_name = option.text.strip()

            if not course_value or course_value == "0" or not dropdown_course_name:
                continue

            logger.debug(f"Processing course from dropdown: {dropdown_course_name}")

            normalized_dropdown_name = re.sub(r"\s+", " ", dropdown_course_name).strip()
            matched_level = absence_summary.get(
                normalized_dropdown_name, "No Warning Level"
            )

            if matched_level == "No Warning Level":
                potential_match_name = dropdown_course_name
                if " - " in dropdown_course_name:
                    parts = dropdown_course_name.split(" - ", 1)
                    if len(parts) > 1:
                        potential_match_name = parts[1].strip()
                if potential_match_name != dropdown_course_name:
                    normalized_potential_name = re.sub(
                        r"\s+", " ", potential_match_name
                    ).strip()
                    matched_level = absence_summary.get(
                        normalized_potential_name, "No Warning Level"
                    )

            form_data = base_form_data.copy()
            form_data[
                "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$DDL_Courses"
            ] = course_value

            response_course = make_request(
                session,
                attendance_url_with_v,
                method="POST",
                data=form_data,
                timeout=(5, 15),
            )

            course_attendance_list = []
            if response_course:
                soup_course = BeautifulSoup(response_course.content, "lxml")
                parsed_list = _parse_attendance_for_course(soup_course)
                if parsed_list is not None:
                    course_attendance_list = parsed_list
                else:
                    logger.error(
                        f"Failed to parse details for course '{dropdown_course_name}'."
                    )
            else:
                logger.error(
                    f"POST request failed for course '{dropdown_course_name}'."
                )

            final_attendance_data[dropdown_course_name] = {
                "absence_level": matched_level,
                "sessions": course_attendance_list,
            }
            time.sleep(0.1)  # Small delay

        return final_attendance_data

    except Exception as e:
        logger.exception(f"Error getting attendance details for all courses: {e}")
        return None


# --- Main Attendance Scraping Function ---
def scrape_attendance(username: str, password: str) -> dict | None:
    """
    Scrapes attendance data for all courses for a user. Fetches summary and details.

    Handles fetching the base page, extracting the 'v' parameter,
    and then retrieving details for each course via POST requests.

    Returns:
        dict: Dictionary with course names as keys. Each value is a dict containing
              'absence_level' (str) and 'sessions' (list of dicts).
        None: On critical failure.
    """
    base_url = config.BASE_ATTENDANCE_URL
    session = create_session(username, password)
    attendance_data = None

    logger.info(
        f"Starting attendance scraping for {username} from base URL: {base_url}"
    )

    try:
        # 1. Fetch base attendance page
        response_base = make_request(session, base_url, method="GET", timeout=(10, 20))
        if not response_base:
            logger.error(f"Failed to fetch base attendance page for {username}.")
            return None  # Auth or connection error

        base_html = response_base.text

        # Check for login page indicators first
        soup_login_check = BeautifulSoup(base_html, "lxml")
        if (
            "Login Failed!" in base_html
            or "Object moved" in base_html
            or (
                response_base.history
                and any("login" in r.url.lower() for r in response_base.history)
            )
            or (
                "login" in response_base.url.lower()
                and soup_login_check.find(
                    "form", action=lambda x: x and "login" in x.lower()
                )
            )
        ):
            logger.warning(
                f"Attendance scraping failed: Auth failed or redirected to login (base page)."
            )
            return None  # Indicate auth failure

        # 2. Extract 'v' parameter OR check for dropdown
        v_param = extract_v_param(base_html)  # Use the corrected helper
        attendance_url_final = None

        if v_param:
            attendance_url_final = urljoin(base_url, f"?v={v_param}")
            logger.info(f"Found 'v' parameter. Using URL: {attendance_url_final}")
        else:
            # 'v' parameter NOT found - Check if dropdown is present
            logger.warning("No 'v' parameter found in base attendance page response.")
            # Save HTML for debugging
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"debug_attendance_no_v_{username}_{timestamp}.html"
                project_root_dir = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..")
                )
                filepath = os.path.join(project_root_dir, filename)
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(base_html)
                logger.info(
                    f"Saved attendance HTML (no 'v' param) for debugging to: {filepath}"
                )
            except Exception as log_err:
                logger.error(f"Failed to save debug HTML: {log_err}")

            # Check robustly for the dropdown using the soup object we already have
            dropdown = soup_login_check.find(
                "select",
                id="ContentPlaceHolderright_ContentPlaceHoldercontent_DDL_Courses",
            )

            if dropdown:
                logger.info(
                    "Dropdown found on initial page. Proceeding without 'v' parameter using base URL."
                )
                attendance_url_final = base_url  # Use the base URL directly
            else:
                # 'v' param missing AND dropdown missing - definite failure
                logger.error(
                    f"Failed to extract 'v' parameter AND dropdown not found for {username}."
                )
                return None  # Critical failure

        # --- Should have a valid URL by now ---
        if not attendance_url_final:
            logger.error(
                f"Internal logic error: attendance_url_final not set for {username}"
            )
            return None

        # 3. Fetch details for all courses using the final URL
        attendance_data = _get_attendance_details_for_all_courses(
            session, attendance_url_final
        )

        if attendance_data is None:
            logger.error(
                f"Failed to get combined attendance details for {username} (helper returned None)."
            )
            return None

        logger.info(
            f"Successfully finished attendance scraping for {username}. Processed {len(attendance_data)} courses."
        )
        return attendance_data  # Return the combined dictionary

    except Exception as e:
        logger.exception(
            f"Unexpected error during attendance scraping for {username}: {e}"
        )
        return None
