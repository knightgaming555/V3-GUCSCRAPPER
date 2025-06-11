# scraping/schedule.py
import re
import logging
from bs4 import BeautifulSoup
import requests
import os  # Added for path joining if needed
from datetime import datetime  # Added, was missing

# Use core session creation and request making helpers
from .core import create_session, make_request
from utils.helpers import extract_v_param
from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

# --- Schedule Parsing Helpers ---


def extract_schedule_details_from_cell(cell_html: str) -> dict:
    """
    Extracts structured schedule information (Course, Type, Location)
    from the HTML content of a single schedule table cell.

    Uses BeautifulSoup for robust parsing.
    """
    details = {"Course_Name": "Unknown", "Type": "Unknown", "Location": "Unknown"}
    if not cell_html or "Free" in cell_html:
        details.update({"Type": "Free", "Location": "Free", "Course_Name": "Free"})
        return details

    try:
        soup = BeautifulSoup(cell_html, "lxml")

        # --- Try common patterns first ---

        # Pattern 1: Lecture spans like <span id="...XlblDayNum_PerNum">...</span>
        lecture_span = soup.select_one(
            "span[id^='ContentPlaceHolderright_ContentPlaceHoldercontent_Xlbl']"
        )
        if lecture_span:
            span_text = lecture_span.get_text(separator=" ", strip=True)
            # Extract location (e.g., H1, D5.01, C7.203) - more flexible regex
            location_match = re.search(
                r"([A-Z]\d+(\.\d+)?\b)$", span_text
            )  # Matches at the end of the string
            location = location_match.group(1) if location_match else "Unknown"
            details["Location"] = location

            # Extract course name (remove "Lecture" and location)
            course_name_part = span_text.replace("Lecture", "").strip()
            if location != "Unknown" and course_name_part.endswith(location):
                details["Course_Name"] = course_name_part[: -len(location)].strip()
            else:
                details["Course_Name"] = course_name_part
            details["Type"] = "Lecture"
            return details  # Found lecture, assume this is the primary content

        # Pattern 2: Tutorial/Lab with <small> tag
        small_tag = soup.select_one("small")
        if small_tag:
            parent_div = small_tag.find_parent("div")  # Often contained in a div
            container = (
                parent_div if parent_div else soup
            )  # Use soup as fallback container

            text_nodes = [text for text in container.stripped_strings]
            # Filter out potential empty strings that might result from stripping complex structures
            text_nodes = [node for node in text_nodes if node]

            if text_nodes:
                # Course name is usually the first piece of text
                details["Course_Name"] = text_nodes[0]
                # Type is the text of the <small> tag itself
                details["Type"] = small_tag.get_text(strip=True)

                # Location hunting: Check subsequent text nodes
                # Look for common location patterns (H1, D5.01, etc.)
                location = "Unknown"
                for i in range(1, len(text_nodes)):
                    node = text_nodes[i]
                    # Flexible regex for locations
                    if re.fullmatch(r"[A-Z]\d+(\.\d+)?", node):
                        location = node
                        break
                    # Sometimes location might be combined like "Tut C1.04"
                    elif details["Type"] in node:
                        potential_loc = node.replace(details["Type"], "").strip()
                        if re.fullmatch(r"[A-Z]\d+(\.\d+)?", potential_loc):
                            location = potential_loc
                            break
                details["Location"] = location
            return details  # Found Tut/Lab with <small>, assume primary content

        # Pattern 3: Tutorial/Lab within a nested <table>
        nested_table = soup.select_one("table")  # Simpler selector
        if nested_table:
            tds = nested_table.select("td")
            if len(tds) >= 3:  # Need at least Course, Loc, Type
                course_name_parts = []
                location = "Unknown"
                type_str = "Unknown"

                # Course Name usually in first TD
                course_name_parts.append(tds[0].get_text(strip=True))

                # Location often in second TD
                loc_text = tds[1].get_text(strip=True)
                if re.fullmatch(r"[A-Z]\d+(\.\d+)?", loc_text):
                    location = loc_text

                # Type often in third TD, sometimes combined with group#
                type_text = tds[2].get_text(strip=True)
                type_match = re.search(r"(Tut|Lab)", type_text, re.IGNORECASE)
                if type_match:
                    type_str = type_match.group(0).capitalize()
                    # Append group number/identifier if present
                    group_part = re.sub(
                        r"(Tut|Lab)", "", type_text, flags=re.IGNORECASE
                    ).strip()
                    if group_part:
                        # Append to course name for clarity (e.g., "CSEN 401 1")
                        course_name_parts.append(group_part)

                details["Course_Name"] = " ".join(course_name_parts).strip()
                details["Location"] = location
                details["Type"] = type_str
                return details  # Found Tut/Lab in table, assume primary

        # Fallback: If no patterns matched, try getting all text
        all_text = soup.get_text(separator=" ", strip=True)
        if all_text:
            logger.debug(
                f"No specific pattern matched cell, using fallback text: '{all_text}'"
            )
            details["Course_Name"] = all_text  # Use combined text as course name
            # Try to guess type/location from text (less reliable)
            if "Lecture" in all_text:
                details["Type"] = "Lecture"
            elif "Tut" in all_text:
                details["Type"] = "Tut"
            elif "Lab" in all_text:
                details["Type"] = "Lab"
            loc_match = re.search(
                r"([A-Z]\d+(\.\d+)?)\b", all_text
            )  # Find potential location anywhere
            if loc_match:
                details["Location"] = loc_match.group(1)

    except Exception as e:
        logger.error(
            f"Error parsing schedule cell: {e}\nHTML: {cell_html[:200]}...",
            exc_info=False,
        )  # Log snippet
        details.update(
            {"Type": "Error", "Location": "Error", "Course_Name": "Parsing Failed"}
        )

    # Final check: If course name is still Unknown but type isn't Free/Error, log it.
    if details["Course_Name"] == "Unknown" and details["Type"] not in ["Free", "Error"]:
        logger.warning(
            f"Parsed schedule cell resulted in Unknown course name. Text: '{soup.get_text(strip=True)}'. HTML: {cell_html[:200]}..."
        )

    return details


def parse_schedule_html(html: str) -> dict:
    """Parses the full schedule page HTML into a structured dictionary."""
    schedule = {}
    if not html:
        logger.warning("parse_schedule_html received empty HTML.")
        return schedule

    try:
        soup = BeautifulSoup(html, "lxml")
        # Find the main schedule table (ID usually ends with _XtblSched)
        schedule_table = soup.find("table", id=lambda x: x and x.endswith("_XtblSched"))
        if not schedule_table:
            # !!! Log the error but DO NOT RETURN YET - attempt fallback row search !!!
            logger.error(
                "Could not find the main schedule table (_XtblSched). Attempting fallback row search."
            )
            # Fallback: Try finding rows directly in the document
            rows = soup.select(
                "tr[id^='ContentPlaceHolderright_ContentPlaceHoldercontent_Xrw']"
            )
            if not rows:
                logger.error("Fallback row search also failed. Cannot parse schedule.")
                return {}  # Return empty if fallbacks also fail
        else:
            # Found the table, find rows within it
            rows = schedule_table.select(
                "tr[id^='ContentPlaceHolderright_ContentPlaceHoldercontent_Xrw']"
            )

        if not rows:
            logger.warning("No schedule rows found (either in table or via fallback).")
            return {}

        period_names = [
            "First Period",
            "Second Period",
            "Third Period",
            "Fourth Period",
            "Fifth Period",
        ]

        for row in rows:
            day = "Unknown Day"
            try:
                # Find day cell (usually first td, specific attrs help)
                day_cell = row.find("td", align="center", valign="middle", width="80")
                if not day_cell:
                    day_cell = row.find("td")  # Fallback
                if day_cell:
                    day = day_cell.get_text(strip=True)

                # Find period cells (usually have width='180')
                period_cells = row.select("td[width='180']")
                if not period_cells:  # Fallback if width attr is missing
                    all_tds = row.find_all("td", recursive=False)
                    if day_cell and all_tds and all_tds[0] == day_cell:
                        period_cells = all_tds[1:]  # Exclude day cell if it was first
                    # Take up to 5 cells after the potential day cell OR just first 5 if day cell not first/found
                    elif len(all_tds) > 1 and day_cell and all_tds[0] == day_cell:
                        period_cells = all_tds[1:6]
                    elif len(all_tds) >= 5:  # Assume first isn't day or day missing
                        period_cells = all_tds[:5]  # Take first 5 as periods
                    else:  # Not enough cells
                        logger.warning(
                            f"Could not identify period cells reliably for day '{day}'. Skipping row."
                        )
                        continue

                day_schedule = {}
                num_periods_found = len(period_cells)
                for i, period_cell in enumerate(period_cells):
                    if i < len(period_names):  # Safety check
                        details = extract_schedule_details_from_cell(str(period_cell))
                        day_schedule[period_names[i]] = details
                    else:
                        logger.warning(
                            f"Found more period cells ({num_periods_found}) than expected ({len(period_names)}) for day '{day}'."
                        )
                        break  # Stop processing extra cells

                # Ensure 5 periods exist, filling with Free if necessary
                for i in range(num_periods_found, len(period_names)):
                    logger.debug(
                        f"Adding missing period '{period_names[i]}' as Free for day '{day}'."
                    )
                    day_schedule[period_names[i]] = {
                        "Type": "Free",
                        "Location": "Free",
                        "Course_Name": "Free",
                    }

                if day_schedule and day != "Unknown Day":
                    schedule[day] = day_schedule
                elif day != "Unknown Day":
                    logger.warning(f"No valid period data extracted for day '{day}'.")

            except Exception as e_row:
                logger.error(
                    f"Error processing schedule row for day '{day}': {e_row}",
                    exc_info=True,
                )

        # Order the days correctly
        day_order = ["Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday"]
        sorted_schedule = {
            day: schedule.get(day, {}) for day in day_order if day in schedule
        }

        if not sorted_schedule:
            logger.warning(
                "Schedule parsing finished, but no valid days were extracted."
            )

        return sorted_schedule

    except Exception as e_main:
        logger.error(f"Critical error parsing schedule HTML: {e_main}", exc_info=True)
        return {}  # Return empty on major error


# --- Main Scraping Function ---


def scrape_schedule(username: str, password: str) -> dict | None:
    """
    Scrapes the user's schedule from the GUC website.

    Handles NTLM authentication and JavaScript redirection ('v' parameter).

    Returns:
        dict: Parsed schedule data on success (can be {} if parsing finds nothing).
        dict: Containing an 'error' key on fetch/auth failure.
        None: If a critical unexpected error occurs during the process.
    """
    base_url = config.BASE_SCHEDULE_URL
    session = create_session(username, password)
    schedule_data = None
    error_reason = "Unknown error"

    try:
        logger.info(
            f"Attempting to fetch initial schedule page for {username}: {base_url}"
        )
        response_initial = make_request(session, base_url, method="GET")

        if not response_initial:
            error_reason = (
                "Initial request failed (timeout, connection error, or auth failure)"
            )
            logger.error(f"Schedule scraping failed for {username}: {error_reason}")
            return {"error": error_reason}  # Return error dict

        initial_html = response_initial.text

        if "Login Failed!" in initial_html or "Object moved" in initial_html:
            # Check more reliably for login redirect
            soup_login_check = BeautifulSoup(initial_html, "lxml")
            if soup_login_check.find(
                "a", href=lambda x: x and "login.aspx" in x.lower()
            ):
                error_reason = "Authentication failed (redirect to login)"
                logger.warning(
                    f"Schedule scraping failed for {username}: {error_reason}"
                )
                return {"error": error_reason}
            else:
                error_reason = "Authentication failed or unexpected page state after initial request"
                logger.warning(
                    f"Schedule scraping failed for {username}: {error_reason}"
                )
                return {"error": error_reason}

        v_param = extract_v_param(initial_html)
        target_html = None

        if v_param:
            schedule_url_final = f"{base_url}?v={v_param}"
            logger.info(
                f"Found 'v' parameter, fetching final schedule page: {schedule_url_final}"
            )
            response_final = make_request(session, schedule_url_final, method="GET")

            if not response_final:
                error_reason = (
                    f"Failed to fetch final schedule page (URL: {schedule_url_final})"
                )
                logger.error(f"Schedule scraping failed for {username}: {error_reason}")
                return {"error": error_reason}

            target_html = response_final.text
            if "Login Failed!" in target_html or "Object moved" in target_html:
                error_reason = "Authentication failed (detected on final schedule page)"
                logger.warning(
                    f"Schedule scraping failed for {username}: {error_reason}"
                )
                return {"error": error_reason}

        else:
            soup_check = BeautifulSoup(initial_html, "lxml")
            if soup_check.find("table", id=lambda x: x and x.endswith("_XtblSched")):
                logger.info(
                    "Already on schedule page (no 'v' parameter found/needed). Parsing initial response."
                )
                target_html = initial_html
            # --- Allow proceeding even if 'v' param AND main table not found ---
            # The parser will try fallback methods. Log a warning.
            else:
                logger.warning(
                    "Could not find 'v' parameter OR main schedule table ID. Proceeding with parsing attempt using fallbacks."
                )
                target_html = initial_html  # Try parsing the initial HTML anyway

        # --- Parse the final HTML ---
        if target_html:
            # --- Add HTML Logging ---
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"debug_schedule_{username}_{timestamp}.html"
                # Ensure project_root is defined or use relative path
                project_root_dir = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..")
                )
                filepath = os.path.join(
                    project_root_dir, filename
                )  # Save in project root
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(target_html)
                logger.info(f"Saved schedule HTML for debugging to: {filepath}")
            except Exception as log_err:
                logger.error(f"Failed to save debug HTML: {log_err}")
            # --- End HTML Logging ---

            logger.info(f"Parsing schedule HTML for {username}")
            schedule_data = parse_schedule_html(
                target_html
            )  # Returns {} on complete parse failure

            # ---> MODIFIED LOGIC <---
            # Check if the returned dictionary is non-empty (parsing succeeded at least partially)
            if schedule_data:
                logger.info(
                    f"Successfully parsed schedule content for {username} (potentially using fallbacks)."
                )
                # Return the valid data dictionary (even if parser logged errors)
                return schedule_data
            else:
                # Parsing returned an empty dict {}, indicating true failure to extract content
                error_reason = "Successfully fetched schedule page, but failed to parse ANY content."
                logger.error(f"Schedule scraping failed for {username}: {error_reason}")
                # Return the error dictionary ONLY if parsing truly yielded nothing
                return {"error": error_reason}
        else:
            error_reason = "Target HTML for parsing was unexpectedly empty."
            logger.error(f"Schedule scraping failed for {username}: {error_reason}")
            return {"error": error_reason}

    except Exception as e:
        logger.exception(
            f"Unexpected error during schedule scraping for {username}: {e}"
        )
        # Return None for critical unexpected errors
        return None


# --- Schedule Filtering Function ---
def filter_schedule_details(schedule_data: dict) -> dict:
    """Filters the parsed schedule to include only essential details."""
    if not isinstance(schedule_data, dict):
        logger.warning(
            "filter_schedule_details received non-dict input, returning empty dict."
        )
        return {}

    filtered_schedule = {}
    for day, periods in schedule_data.items():
        if not isinstance(periods, dict):
            logger.warning(
                f"Skipping day '{day}' due to invalid periods format: {type(periods)}"
            )
            continue
        filtered_periods = {}
        for period_name, period_details in periods.items():
            if isinstance(period_details, dict):
                filtered_periods[period_name] = {
                    "Course_Name": period_details.get("Course_Name", "N/A"),
                    "Type": period_details.get("Type", "N/A"),
                    "Location": period_details.get("Location", "N/A"),
                }
            else:
                logger.warning(
                    f"Invalid period_details format for {day} - {period_name}: {period_details}"
                )
                filtered_periods[period_name] = {
                    "Course_Name": "Error",
                    "Type": "Error",
                    "Location": "Invalid Data",
                }
        filtered_schedule[day] = filtered_periods
    return filtered_schedule
