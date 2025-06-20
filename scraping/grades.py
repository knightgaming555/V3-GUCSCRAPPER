# scraping/grades.py
import logging
from bs4 import BeautifulSoup
import concurrent.futures # Renamed from 'concurrent' for clarity
import requests # Not used directly in this file but good to keep if session might be passed around
import time

from .core import create_session, make_request
from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

def _clean_string(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return " ".join(text.strip().split())


def _parse_midterm_grades(soup: BeautifulSoup) -> dict:
    # ... (no changes needed here, assuming it's stable) ...
    midterm_results = {}
    midterm_table = soup.find("table", id="ContentPlaceHolderright_ContentPlaceHoldercontent_midDg")
    if not midterm_table: return midterm_results
    rows = midterm_table.find_all("tr")
    if len(rows) <= 1: return midterm_results
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) >= 2:
            try:
                course_name = _clean_string(cells[0].get_text(strip=True))
                percentage = _clean_string(cells[1].get_text(strip=True))
                if course_name: midterm_results[course_name] = percentage
            except Exception as e: logger.error(f"Error parsing midterm grade row: {e}. Row: {row}", exc_info=False)
        else: logger.warning(f"Skipping midterm row with {len(cells)} cells.")
    return midterm_results


def _parse_subject_codes(soup: BeautifulSoup) -> dict:
    # ... (no changes needed here, assuming it's stable) ...
    subject_codes = {}
    subject_dropdown = soup.find("select", id="ContentPlaceHolderright_ContentPlaceHoldercontent_smCrsLst")
    if not subject_dropdown: return subject_codes
    options = subject_dropdown.find_all("option")
    if not options: return subject_codes
    for option in options:
        value = option.get("value")
        text = _clean_string(option.get_text(strip=True))
        if value and value != "0" and text: subject_codes[text] = value
    return subject_codes


def _extract_detailed_grades_table(soup: BeautifulSoup) -> dict | None:
    """Extracts detailed grades (quizzes, assignments) for a selected subject."""
    detailed_grades = {} # This will store {final_string_key: grade_details_dict}
    try:
        container_div = soup.find("div", id="ContentPlaceHolderright_ContentPlaceHoldercontent_nttTr")
        detailed_grades_table = None
        if not container_div:
            detailed_grades_table = soup.find("table", id=lambda x: x and "GridViewNtt" in x)
            if not detailed_grades_table:
                logger.info("Detailed grades container div '...nttTr' and fallback table '...GridViewNtt' not found.")
                return {} # Return empty dict for consistency, indicates no items found
        else:
            detailed_grades_table = container_div.find("table")
            if not detailed_grades_table:
                logger.info("Detailed grades container div found, but no table inside.")
                return {} # Return empty dict

        rows = detailed_grades_table.find_all("tr")
        if len(rows) <= 1:
            logger.info("Detailed grades table found but is empty (only header or no rows).")
            return {} # Return empty dict

        header_row = rows[0]
        headers_raw = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]
        headers = [_clean_string(h) for h in headers_raw if h] # Clean and filter empty headers
        
        if not headers or not all(expected_header in headers for expected_header in ["Quiz/Assignment", "Element Name", "Grade"]):
            logger.warning(
                f"Missing one or more critical headers (Quiz/Assignment, Element Name, Grade) in detailed grades table. Found: {headers}. HTML: {header_row}"
            )
            return None # Indicate parsing failure more strongly if headers are bad

        # This counter is to make keys unique if the primary identifiers are repeated
        # (e.g., multiple "Question1" under "Quiz 1").
        # Its effectiveness depends on stable row ordering from GUC if identifiers are not truly unique.
        # If GUC row order is unstable AND identifiers are not unique, this is a very hard problem.
        # We assume for now that if identifiers are not unique, row order is stable, OR
        # that the combination of key parts we try to make will usually be unique.
        item_occurrence_counter = {} 

        for row_idx, row in enumerate(rows[1:]): # Start from 1 to skip header
            cells = row.find_all("td")
            if len(cells) == len(headers):
                try:
                    row_data = {
                        headers[i]: _clean_string(cells[i].get_text(strip=True)) # Assumes headers[i] is safe
                        for i in range(len(headers))
                    }

                    # --- Key Generation Logic ---
                    # These are the raw values extracted from the current row
                    raw_quiz_assignment = row_data.get("Quiz/Assignment", "")
                    raw_element_name = row_data.get("Element Name", "")
                    # Potentially add other raw, stable fields from the row if they help uniqueness:
                    # raw_other_stable_field = row_data.get("SomeOtherStableColumn", "")

                    # Clean these specific parts for key generation.
                    # THE CONSISTENCY OF raw_quiz_assignment and raw_element_name (as extracted from HTML)
                    # IS THE MOST CRITICAL FACTOR. If these flip-flop (e.g., "bonus" vs "discussion 1" for
                    # the same logical item due to scraper instability), the key will change.
                    
                    key_part_qa = _clean_string(raw_quiz_assignment) if raw_quiz_assignment else "NO_QA_CATEGORY"
                    key_part_en = _clean_string(raw_element_name) if raw_element_name else "NO_ELEMENT_NAME"
                    # key_part_other = _clean_string(raw_other_stable_field) if raw_other_stable_field else "NO_OTHER_FIELD"

                    # Construct a base key tuple from the STABLE parts.
                    # The goal is for this base_key_tuple to be unique for each distinct grade item.
                    base_key_tuple = (key_part_qa, key_part_en) # Add key_part_other if used

                    # Use the occurrence counter based on this stable base_key_tuple
                    occurrence = item_occurrence_counter.get(base_key_tuple, 0)
                    final_string_key = f"{base_key_tuple[0]}::{base_key_tuple[1]}::{occurrence}"
                    # If using more parts: f"{base_key_tuple[0]}::{base_key_tuple[1]}::{base_key_tuple[2]}::{occurrence}"
                    
                    item_occurrence_counter[base_key_tuple] = occurrence + 1
                    # --- End Key Generation Logic ---
                    
                    grade_value = row_data.get("Grade", "") # Already cleaned
                    percentage, out_of = 0.0, 0.0
                    if grade_value and "/" in grade_value:
                        parts = grade_value.split("/")
                        if len(parts) == 2:
                            try:
                                score_str, total_str = parts[0].strip(), parts[1].strip()
                                if score_str: percentage = float(score_str)
                                if total_str: out_of = float(total_str)
                            except ValueError:
                                logger.warning(f"Could not parse grade fraction '{grade_value}' for key '{final_string_key}'. Setting to 0/0.")
                                percentage, out_of = 0.0, 0.0
                        else:
                            logger.warning(f"Invalid grade fraction format '{grade_value}' for key '{final_string_key}'. Setting to 0/0.")
                            percentage, out_of = 0.0, 0.0
                    elif grade_value and grade_value.lower() not in ["undetermined", "", "-"]:
                        try:
                            percentage = float(grade_value) # Assumes standalone score
                            # out_of might remain 0 or be set to a special value if it's a raw score
                        except ValueError:
                            logger.warning(f"Non-numeric, non-fraction grade '{grade_value}' for key '{final_string_key}'. Storing as is, parsed as 0/0.")
                    
                    detailed_grades[final_string_key] = {
                        "Quiz/Assignment": raw_quiz_assignment, # Store original (but whitespace cleaned) for display
                        "Element Name": raw_element_name,       # Store original (but whitespace cleaned) for display
                        "grade": grade_value, 
                        "percentage": percentage,
                        "out_of": out_of,
                        # Add any other columns you want to store from row_data
                        # "Weight": row_data.get("Weight",""), # Example
                    }
                except KeyError as e_key:
                    logger.error(f"KeyError processing detailed grade row (missing expected header key in row_data): {e_key}. Headers: {headers}, Row HTML: {row}", exc_info=False)
                except Exception as e_cell:
                    logger.error(f"General error processing detailed grade row: {e_cell}. Row HTML: {row}", exc_info=False)
            else:
                logger.warning(f"Skipping detailed grade row - cell count ({len(cells)}) mismatch with header count ({len(headers)}). Row HTML: {row}")
        return detailed_grades
    except Exception as e:
        logger.error(f"Critical error during detailed grades table extraction: {e}", exc_info=True)
        return None # Indicate a more severe parsing failure for the whole table


def scrape_grades(username: str, password: str) -> dict | None:
    # ... (Initial part: grades_url, session, all_grades_data, retries - no changes) ...
    grades_url = config.BASE_GRADES_URL
    session = create_session(username, password)
    all_grades_data = None # Will be populated
    # max_retries = config.DEFAULT_MAX_RETRIES # Not used in this version's loop, make_request handles retries
    # retry_delay = config.DEFAULT_RETRY_DELAY

    logger.info(f"Starting grades scraping for {username} from {grades_url}")

    try:
        response_initial = make_request(session, grades_url, method="GET", timeout=(10, 20))
        if not response_initial:
            logger.error(f"Failed to fetch initial grades page for {username}.")
            return {"error": "Failed to fetch initial grades page"} # Return error dict

        initial_html = response_initial.text
        if "Login Failed!" in initial_html or "Object moved" in initial_html or "The username or password you entered is incorrect" in initial_html:
            logger.warning(f"Grades scraping failed for {username}: Authentication failed (detected on initial page).")
            return {"error": "Authentication failed"}

        soup_initial = BeautifulSoup(initial_html, "lxml")
        initial_grades = _parse_midterm_grades(soup_initial)
        subject_codes = _parse_subject_codes(soup_initial)

        all_grades_data = {
            "midterm_results": initial_grades,
            "subject_codes": subject_codes,
            "detailed_grades": {},
        }

        if not subject_codes:
            logger.warning(f"No subject codes found for {username}. Cannot fetch detailed grades.")
            return all_grades_data # Return what we have (midterms, empty subject_codes/detailed_grades)

        logger.info(f"Found {len(subject_codes)} subjects. Fetching detailed grades...")
        viewstate = soup_initial.find("input", {"name": "__VIEWSTATE"})
        viewstate_gen = soup_initial.find("input", {"name": "__VIEWSTATEGENERATOR"})
        event_validation = soup_initial.find("input", {"name": "__EVENTVALIDATION"})
        hidden_student = soup_initial.find("input", id="ContentPlaceHolderright_ContentPlaceHoldercontent_HiddenFieldstudent")
        hidden_season = soup_initial.find("input", id="ContentPlaceHolderright_ContentPlaceHoldercontent_HiddenFieldseason")

        if not (viewstate and viewstate_gen and event_validation and hidden_student and hidden_season):
            logger.error(f"Missing essential form elements on initial grades page for {username}. Cannot fetch detailed grades.")
            return all_grades_data # Return data obtained so far

        base_form_data = {
            "__EVENTTARGET": "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$smCrsLst",
            "__EVENTARGUMENT": "", "__LASTFOCUS": "",
            "__VIEWSTATE": viewstate["value"],
            "__VIEWSTATEGENERATOR": viewstate_gen["value"],
            "__EVENTVALIDATION": event_validation["value"],
            "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$HiddenFieldstudent": hidden_student["value"],
            "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$HiddenFieldseason": hidden_season["value"],
            "ctl00$ctl00$div_position": "0",
        }
        
        max_workers_detailed = min(getattr(config, 'MAX_CONCURRENT_FETCHES_PER_SESSION', 5), len(subject_codes))
        if max_workers_detailed <= 0: max_workers_detailed = 1

        detailed_grades_results = {} 
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_detailed, thread_name_prefix="GradeDetail") as executor:
            future_to_subject = {
                executor.submit(
                    _fetch_and_parse_detailed_grades,
                    username, password, grades_url, 
                    {**base_form_data, "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$smCrsLst": subject_code},
                    subject_name
                ): subject_name 
                for subject_name, subject_code in subject_codes.items()
            }

            for future in concurrent.futures.as_completed(future_to_subject):
                subject_name = future_to_subject[future]
                try:
                    detailed_result = future.result() # This is a dict from _extract_detailed_grades_table or None
                    if detailed_result is not None: # Can be an empty dict {}
                        detailed_grades_results[subject_name] = detailed_result
                        logger.info(f"Successfully processed detailed grades task for: {subject_name} (Items: {len(detailed_result)})")
                    else: # Parsing failed for this subject, _extract_detailed_grades_table returned None
                        logger.warning(f"Detailed grades task for {subject_name} returned None (parsing/fetch error). Storing empty dict.")
                        detailed_grades_results[subject_name] = {} 
                except Exception as exc:
                    logger.error(f"Fetching detailed grades for {subject_name} generated exception: {exc}", exc_info=True)
                    detailed_grades_results[subject_name] = {} # Default to empty on error for structure

        all_grades_data["detailed_grades"] = detailed_grades_results
        logger.info(f"Finished fetching detailed grades for {username}.")
        return all_grades_data

    except requests.exceptions.RequestException as e: # Catch network errors specifically for initial page
        logger.error(f"Network error during initial grades page fetch for {username}: {e}", exc_info=True)
        return {"error": f"Network error fetching grades page: {e!s}"}
    except Exception as e:
        logger.exception(f"Unexpected error during grades scraping for {username}: {e}")
        # If all_grades_data was initialized, return it, else None or an error dict
        if all_grades_data and "subject_codes" in all_grades_data : # Check if it's at least partially formed
             return all_grades_data
        return {"error": f"Unexpected error during grades scraping: {e!s}"}


def _fetch_and_parse_detailed_grades(
    username: str, password: str, url: str, form_data: dict, subject_name: str
) -> dict | None:
    session = create_session(username, password)
    logger.debug(f"Executing detailed grade fetch task for: {subject_name}")
    try:
        response = make_request(session, url, method="POST", data=form_data, timeout=(10, 20))
        if not response:
            logger.error(f"Failed to fetch detailed grades page for subject '{subject_name}'.")
            return None # Indicates fetch failure

        subject_soup = BeautifulSoup(response.content, "lxml") # Use response.content for bs4 if bytes
        detailed_grades_for_subject = _extract_detailed_grades_table(subject_soup)
        
        # _extract_detailed_grades_table now returns {} for empty/not found, None for major parse error
        return detailed_grades_for_subject 
    except Exception as e:
        logger.error(f"Error in detailed grade fetch/parse task for subject '{subject_name}': {e}", exc_info=True)
        return None # Indicate task failure