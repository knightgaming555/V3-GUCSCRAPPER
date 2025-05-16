# scraping/grades.py
import logging
from bs4 import BeautifulSoup
import concurrent
import requests
import time

from .core import create_session, make_request
from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

# --- Helper function (can be moved to a common utils if used elsewhere) ---
def _clean_string(text: str) -> str:
    """Helper to normalize strings for comparison."""
    if not isinstance(text, str):
        return ""
    return " ".join(text.strip().split()) # Collapse whitespace and strip

# --- Grades Parsing Functions ---


def _parse_midterm_grades(soup: BeautifulSoup) -> dict:
    """Extracts midterm grades from the main table."""
    midterm_results = {}
    midterm_table = soup.find(
        "table", id="ContentPlaceHolderright_ContentPlaceHoldercontent_midDg"
    )
    if not midterm_table:
        logger.warning("Midterm grades table '...midDg' not found.")
        return midterm_results  # Return empty dict if table not found

    rows = midterm_table.find_all("tr")
    if len(rows) <= 1:  # Only header row
        logger.info("Midterm grades table found but is empty.")
        return midterm_results

    for row in rows[1:]:  # Skip header row
        cells = row.find_all("td")
        if len(cells) >= 2:
            try:
                course_name = _clean_string(cells[0].get_text(strip=True))
                percentage = _clean_string(cells[1].get_text(strip=True))
                if course_name:
                    midterm_results[course_name] = percentage
            except Exception as e:
                logger.error(
                    f"Error parsing midterm grade row: {e}. Row HTML: {row}",
                    exc_info=False,
                )
        else:
            logger.warning(
                f"Skipping midterm grade row with insufficient cells ({len(cells)})."
            )
    return midterm_results


def _parse_subject_codes(soup: BeautifulSoup) -> dict:
    """Extracts subject names and codes from the dropdown menu."""
    subject_codes = {}
    subject_dropdown = soup.find(
        "select", id="ContentPlaceHolderright_ContentPlaceHoldercontent_smCrsLst"
    )
    if not subject_dropdown:
        logger.warning("Subject dropdown '...smCrsLst' not found.")
        return subject_codes

    options = subject_dropdown.find_all("option")
    if not options:
        logger.info("Subject dropdown found but contains no options.")
        return subject_codes

    for option in options:
        value = option.get("value")
        text = _clean_string(option.get_text(strip=True))
        if value and value != "0" and text:
            subject_codes[text] = value
    return subject_codes


def _extract_detailed_grades_table(soup: BeautifulSoup) -> dict | None:
    """Extracts detailed grades (quizzes, assignments) for a selected subject."""
    detailed_grades = {}
    try:
        container_div = soup.find(
            "div", id="ContentPlaceHolderright_ContentPlaceHoldercontent_nttTr"
        )
        if not container_div:
            detailed_grades_table = soup.find(
                "table", id=lambda x: x and "GridViewNtt" in x
            ) 
            if not detailed_grades_table:
                logger.info(
                    "Detailed grades container div '...nttTr' (and fallback table) not found."
                )
                return None
        else:
            detailed_grades_table = container_div.find("table")
            if not detailed_grades_table:
                logger.info("Detailed grades container div found, but no table inside.")
                return None

        rows = detailed_grades_table.find_all("tr")
        if len(rows) <= 1:
            logger.info("Detailed grades table found but is empty.")
            return detailed_grades

        header_row = rows[0]
        headers_raw = [
            th.get_text(strip=True) for th in header_row.find_all(["th", "td"])
        ]
        headers = [_clean_string(h) for h in headers_raw if h]
        if not headers:
            logger.warning("Could not extract headers from detailed grades table.")
            return None

        try:
            # Ensure all expected columns are present by trying to find their indices
            # These specific names might need adjustment based on actual table headers
            # For now, assume "Quiz/Assignment", "Element Name", and "Grade" are critical.
            _ = headers.index("Quiz/Assignment") 
            _ = headers.index("Element Name")
            _ = headers.index("Grade")
        except ValueError:
            logger.warning(
                f"Missing one or more expected headers (Quiz/Assignment, Element Name, Grade) in detailed grades table. Found: {headers}"
            )
            return None 

        item_occurrence_counter = {} # To handle potential duplicate (quiz_name, element_name) keys

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) == len(headers):
                try:
                    row_data = {
                        _clean_string(headers[i]): _clean_string(cells[i].get_text(strip=True))
                        for i in range(len(headers))
                    }

                    quiz_assignment_raw = row_data.get("Quiz/Assignment", "")
                    element_name_raw = row_data.get("Element Name", "")
                    grade_value = row_data.get("Grade", "") # Already cleaned by dict comprehension

                    # Consistently generate key parts
                    cleaned_qa_for_key = _clean_string(quiz_assignment_raw)
                    key_part_1 = cleaned_qa_for_key if cleaned_qa_for_key else "NO_QUIZ_ASSIGN_NAME"

                    cleaned_en_for_key = _clean_string(element_name_raw)
                    key_part_2 = cleaned_en_for_key if cleaned_en_for_key else "NO_ELEMENT_NAME"
                    
                    counter_tuple_key = (key_part_1, key_part_2) # Used to count occurrences of this specific combination
                    final_string_key: str

                    if key_part_1 == "NO_QUIZ_ASSIGN_NAME" and key_part_2 == "NO_ELEMENT_NAME":
                        # Fallback for truly anonymous items, using a distinct counter key for these
                        occurrence = item_occurrence_counter.get(("_ANONYMOUS_INTERNAL_TRACKER_", "_ANONYMOUS_INTERNAL_TRACKER_"), 0)
                        final_string_key = f"_ANONYMOUS_ROW_::{occurrence}"
                        item_occurrence_counter[("_ANONYMOUS_INTERNAL_TRACKER_", "_ANONYMOUS_INTERNAL_TRACKER_")] = occurrence + 1
                    else:
                        # Count occurrences of this specific (key_part_1, key_part_2) combination
                        occurrence = item_occurrence_counter.get(counter_tuple_key, 0)
                        final_string_key = f"{key_part_1}::{key_part_2}::{occurrence}"
                        item_occurrence_counter[counter_tuple_key] = occurrence + 1
                    
                    percentage = 0.0
                    out_of = 0.0
                    if grade_value and "/" in grade_value:
                        parts = grade_value.split("/")
                        if len(parts) == 2:
                            try:
                                score_str = parts[0].strip()
                                total_str = parts[1].strip()
                                if score_str: # Only parse if score is not empty
                                    percentage = float(score_str)
                                if total_str: # Only parse if total is not empty
                                    out_of = float(total_str)
                            except ValueError:
                                # Log original element name if available, otherwise parts of key for context
                                name_for_log = element_name_raw if element_name_raw else (quiz_assignment_raw if quiz_assignment_raw else "Unknown Item")
                                logger.warning(
                                    f"Could not parse grade fraction '{grade_value}' for '{name_for_log}'. Setting to 0/0."
                                )
                                percentage = 0.0
                                out_of = 0.0 # Explicitly set out_of to 0 on parse error
                        else: # Invalid fraction format e.g. "/" or "1/2/3"
                             name_for_log = element_name_raw if element_name_raw else (quiz_assignment_raw if quiz_assignment_raw else "Unknown Item")
                             logger.warning(f"Invalid grade fraction format '{grade_value}' for '{name_for_log}'. Setting to 0/0.")
                             percentage = 0.0
                             out_of = 0.0
                    elif grade_value and grade_value.lower() not in ["undetermined", "", "-"]:
                        try:
                            percentage = float(grade_value)
                            out_of = 0.0 # Or some other default if it's a standalone score without a total
                            logger.debug(
                                f"Interpreting non-fraction grade '{grade_value}' for '{element_name_raw or quiz_assignment_raw}' as a standalone score."
                            )
                        except ValueError:
                            logger.warning(
                                f"Non-numeric, non-fraction grade '{grade_value}' for '{element_name_raw or quiz_assignment_raw}'. Store as is, parsed as 0/0."
                            )
                    # else: grade is undetermined, empty, or placeholder, percentage/out_of remain 0.0
                    
                    detailed_grades[final_string_key] = {
                        "Quiz/Assignment": quiz_assignment_raw, # Store original, uncleaned names for display
                        "Element Name": element_name_raw,
                        "grade": grade_value, 
                        "percentage": percentage,
                        "out_of": out_of,
                    }
                except Exception as e_cell:
                    logger.error(
                        f"Error processing detailed grade row: {e_cell}. Row HTML: {row}",
                        exc_info=False,
                    )
            else:
                logger.warning(
                    f"Skipping detailed grade row - cell count mismatch. Expected {len(headers)}, got {len(cells)}. Row HTML: {row}"
                )
        return detailed_grades
    except Exception as e:
        logger.error(f"Error extracting detailed grades table: {e}", exc_info=True)
        return None


# --- Main Grades Scraping Function ---


def scrape_grades(username: str, password: str) -> dict | None:
    """
    Scrapes midterm and detailed grades for all subjects for a user.

    Returns:
        dict: Combined grades data including 'midterm_results', 'subject_codes',
              and 'detailed_grades' (nested by subject name).
        None: On critical failure (auth, network, initial page parse failure).
    """
    grades_url = config.BASE_GRADES_URL
    session = create_session(username, password)
    all_grades_data = None
    max_retries = config.DEFAULT_MAX_RETRIES
    retry_delay = config.DEFAULT_RETRY_DELAY

    logger.info(f"Starting grades scraping for {username} from {grades_url}")

    try:
        # 1. Fetch the initial grades page
        response_initial = make_request(
            session, grades_url, method="GET", timeout=(10, 20)
        )  # Longer read timeout
        if not response_initial:
            logger.error(f"Failed to fetch initial grades page for {username}.")
            return None  # Auth or connection error

        initial_html = response_initial.text
        # Check for login failure indicators
        if "Login Failed!" in initial_html or "Object moved" in initial_html:
            logger.warning(
                f"Grades scraping failed for {username}: Authentication failed (detected on initial page)."
            )
            return {"error": "Authentication failed"}  # Return error dict

        soup_initial = BeautifulSoup(initial_html, "lxml")

        # 2. Parse initial page (midterms, subject codes)
        initial_grades = _parse_midterm_grades(soup_initial)
        subject_codes = _parse_subject_codes(soup_initial)

        if not subject_codes:
            logger.warning(
                f"No subject codes found for {username}. Cannot fetch detailed grades."
            )
            # Return only midterm results if found, otherwise indicate failure
            if initial_grades:
                return {
                    "midterm_results": initial_grades,
                    "subject_codes": {},
                    "detailed_grades": {},
                }
            else:
                logger.error(
                    f"Failed to extract both midterms and subject codes for {username}."
                )
                return None  # Indicate failure if nothing could be parsed initially

        all_grades_data = {
            "midterm_results": initial_grades,
            "subject_codes": subject_codes,
            "detailed_grades": {},  # Initialize detailed grades dict
        }

        # 3. Fetch detailed grades for each subject
        logger.info(f"Found {len(subject_codes)} subjects. Fetching detailed grades...")

        # Extract necessary form fields from the initial page *once*
        viewstate = soup_initial.find("input", {"name": "__VIEWSTATE"})
        viewstate_gen = soup_initial.find("input", {"name": "__VIEWSTATEGENERATOR"})
        event_validation = soup_initial.find("input", {"name": "__EVENTVALIDATION"})
        hidden_student = soup_initial.find(
            "input",
            id="ContentPlaceHolderright_ContentPlaceHoldercontent_HiddenFieldstudent",
        )
        hidden_season = soup_initial.find(
            "input",
            id="ContentPlaceHolderright_ContentPlaceHoldercontent_HiddenFieldseason",
        )

        if not (
            viewstate
            and viewstate_gen
            and event_validation
            and hidden_student
            and hidden_season
        ):
            logger.error(
                f"Missing essential form elements on initial grades page for {username}. Cannot fetch detailed grades."
            )
            # Return the data obtained so far (midterms, subjects)
            return all_grades_data

        base_form_data = {
            "__EVENTTARGET": "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$smCrsLst",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": viewstate["value"],
            "__VIEWSTATEGENERATOR": viewstate_gen["value"],
            "__EVENTVALIDATION": event_validation["value"],
            "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$HiddenFieldstudent": hidden_student[
                "value"
            ],
            "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$HiddenFieldseason": hidden_season[
                "value"
            ],
            "ctl00$ctl00$div_position": "0",  # Common hidden field
        }

        # Use a ThreadPoolExecutor to fetch detailed grades concurrently
        # Limit max_workers to avoid overwhelming the server or local resources
        max_workers_detailed = min(getattr(config, 'MAX_CONCURRENT_FETCHES_PER_SESSION', 5), len(subject_codes))
        if max_workers_detailed <= 0: # Ensure positive number of workers
            max_workers_detailed = 1

        detailed_grades_results = {} 
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers_detailed, thread_name_prefix="GradeDetail"
        ) as executor:
            future_to_subject = {}
            for subject_name, subject_code in subject_codes.items():
                logger.debug(
                    f"Submitting detailed grade fetch for: {subject_name} ({subject_code})"
                )
                form_data = base_form_data.copy()
                form_data[
                    "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$smCrsLst"
                ] = subject_code
                
                future = executor.submit(
                    _fetch_and_parse_detailed_grades,
                    username,
                    password,
                    grades_url,
                    form_data,
                    subject_name,
                )
                future_to_subject[future] = subject_name

            for future in concurrent.futures.as_completed(future_to_subject):
                subject_name = future_to_subject[future]
                try:
                    # detailed_result is the direct return of _fetch_and_parse_detailed_grades
                    # It can be: a dict of grades (possibly empty), or None if parsing/fetch failed internally
                    detailed_result = future.result()
                    
                    if detailed_result is not None:
                        detailed_grades_results[subject_name] = detailed_result
                        # This log is fine if detailed_result is not None, means the task returned a dict.
                        # It could be an empty dict if the table was empty but parsed, or populated if grades found.
                        logger.info(
                            f"Successfully processed detailed grades task for: {subject_name}"
                        )
                    else:
                        # _fetch_and_parse_detailed_grades itself returned None (e.g., request failed, or _extract_detailed_grades_table returned None)
                        logger.warning(
                            f"Detailed grades task for {subject_name} returned None. Storing empty dict for stability."
                        )
                        detailed_grades_results[subject_name] = {}
                except Exception as exc:
                    logger.error(
                        f"Fetching detailed grades for {subject_name} generated an exception: {exc}",
                        exc_info=True,
                    )
                    detailed_grades_results[subject_name] = {} # Ensure structural consistency on exception

        all_grades_data["detailed_grades"] = detailed_grades_results
        logger.info(f"Finished fetching detailed grades for {username}.")
        return all_grades_data

    except Exception as e:
        logger.exception(f"Unexpected error during grades scraping for {username}: {e}")
        # Return partially gathered data if available, otherwise None
        return all_grades_data if all_grades_data else None


def _fetch_and_parse_detailed_grades(
    username: str, password: str, url: str, form_data: dict, subject_name: str
) -> dict | None:
    """
    Task function to fetch and parse detailed grades for one subject.
    Creates its own session for thread safety.
    """
    session = create_session(
        username, password
    )  # Create fresh session for thread safety
    logger.debug(f"Executing detailed grade fetch task for: {subject_name}")
    try:
        # Make POST request to select the subject
        response = make_request(
            session, url, method="POST", data=form_data, timeout=(10, 20)
        )  # Longer read timeout

        if not response:
            logger.error(
                f"Failed to fetch detailed grades page for subject '{subject_name}'."
            )
            return None

        # Parse the response HTML for the detailed grades table
        subject_soup = BeautifulSoup(response.content, "lxml")
        detailed_grades = _extract_detailed_grades_table(
            subject_soup
        )  # Returns dict or None

        # detailed_grades can be an empty dict {} if table was empty, which is valid.
        # It will be None if parsing failed or table was missing.
        return detailed_grades

    except Exception as e:
        logger.error(
            f"Error in detailed grade fetch/parse task for subject '{subject_name}': {e}",
            exc_info=True,
        )
        return None
