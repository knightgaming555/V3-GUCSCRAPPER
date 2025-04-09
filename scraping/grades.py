# scraping/grades.py
import logging
from bs4 import BeautifulSoup
import concurrent
import requests
import time

from .core import create_session, make_request
from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

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
                # Clean whitespace and remove potential unwanted characters
                course_name = (
                    cells[0].get_text(strip=True).replace("\r", "").replace("\n", " ")
                )
                percentage = (
                    cells[1].get_text(strip=True).replace("\r", "").replace("\n", " ")
                )
                if course_name:  # Ensure course name is not empty
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
        text = option.get_text(strip=True)
        # Skip default/placeholder options (often value="0" or empty)
        if value and value != "0" and text:
            subject_codes[text] = value

    return subject_codes


def _extract_detailed_grades_table(soup: BeautifulSoup) -> dict | None:
    """Extracts detailed grades (quizzes, assignments) for a selected subject."""
    detailed_grades = {}
    try:
        # Find the container div first, then the table inside
        container_div = soup.find(
            "div", id="ContentPlaceHolderright_ContentPlaceHoldercontent_nttTr"
        )
        if not container_div:
            # Check if the div might be missing but the table exists directly (less common)
            detailed_grades_table = soup.find(
                "table", id=lambda x: x and "GridViewNtt" in x
            )  # Alternative ID check
            if not detailed_grades_table:
                logger.info(
                    "Detailed grades container div '...nttTr' (and fallback table) not found."
                )
                return None  # Indicate section not present
        else:
            detailed_grades_table = container_div.find("table")
            if not detailed_grades_table:
                logger.info("Detailed grades container div found, but no table inside.")
                return None  # Section header present, but no table

        rows = detailed_grades_table.find_all("tr")
        if len(rows) <= 1:  # Only header or empty
            logger.info("Detailed grades table found but is empty.")
            return detailed_grades  # Return empty dict for empty table

        # --- Extract Headers ---
        header_row = rows[0]
        headers_raw = [
            th.get_text(strip=True) for th in header_row.find_all(["th", "td"])
        ]  # Allow td as header cells too
        headers = [h for h in headers_raw if h]  # Filter empty headers
        if not headers:
            logger.warning("Could not extract headers from detailed grades table.")
            return None  # Cannot process rows without headers

        # Expected headers (adjust if needed): "Quiz/Assignment", "Element Name", "Grade"
        try:
            quiz_col = headers.index("Quiz/Assignment")
            element_col = headers.index("Element Name")
            grade_col = headers.index("Grade")
        except ValueError:
            logger.warning(
                f"Missing expected headers in detailed grades table. Found: {headers}"
            )
            return None  # Cannot process if essential columns are missing

        # --- Extract Rows ---
        row_counter = 0  # To generate unique keys if element names repeat
        for row in rows[1:]:  # Skip header row
            cells = row.find_all("td")
            if len(cells) == len(headers):  # Ensure row structure matches headers
                try:
                    row_data = {
                        headers[i]: cells[i].get_text(strip=True)
                        for i in range(len(headers))
                    }

                    quiz_assignment = row_data.get("Quiz/Assignment", "").strip()
                    element_name = row_data.get("Element Name", "").strip()
                    grade_value_raw = row_data.get("Grade", "").strip()
                    # Clean grade value further
                    grade_value = (
                        grade_value_raw.replace("\r", "")
                        .replace("\n", "")
                        .replace("\t", "")
                        .strip()
                    )

                    # Handle missing element name (use Quiz/Assignment as fallback?)
                    if not element_name:
                        element_name = f"Unnamed_{quiz_assignment}_{row_counter}"

                    # Create a unique key (prefer element name, add counter for duplicates)
                    unique_key = f"{element_name}_{row_counter}"
                    row_counter += 1

                    # --- Parse Grade Value (Score/Total) ---
                    percentage = 0.0
                    out_of = 0.0
                    if grade_value and "/" in grade_value:
                        parts = grade_value.split("/")
                        if len(parts) == 2:
                            try:
                                percentage = float(parts[0].strip())
                                out_of = float(parts[1].strip())
                            except ValueError:
                                logger.warning(
                                    f"Could not parse grade fraction '{grade_value}' for '{element_name}'. Setting to 0."
                                )
                                percentage = 0.0
                                out_of = 0.0
                    elif (
                        grade_value and grade_value != "Undetermined"
                    ):  # Handle non-fraction grades if needed
                        try:
                            # Attempt to parse as a single number (maybe percentage?)
                            # Adjust logic based on how single grades are represented
                            percentage = float(grade_value)
                            out_of = 100.0  # Assuming it's a percentage if not a fraction? Risky assumption.
                            logger.debug(
                                f"Interpreting non-fraction grade '{grade_value}' for '{element_name}' as percentage."
                            )
                        except ValueError:
                            logger.warning(
                                f"Non-numeric, non-fraction grade '{grade_value}' for '{element_name}'."
                            )
                            # Keep percentage/out_of as 0

                    detailed_grades[unique_key] = {
                        "Quiz/Assignment": quiz_assignment,
                        "Element Name": element_name,
                        "grade": grade_value,  # Store cleaned raw grade string
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
        return None  # Indicate failure


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

        # Use ThreadPoolExecutor for concurrent subject requests
        detailed_grades_results = {}
        # Adjust workers based on typical number of subjects
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(subject_codes), 5), thread_name_prefix="GradeDetail"
        ) as executor:
            future_to_subject = {}
            for subject_name, subject_code in subject_codes.items():
                logger.debug(
                    f"Submitting detailed grade fetch for: {subject_name} ({subject_code})"
                )
                # Prepare form data specific to this subject
                form_data = base_form_data.copy()
                form_data[
                    "ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$smCrsLst"
                ] = subject_code
                # Submit the task: make_request needs its own session per thread or careful session handling
                # For simplicity, we might pass username/password and create session inside task,
                # or use a thread-local session if optimizing further.
                # Let's pass necessary info to a wrapper task function.
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
                    detailed_result = (
                        future.result()
                    )  # Returns dict of detailed grades or None
                    if detailed_result is not None:  # Can be empty dict {}
                        detailed_grades_results[subject_name] = detailed_result
                        logger.info(
                            f"Successfully got detailed grades for: {subject_name}"
                        )
                    # else: Failure logged within the task function
                except Exception as exc:
                    logger.error(
                        f"Fetching detailed grades for {subject_name} generated an exception: {exc}",
                        exc_info=True,
                    )

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
