# scraping/exams.py
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import requests

from .core import create_session, make_request
from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

# --- Exam Seats Parsing Function ---


def parse_exam_seats_html(html: str) -> list:
    """Parses the exam seats table from the provided HTML."""
    exam_seats = []
    if not html:
        logger.warning("parse_exam_seats_html received empty HTML.")
        return exam_seats

    try:
        soup = BeautifulSoup(html, "lxml")
        # Find the main table (adjust selector if ID changes)
        # Common IDs: Table2, ...GridViewExams, etc. Check actual source.
        table = soup.find("table", id="Table2")
        if not table:
            # Fallback selector if primary ID fails
            table = soup.find("table", id=lambda x: x and "GridViewExams" in x)
            if not table:
                logger.warning(
                    "Exam seats table ('Table2' or '...GridViewExams') not found."
                )
                # Check for "No exam seats" messages
                no_seats_label = soup.find(
                    "span", id=lambda x: x and "lblNoData" in x
                )  # Example ID
                if no_seats_label and (
                    "no exam" in no_seats_label.text.lower()
                    or "not assigned" in no_seats_label.text.lower()
                ):
                    logger.info("Exam seats page indicates no seats assigned.")
                    return []  # Explicitly no seats found
                return []  # Return empty list if table missing

        rows = table.find_all("tr")
        if len(rows) <= 1:  # Only header or empty
            logger.info("Exam seats table found but is empty.")
            return []

        # Dynamically find headers if possible (more robust)
        headers = [
            th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])
        ]
        logger.debug(f"Exam seats table headers found: {headers}")

        # Map expected data to potential header variations
        header_map = {
            # ---> Add the actual header name found in the logs <---
            "course": ["course", "course name", "course name - season"],
            "date": ["date"],
            "end_time": ["end time", "end"],
            "exam_day": ["day", "exam day"],
            "hall": ["hall", "location"],
            "seat": ["seat", "seat no.", "seat number"],
            "start_time": ["start time", "start"],
            "type": ["type", "exam type"],
            # 'season': ['season'] # Season is derived later
        }
        # Find column indices based on headers
        col_indices = {}
        for key, possible_headers in header_map.items():
            found = False
            for p_header in possible_headers:
                try:
                    col_indices[key] = headers.index(p_header)
                    found = True
                    break
                except ValueError:
                    continue  # Try next possible header
            if not found:
                logger.warning(
                    f"Could not find column index for expected field '{key}' in headers: {headers}"
                )
                # Decide if this is critical? Maybe seat/course/date/time/hall are essential.
                # if key in ['course', 'date', 'start_time', 'hall', 'seat']: return [] # Critical column missing

        # Default indices if dynamic mapping fails (less robust)
        if not col_indices or len(col_indices) < 5:  # Basic sanity check
            logger.warning(
                "Using default column indices for exam seats as dynamic mapping failed."
            )
            col_indices = {
                "course": 0,
                "exam_day": 1,
                "date": 2,
                "start_time": 3,
                "end_time": 4,
                "hall": 5,
                "seat": 6,
                "type": 7,
            }

        for row in rows[1:]:  # Skip header row
            cells = row.find_all("td")
            if len(cells) >= max(col_indices.values()) + 1:
                try:
                    exam_data = {}
                    season = ""  # Initialize season
                    course_name_only = ""  # Initialize course name without season

                    for key, index in col_indices.items():
                        if index < len(cells):
                            cell_text = (
                                cells[index]
                                .get_text(strip=True)
                                .replace("\r", "")
                                .replace("\n", "")
                            )
                            exam_data[key] = cell_text

                            # ---> Special handling for the course column <---
                            if key == "course":
                                course_full = cell_text
                                if " - " in course_full:
                                    parts = course_full.split(" - ")
                                    if len(parts) > 1:
                                        season = parts[-1].strip()
                                        course_name_only = " - ".join(
                                            parts[:-1]
                                        ).strip()
                                    else:  # Just in case " - " is at start/end
                                        course_name_only = course_full
                                else:
                                    course_name_only = course_full
                        else:
                            exam_data[key] = ""

                    # Add derived season and potentially overwrite course with cleaned name
                    exam_data["season"] = season
                    exam_data["course"] = (
                        course_name_only  # Store only the course name part
                    )

                    # --- Validation using the cleaned course name ---
                    if not all(
                        exam_data.get(k)
                        for k in ["course", "date", "start_time", "seat"]
                    ):
                        # Log the original full course name if available for context
                        original_course_field = (
                            cells[col_indices.get("course", 0)].get_text(strip=True)
                            if "course" in col_indices
                            else "N/A"
                        )
                        logger.warning(
                            f"Skipping exam seat row due to missing essential data. Original Course Field: '{original_course_field}'. Parsed Data: {exam_data}"
                        )
                        continue

                    exam_seats.append(exam_data)

                except Exception as e_cell:
                    logger.error(
                        f"Error parsing exam seat row cells: {e_cell}. Row HTML: {row}",
                        exc_info=False,
                    )
            else:
                logger.warning(
                    f"Skipping exam seat row - cell count mismatch. Found {len(cells)}, needed >= {max(col_indices.values()) + 1}. Row: {row}"
                )

        # Sort by date and then start time
        def sort_key(exam):
            try:
                # Adjust date format if needed (e.g., '%d/%m/%Y' or '%m/%d/%Y')
                date_obj = datetime.strptime(
                    exam.get("date", ""), "%d - %B - %Y"
                )  # Assumes "DD - MonthName - YYYY"
            except ValueError:
                logger.warning(
                    f"Could not parse date '{exam.get('date')}' for sorting. Placing first."
                )
                date_obj = datetime.min
            try:
                # Adjust time format if needed (e.g., '%H:%M')
                time_obj = datetime.strptime(
                    exam.get("start_time", ""), "%I:%M:%S %p"
                ).time()  # Assumes "HH:MM:SS AM/PM"
            except ValueError:
                logger.warning(
                    f"Could not parse start time '{exam.get('start_time')}' for sorting. Placing first."
                )
                time_obj = datetime.min.time()
            return (date_obj, time_obj)

        try:
            exam_seats.sort(key=sort_key)
        except Exception as sort_err:
            logger.error(f"Failed to sort exam seats: {sort_err}")

    except Exception as e:
        logger.exception(f"Error parsing exam seats HTML: {e}")
        # Return partially parsed seats if any, otherwise empty list
    return exam_seats


# --- Main Exam Seats Scraping Function ---


def scrape_exam_seats(username: str, password: str) -> list | None:
    """
    Scrapes exam seat information for the user.

    Returns:
        list: A list of exam seat dictionaries, sorted by date/time.
              Returns an empty list [] if scraping succeeds but no seats are found.
        None: On critical failure (auth, network error, critical parsing failure).
    """
    exam_seats_url = config.BASE_EXAM_SEATS_URL
    session = create_session(username, password)
    seats_data = None

    logger.info(f"Starting exam seats scraping for {username} from {exam_seats_url}")

    try:
        response = make_request(
            session, exam_seats_url, method="GET", timeout=(10, 20)
        )  # Allow more time

        if not response:
            logger.error(
                f"Failed to fetch exam seats page for {username}. Make_request returned None."
            )
            return None  # Auth or connection error

        html_content = response.text
        # Check for login failure indicators
        if "Login Failed!" in html_content or "Object moved" in html_content:
            logger.warning(
                f"Exam seats scraping failed for {username}: Authentication failed."
            )
            return None  # Indicate auth failure

        # Parse the HTML
        seats_data = parse_exam_seats_html(html_content)

        # parse_exam_seats_html returns list (potentially empty) on success, [] on non-critical parse failure
        if (
            seats_data is None
        ):  # Should not happen based on parser logic, but check defensively
            logger.error(
                f"Exam seats parsing function returned None unexpectedly for {username}."
            )
            return None  # Indicate critical parsing failure
        else:
            logger.info(
                f"Successfully scraped and parsed exam seats for {username}. Found {len(seats_data)} seats."
            )
            return seats_data  # Return list (can be empty [])

    except Exception as e:
        logger.exception(
            f"Unexpected error during exam seats scraping for {username}: {e}"
        )
        return None  # Indicate critical unexpected failure
