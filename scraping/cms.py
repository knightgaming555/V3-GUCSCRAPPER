# scraping/cms.py
import logging
import concurrent.futures
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser  # Use selectolax for fast parsing
import requests
from datetime import datetime  # For week sorting

from .core import create_session, make_request
from utils.helpers import normalize_course_url  # Use helper for normalization
from config import config  # Import the singleton instance

logger = logging.getLogger(__name__)

# --- CMS Course List Scraping ---


def scrape_cms_courses(username: str, password: str) -> list | None:
    """
    Scrapes the list of courses from the user's CMS homepage.

    Returns:
        list: A list of course dictionaries [{'course_name': ..., 'course_url': ..., 'season_name': ...}] on success.
        None: On failure (auth error, network error, parsing error).
    """
    cms_home_url = config.CMS_HOME_URL
    session = create_session(username, password)
    courses = []

    logger.info(f"Fetching CMS course list for {username} from {cms_home_url}")
    response = make_request(session, cms_home_url, method="GET")

    if not response:
        logger.error(
            f"Failed to fetch CMS homepage for {username}. Make_request returned None."
        )
        return None  # Auth (401) or connection errors handled by make_request

    try:
        # Check for login page indicators even on success response
        if "login" in response.url.lower():
            soup_login_check = BeautifulSoup(response.text, "lxml")
            if soup_login_check.find(
                "form", action=lambda x: x and "login" in x.lower()
            ):
                logger.warning(
                    f"CMS Course List: Detected login page redirect for {username}."
                )
                return None  # Treat as auth failure

        # Use selectolax for potentially faster parsing of the table
        tree = HTMLParser(response.text)
        table = tree.css_first(
            "#ContentPlaceHolderright_ContentPlaceHoldercontent_GridViewcourses"
        )

        if not table:
            logger.warning(
                f"CMS courses table not found on {cms_home_url} for {username}."
            )
            # Check if the page structure might indicate no courses enrolled
            no_courses_indicator = tree.css_first(
                "span#ContentPlaceHolderright_ContentPlaceHoldercontent_LabelNoCourses"
            )  # Example ID, adjust if needed
            if (
                no_courses_indicator
                and "no courses" in no_courses_indicator.text().lower()
            ):
                logger.info(f"User {username} has no courses enrolled on CMS.")
                return []  # Return empty list if explicitly no courses found
            # Otherwise, it's likely a parsing or unexpected page structure issue
            return None  # Indicate failure if table missing and no 'no courses' message

        rows = table.css("tr")
        if len(rows) <= 1:  # Only header row or empty
            logger.info(f"CMS courses table found but is empty for {username}.")
            return []  # Return empty list

        for row in rows[1:]:  # Skip header row
            cells = row.css("td")
            if len(cells) >= 6:
                try:
                    # Extract data, cleaning whitespace
                    course_name = cells[1].text(strip=True)
                    course_id = cells[4].text(strip=True)
                    season_id = cells[5].text(strip=True)
                    season_name = cells[3].text(strip=True)

                    # Construct the course URL safely
                    if course_id and season_id:
                        # Use urljoin for robust URL construction relative to base CMS URL
                        relative_path = f"/apps/student/CourseViewStn.aspx?id={course_id}&sid={season_id}"
                        course_url = urljoin(config.BASE_CMS_URL, relative_path)
                        # Normalize the generated URL
                        normalized_url = normalize_course_url(course_url)

                        courses.append(
                            {
                                "course_name": course_name,
                                "course_url": normalized_url,  # Store normalized URL
                                "season_name": season_name,
                            }
                        )
                    else:
                        logger.warning(
                            f"Skipping row due to missing course_id or season_id for {username}: {row.html[:100]}"
                        )

                except Exception as cell_err:
                    logger.error(
                        f"Error parsing course row cells for {username}: {cell_err} - Row HTML: {row.html[:100]}",
                        exc_info=False,
                    )  # Avoid full HTML log usually
            else:
                logger.warning(
                    f"Skipping course row with insufficient cells ({len(cells)}) for {username}."
                )

        logger.info(f"Successfully scraped {len(courses)} courses for {username}.")
        return courses

    except Exception as e:
        logger.exception(f"Unexpected error scraping CMS courses for {username}: {e}")
        return None


# --- CMS Course Content Scraping ---


def _parse_content_item(card_node) -> dict | None:
    """Parses a single content item card using selectolax."""
    title_text = "Unknown Content"
    download_url = None
    try:
        # Find title - ID often starts with 'content' followed by numbers
        title_div = card_node.css_first("[id^='content']")
        if title_div:
            title_text = (
                title_div.text(strip=True, separator=" ")
                .replace("\n", " ")
                .replace("\r", "")
                .strip()
            )
        else:
            # Fallback: Maybe title is in a different tag like h5? Check common structures.
            h_tag = card_node.css_first("h5, h6")  # Check h5 or h6
            if h_tag:
                title_text = h_tag.text(strip=True)
            else:
                logger.debug(
                    "Could not find title div/tag in content card."
                )  # Log only if truly not found

        # Find download link (assuming it has id='download')
        download_link_node = card_node.css_first("a#download")
        if download_link_node:
            href = download_link_node.attributes.get("href")
            if href:
                # Use urljoin to handle relative URLs correctly
                download_url = urljoin(config.BASE_CMS_URL, href)
        else:
            # Fallback: Check for other typical download links (e.g., class 'contentbtn')
            download_link_node = card_node.css_first("a.contentbtn")
            if download_link_node:
                href = download_link_node.attributes.get("href")
                if href:
                    download_url = urljoin(config.BASE_CMS_URL, href)

        # Return None only if title remains completely unknown (likely not a valid item)
        # Allow items with no download URL
        if title_text == "Unknown Content" and not title_div and not h_tag:
            return None

        return {"title": title_text, "download_url": download_url}
    except Exception as e:
        logger.error(
            f"Error parsing content item: {e}. Card HTML: {card_node.html[:100]}",
            exc_info=False,
        )
        return None


def _parse_single_week(week_div_node) -> dict | None:
    """Parses a single week's data using selectolax."""
    week_name = "Unknown Week"
    try:
        # Find week title
        week_title_tag = week_div_node.css_first("h2.text-big")
        if week_title_tag:
            week_name = week_title_tag.text(strip=True)

        week_data = {
            "week_name": week_name,
            "announcement": "",
            "description": "",
            "contents": [],
        }

        # Find main content area (often div.p-3)
        p3_div = week_div_node.css_first("div.p-3")
        if p3_div:
            # --- Extract Announcement / Description ---
            # Find divs containing strong tags (Announce/Desc headers)
            info_divs = p3_div.css("div > strong")  # More specific selector
            content_header_found = False
            for strong_tag in info_divs:
                header_text = strong_tag.text(strip=True).lower()
                parent_div = strong_tag.parent  # Get the containing div

                # Check if this section is hidden via inline style
                is_hidden = "display:none" in parent_div.attributes.get(
                    "style", ""
                ).replace(" ", "")

                # Find the associated paragraph (usually the next <p> sibling)
                para_text = ""
                next_node = parent_div.next
                while next_node:
                    if next_node.tag == "p" and "m-2" in next_node.attributes.get(
                        "class", ""
                    ):
                        para_text = (
                            next_node.text(strip=True, separator=" ")
                            .replace("\n", " ")
                            .replace("\r", "")
                            .strip()
                        )
                        break
                    # Stop if we hit the next potential section header (div>strong) or content cards
                    if next_node.tag == "div" and (
                        next_node.css_matches("div > strong")
                        or next_node.css_first(".card.mb-4")
                    ):
                        break
                    next_node = next_node.next  # Move to the *actual* next sibling

                if "announcement" in header_text and not is_hidden:
                    week_data["announcement"] = para_text
                elif (
                    "description" in header_text
                ):  # Keep description even if hidden? Yes.
                    week_data["description"] = para_text
                elif "content" in header_text:
                    content_header_found = True
                    # Once content header is found, stop looking for announce/desc
                    break

            # --- Extract Content Items ---
            content_cards = p3_div.css(".card.mb-4")
            if content_cards:
                # Using threads here might be overkill unless parsing _parse_content_item is very slow
                # For selectolax, sequential might be fast enough. Test if needed.
                contents = [_parse_content_item(card) for card in content_cards]
                week_data["contents"] = [
                    c for c in contents if c
                ]  # Filter out None results

        # Return None only if week name is still Unknown (parsing likely failed badly)
        if week_name == "Unknown Week" and not week_title_tag:
            logger.warning(
                f"Could not parse week name. Week div HTML: {week_div_node.html[:100]}"
            )
            return None

        return week_data

    except Exception as e:
        logger.error(
            f"Error parsing single week '{week_name}': {e}. Week HTML: {week_div_node.html[:100]}",
            exc_info=False,
        )
        return None


def parse_course_content_html(html_content: str) -> list:
    """Parses the main content area for weeks using selectolax."""
    weeks = []
    if not html_content:
        logger.warning("parse_course_content_html received empty HTML.")
        return weeks
    try:
        tree = HTMLParser(html_content)
        week_divs = tree.css(".weeksdata")
        if not week_divs:
            logger.warning(
                "No week sections found (selector '.weeksdata'). Check CMS page structure."
            )
            return weeks

        # Using threads here can speed up parsing if _parse_single_week is complex enough
        # Adjust max_workers based on typical number of weeks and CPU cores
        num_weeks = len(week_divs)
        weeks_data = []
        if num_weeks >= 3:  # Threshold to use threading
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(num_weeks, 4), thread_name_prefix="WeekParse"
            ) as executor:
                future_to_week = {
                    executor.submit(_parse_single_week, div): div for div in week_divs
                }
                for future in concurrent.futures.as_completed(future_to_week):
                    try:
                        result = future.result()
                        if result:
                            weeks_data.append(result)
                    except Exception as exc:
                        logger.error(f"Week parsing generated an exception: {exc}")
        else:  # Parse sequentially for fewer weeks
            weeks_data = [_parse_single_week(div) for div in week_divs]
            weeks_data = [w for w in weeks_data if w]  # Filter None results

        # Sort weeks by date (newest first) if possible
        try:

            def get_week_date(week_dict):
                name = week_dict.get("week_name", "")
                try:
                    # Flexible date parsing (YYYY-MM-DD, DD/MM/YYYY, etc. - add formats as needed)
                    date_str_match = re.search(
                        r"(\d{4}-\d{2}-\d{2})|(\d{1,2}/\d{1,2}/\d{4})", name
                    )
                    if date_str_match:
                        date_str = date_str_match.group(0)
                        if "-" in date_str:
                            return datetime.strptime(date_str, "%Y-%m-%d")
                        if "/" in date_str:
                            return datetime.strptime(
                                date_str, "%d/%m/%Y"
                            )  # Adjust format if needed
                    return datetime.min  # Cannot parse date, put at end
                except (ValueError, IndexError):
                    return datetime.min

            weeks_data.sort(key=get_week_date, reverse=True)
        except Exception as sort_err:
            logger.warning(
                f"Could not sort weeks based on date: {sort_err}. Returning in parsed order."
            )

        return weeks_data

    except Exception as e:
        logger.exception(f"Error during course content HTML parsing: {e}")
        return []  # Return empty list on major parsing error


def scrape_course_content(username: str, password: str, course_url: str) -> list | None:
    """
    Scrapes the content (weeks, materials) for a specific CMS course page.

    Args:
        username: User's GUC username.
        password: User's GUC password.
        course_url: The *normalized* URL of the course.

    Returns:
        list: A list of week dictionaries containing parsed content on success.
        None: On failure (auth, network, parsing errors).
    """
    if not course_url:
        logger.error("scrape_course_content called with empty course_url.")
        return None

    session = create_session(username, password)
    logger.info(f"Fetching CMS course content for {username} from {course_url}")

    response = make_request(session, course_url, method="GET")

    if not response:
        logger.error(
            f"Failed to fetch course content page for {username} from {course_url}."
        )
        return None  # Auth or connection error handled by make_request

    try:
        # Check for login page indicators even on success response
        if "login" in response.url.lower():
            soup_login_check = BeautifulSoup(response.text, "lxml")
            if soup_login_check.find(
                "form", action=lambda x: x and "login" in x.lower()
            ):
                logger.warning(
                    f"CMS Course Content: Detected login page redirect for {username} at {course_url}."
                )
                return None  # Treat as auth failure

        html_content = response.text
        if not html_content:
            logger.warning(f"Received empty HTML content for {course_url}")
            return None

        parsed_content = parse_course_content_html(html_content)

        # parse_course_content_html returns [] on parsing success but no weeks found,
        # or on major parsing error. It logs errors internally.
        # We return the list (even if empty) on success or partial success.
        # Return None only if the fetch itself failed initially.
        logger.info(
            f"Finished parsing course content for {username} from {course_url}. Found {len(parsed_content)} weeks."
        )
        return parsed_content

    except Exception as e:
        logger.exception(
            f"Unexpected error scraping course content for {username} at {course_url}: {e}"
        )
        return None


# --- CMS Course Announcements Scraping ---


def scrape_course_announcements(
    username: str, password: str, course_url: str
) -> dict | None:
    """
    Scrapes the main announcement section from a specific CMS course page.

    Args:
        username: User's GUC username.
        password: User's GUC password.
        course_url: The *normalized* URL of the course.

    Returns:
        dict: {'announcements_html': '...'} containing the raw HTML of the announcement section.
        dict: {'error': '...'} if announcements section not found or other error.
        None: On critical failure (auth, network).
    """
    if not course_url:
        logger.error("scrape_course_announcements called with empty course_url.")
        return {"error": "Missing course URL"}

    session = create_session(username, password)
    logger.info(f"Fetching CMS course announcements for {username} from {course_url}")

    response = make_request(session, course_url, method="GET")

    if not response:
        logger.error(f"Failed to fetch course page for announcements: {course_url}")
        return None  # Auth or connection error

    try:
        # Check for login page indicators
        if "login" in response.url.lower():
            soup_login_check = BeautifulSoup(response.text, "lxml")
            if soup_login_check.find(
                "form", action=lambda x: x and "login" in x.lower()
            ):
                logger.warning(
                    f"CMS Announcements: Detected login page redirect for {username} at {course_url}."
                )
                return None  # Treat as auth failure

        # Use selectolax for speed
        tree = HTMLParser(response.text)
        # Find the specific announcement div (adjust ID if needed)
        # Common IDs: desc, GeneralAnnouncements, CourseDescription... check actual source
        announcement_div = tree.css_first(
            "div#ContentPlaceHolderright_ContentPlaceHoldercontent_desc"
        )  # Check this ID first
        if not announcement_div:
            # Add fallbacks if the ID changes or varies
            announcement_div = tree.css_first(
                "div#GeneralAnnouncements"
            )  # Example fallback
            if not announcement_div:
                # Try finding a div with a header like "General Announcement"
                headers = tree.css("h3, h4")  # Check common header tags
                for header in headers:
                    if "general announcement" in header.text().lower():
                        # Assume announcement div is the parent or a sibling? Needs inspection.
                        # This is less reliable. Parent is common.
                        announcement_div = header.parent
                        if announcement_div:
                            break
                if not announcement_div:
                    logger.warning(
                        f"Course announcement section not found on {course_url} for {username}."
                    )
                    return {"error": "Announcement section not found"}

        # Get the inner HTML content of the div
        # html_content = announcement_div.innerHTML # selectolax way
        # Use decode_contents with BS4 if innerHTML causes issues or need BS4 processing
        soup_div = BeautifulSoup(announcement_div.html, "lxml").find(
            "div"
        )  # Re-parse fragment if needed
        html_content = soup_div.decode_contents() if soup_div else announcement_div.html

        logger.info(f"Successfully scraped course announcements from {course_url}")
        # Return the raw HTML content within the expected key
        return {"announcements_html": html_content}

    except Exception as e:
        logger.exception(
            f"Error scraping course announcements for {username} at {course_url}: {e}"
        )
        return {"error": f"Unexpected error during announcement scraping: {e}"}


# --- Combined CMS Scraper (Refactored) ---
# This function now orchestrates fetching course list OR specific content/announcements


def cms_scraper(
    username: str, password: str, course_url: str = None, force_refresh: bool = False
) -> list | dict | None:
    """
    Main function to scrape CMS data.

    - If course_url is None: Fetches the list of all courses for the user.
    - If course_url is provided: Fetches content and announcements for that specific course.

    Args:
        username (str): User's GUC username.
        password (str): User's GUC password.
        course_url (str, optional): The specific course URL to scrape. Defaults to None.
        force_refresh (bool, optional): If True, bypasses cache for the course list. Defaults to False.

    Returns:
        list: List of course dicts if course_url is None.
        dict: Dict containing {'course_content': list, 'course_announcement': dict | None} if course_url is provided.
              The announcement dict is {'announcements_html': '...'}.
        None: On critical failures (auth, network).
    """
    if course_url:
        # --- Fetch Specific Course Data ---
        normalized_url = normalize_course_url(course_url)
        if not normalized_url:
            return {
                "error": "Invalid course URL provided."
            }  # Return error dict for specific course failure

        logger.info(f"Scraping specific CMS course: {username} - {normalized_url}")

        # Fetch content and announcements concurrently
        content_list = None
        announcement_result = None
        fetch_success = False

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="CourseData"
        ) as executor:
            content_future = executor.submit(
                scrape_course_content, username, password, normalized_url
            )
            announcement_future = executor.submit(
                scrape_course_announcements, username, password, normalized_url
            )
            try:
                content_list = content_future.result()  # Returns list or None
                if content_list is not None:
                    fetch_success = True  # Success even if list is empty
            except Exception as e:
                logger.error(f"Course content future error: {e}")
            try:
                announcement_result = (
                    announcement_future.result()
                )  # Returns dict or None
                if announcement_result is not None:
                    fetch_success = True  # Success even if {'error':...}
            except Exception as e:
                logger.error(f"Course announcement future error: {e}")

        if not fetch_success:
            logger.error(
                f"Both content and announcement fetch failed for specific course: {normalized_url}"
            )
            # Decide return value: None indicates total failure, dict indicates partial/specific failure
            return {"error": "Failed to fetch data for the specified course."}

        # Structure the result for a single course
        # announcement_result might be None or {'error':...} or {'announcements_html':...}
        course_data = {
            "course_url": normalized_url,
            "course_content": (
                content_list if content_list is not None else []
            ),  # Return empty list on content fetch failure
            "course_announcement": announcement_result,  # Pass through the result (None, error dict, or success dict)
        }
        return course_data

    else:
        # --- Fetch Course List ---
        logger.info(f"Fetching CMS course list for user: {username}")
        # Cache logic for course list needs to be handled here if desired
        # For simplicity in refactor, fetching fresh list each time unless cached by a separate mechanism
        # If force_refresh is used elsewhere, it might apply here too.
        # cache_key = generate_cache_key("cms_courses", username) etc.
        courses = scrape_cms_courses(username, password)
        # scrape_cms_courses returns list or None
        return courses  # Return the list of courses or None on failure
