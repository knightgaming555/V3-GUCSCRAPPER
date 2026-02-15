# scraping/cms.py
import logging
import concurrent.futures
import re
import json
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser
import requests
from datetime import datetime

try:
    from .core import create_session, make_request
    from utils.helpers import normalize_course_url
    from config import config
except ImportError:
    from scraping.core import create_session, make_request
    from utils.helpers import normalize_course_url
    from config import config

logger = logging.getLogger(__name__)

DACAST_INFO_URL_TEMPLATE = "https://playback.dacast.com/content/info?contentId={player_content_id}&provider=dacast"
DACAST_ACCESS_URL_TEMPLATE = "https://playback.dacast.com/content/access?contentId={actual_content_id}&provider=universe"
DACAST_REQUEST_TIMEOUT = 10
DACAST_HEADERS = {"User-Agent": "Mozilla/5.0"}


# --- scrape_cms_courses --- (No changes)
def scrape_cms_courses(username: str, password: str) -> list | None:
    # ... (previous correct code) ...
    cms_home_url = config.CMS_HOME_URL
    session = create_session(username, password)
    courses = []
    logger.info(f"Fetching CMS course list for {username} from {cms_home_url}")
    response = make_request(session, cms_home_url, method="GET")
    if not response:
        return None
    try:
        if "login" in response.url.lower():
            soup_login_check = BeautifulSoup(response.text, "lxml")
            if soup_login_check.find(
                "form", action=lambda x: x and "login" in x.lower()
            ):
                logger.warning(
                    f"CMS Course List: Detected login page redirect for {username}."
                )
                return None
        tree = HTMLParser(response.text)
        table = tree.css_first(
            "#ContentPlaceHolderright_ContentPlaceHoldercontent_GridViewcourses"
        )
        if not table:
            no_courses_indicator = tree.css_first(
                "span#ContentPlaceHolderright_ContentPlaceHoldercontent_LabelNoCourses"
            )
            if (
                no_courses_indicator
                and "no courses" in no_courses_indicator.text().lower()
            ):
                logger.info(f"User {username} has no courses enrolled on CMS.")
                return []
            logger.warning(
                f"CMS courses table not found on {cms_home_url} for {username}."
            )
            return None
        rows = table.css("tr")
        if len(rows) <= 1:
            return []
        for row in rows[1:]:
            cells = row.css("td")
            if len(cells) >= 6:
                try:
                    course_name, course_id_raw, season_id_raw, season_name = (
                        cells[1].text(strip=True),
                        cells[4].text(strip=True),
                        cells[5].text(strip=True),
                        cells[3].text(strip=True),
                    )

                    # Clean trailing ".?" from IDs, as reported in logs
                    course_id = course_id_raw.removesuffix(".?") if course_id_raw else None
                    season_id = season_id_raw.removesuffix(".?") if season_id_raw else None

                    if course_id and season_id:
                        rel_path = f"/apps/student/CourseViewStn.aspx?id={course_id}&sid={season_id}"
                        course_url = urljoin(config.BASE_CMS_URL, rel_path)
                        courses.append(
                            {
                                "course_name": course_name,
                                "course_url": normalize_course_url(course_url),
                                "season_name": season_name,
                            }
                        )
                    else:
                        logger.warning(
                            f"Skipping row due to missing course_id/season_id for {username}: {row.html[:100]}"
                        )
                except Exception as cell_err:
                    logger.error(
                        f"Error parsing course row cells for {username}: {cell_err} - Row HTML: {row.html[:100]}",
                        exc_info=False,
                    )
            else:
                logger.warning(
                    f"Skipping course row with insufficient cells ({len(cells)}) for {username}."
                )
        logger.info(f"Successfully scraped {len(courses)} courses for {username}.")
        return courses
    except Exception as e:
        logger.exception(f"Unexpected error scraping CMS courses for {username}: {e}")
        return None


# --- _get_dacast_access_url --- (No changes)
def _get_dacast_access_url(player_content_id: str) -> str | None:
    # ... (previous correct code) ...
    if not player_content_id:
        return None
    info_url = DACAST_INFO_URL_TEMPLATE.format(player_content_id=player_content_id)
    logger.debug(f"Fetching Dacast info URL: {info_url}")
    try:
        response = requests.get(
            info_url,
            timeout=DACAST_REQUEST_TIMEOUT,
            headers=DACAST_HEADERS,
            verify=config.VERIFY_SSL,
        )
        response.raise_for_status()
        data = response.json()
        actual_content_id = data.get("contentInfo", {}).get("contentId")
        if not actual_content_id:
            logger.error(
                f"Could not find 'contentInfo.contentId' in Dacast info response for {player_content_id}. Response: {data}"
            )
            return None
        access_url = DACAST_ACCESS_URL_TEMPLATE.format(
            actual_content_id=actual_content_id
        )
        logger.debug(f"Constructed Dacast access URL: {access_url}")
        return access_url
    except requests.exceptions.RequestException as req_err:
        status_code = (
            req_err.response.status_code if req_err.response is not None else "N/A"
        )
        logger.error(
            f"Network error fetching Dacast info for {player_content_id} (Status: {status_code}): {req_err}"
        )
        return None
    except json.JSONDecodeError as json_err:
        logger.error(
            f"Error decoding Dacast JSON response for {player_content_id}: {json_err}. Response text: {response.text[:200]}"
        )
        return None
    except KeyError as key_err:
        logger.error(
            f"Missing key in Dacast info response for {player_content_id}: {key_err}. Response: {data}"
        )
        return None
    except Exception as e:
        logger.error(
            f"Unexpected error getting Dacast access URL for {player_content_id}: {e}",
            exc_info=True,
        )
        return None


# --- _parse_content_item --- (FINAL CORRECTION)
def _parse_content_item(card_node) -> dict | None:
    """Parses a single content item card, handling VODs and downloads correctly."""
    title_text = "Unknown Content"
    item_url = None
    # Determine type based on visible buttons, default to Info
    item_type = "Info"

    try:
        # 1. Find Title
        title_div = card_node.css_first("div[id^='content']")
        if title_div:
            title_text = (
                title_div.text(strip=True, separator=" ")
                .replace("\n", " ")
                .replace("\r", "")
                .strip()
            )
        else:
            h_tag = card_node.css_first("h5, h6")
            if h_tag:
                title_text = h_tag.text(strip=True)
            else:
                logger.debug(
                    f"Could not find title for card. HTML: {card_node.html[:100]}"
                )
                return None  # Cannot proceed without a title

        # 2. Find potential buttons/links
        vod_button_node = card_node.css_first("input.vodbutton[data-toggle='modal']")
        download_link_node = card_node.css_first("a#download, a.contentbtn[download]")

        # 3. Determine VISIBILITY
        vod_is_visible = (
            vod_button_node
            and "display:none"
            not in vod_button_node.attributes.get("style", "").replace(" ", "")
        )
        download_is_visible = (
            download_link_node
            and "display:none"
            not in download_link_node.attributes.get("style", "").replace(" ", "")
        )

        # 4. Process based on VISIBLE button type
        if vod_is_visible:
            item_type = "VOD"  # Set type definitively
            player_content_id = vod_button_node.attributes.get("id")

            if not player_content_id:
                logger.warning(
                    f"Visible VOD button found for '{title_text}' but lacks an ID. URL will be null."
                )
                item_url = None
            else:
                # --- NEW LOGIC: Check if player_id already contains '-vod-' ---
                if "-vod-" in player_content_id:
                    # Assume player_id IS the actual content ID
                    item_url = DACAST_ACCESS_URL_TEMPLATE.format(
                        actual_content_id=player_content_id
                    )
                    logger.info(
                        f"VOD '{title_text}' ID contains '-vod-'. Using direct ID '{player_content_id}' for access URL."
                    )
                else:
                    # ID doesn't contain '-vod-', need to perform the API lookup
                    logger.info(
                        f"VOD '{title_text}' ID '{player_content_id}' lacks '-vod-'. Fetching actual ID via Dacast API..."
                    )
                    item_url = _get_dacast_access_url(player_content_id)
                    if not item_url:
                        logger.warning(
                            f"Could not retrieve Dacast access URL for VOD '{title_text}' (Player ID: {player_content_id}). URL set to null."
                        )
                        item_url = None  # Ensure null on failure

        elif download_is_visible:
            item_type = "Download"  # Set type definitively
            href = download_link_node.attributes.get("href")
            if href:
                item_url = urljoin(config.BASE_CMS_URL, href)
                logger.debug(f"Found Download: '{title_text}'. URL: {item_url}")
            else:
                logger.warning(
                    f"Found download link for '{title_text}' but href is empty."
                )
                item_url = None
        else:
            # Neither visible VOD nor visible Download - Keep type as "Info"
            item_type = "Info"
            logger.debug(
                f"Content item '{title_text}' has no visible VOD or download action."
            )
            item_url = None

        # 5. Return structured data WITHOUT the "type" key
        return {"title": title_text, "download_url": item_url}

    except Exception as e:
        logger.error(
            f"Error parsing content item '{title_text}': {e}. Card HTML: {card_node.html[:100]}",
            exc_info=True,
        )
        if title_text != "Unknown Content":
            # Return structure without type on error as well
            return {"title": title_text, "download_url": None}
        return None


# --- _parse_single_week --- (No changes needed)
def _parse_single_week(week_div_node) -> dict | None:
    week_name = "Unknown Week"
    try:
        week_title_tag = week_div_node.css_first("h2.text-big")
        if week_title_tag:
            week_name = week_title_tag.text(strip=True)
        week_data = {
            "week_name": week_name,
            "announcement": "",
            "description": "",
            "contents": [],
        }
        p3_div = week_div_node.css_first("div.p-3")
        if p3_div:
            info_divs = p3_div.css("div > strong")
            content_header_found = False
            for strong_tag in info_divs:
                header_text, parent_div = (
                    strong_tag.text(strip=True).lower(),
                    strong_tag.parent,
                )
                is_hidden = "display:none" in parent_div.attributes.get(
                    "style", ""
                ).replace(" ", "")
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
                    if next_node.tag == "div" and (
                        next_node.css_matches("div > strong")
                        or next_node.css_first(".card.mb-4")
                    ):
                        break
                    next_node = next_node.next
                if "announcement" in header_text and not is_hidden:
                    week_data["announcement"] = para_text
                elif "description" in header_text:
                    week_data["description"] = para_text
                elif "content" in header_text:
                    content_header_found = True
                    break
            content_cards = p3_div.css(".card.mb-4")
            if content_cards:
                contents = [_parse_content_item(card) for card in content_cards]
                week_data["contents"] = [c for c in contents if c]
        if week_name == "Unknown Week" and not week_title_tag:
            return None
        return week_data
    except Exception as e:
        logger.error(
            f"Error parsing single week '{week_name}': {e}. Week HTML: {week_div_node.html[:100]}",
            exc_info=False,
        )
        return None


# --- parse_course_content_html --- (No changes needed)
def parse_course_content_html(html_content: str) -> list:
    weeks = []
    if not html_content:
        return weeks
    try:
        tree = HTMLParser(html_content)
        week_divs = tree.css(".weeksdata")
        if not week_divs:
            logger.warning("No week sections found (selector '.weeksdata').")
            return weeks
        weeks_data = [_parse_single_week(div) for div in week_divs]
        weeks_data = [w for w in weeks_data if w]
        # Assuming weeks are scraped in the desired order (newest to oldest)
        # from the HTML structure. No explicit sort or reverse is applied.
        return weeks_data
    except Exception as e:
        logger.exception(f"Error during course content HTML parsing: {e}")
        return []


# --- scrape_course_content --- (No changes needed)
def scrape_course_content(username: str, password: str, course_url: str) -> list | None:
    if not course_url:
        return None
    session = create_session(username, password)
    logger.info(f"Fetching CMS course content for {username} from {course_url}")
    response = make_request(session, course_url, method="GET")
    if not response:
        return None
    try:
        if "login" in response.url.lower():
            from bs4 import BeautifulSoup

            soup_login_check = BeautifulSoup(response.text, "lxml")
            if soup_login_check.find(
                "form", action=lambda x: x and "login" in x.lower()
            ):
                return None
        html_content = response.text
        if not html_content:
            return None
        parsed_content = parse_course_content_html(html_content)
        logger.info(
            f"Finished parsing course content for {username} from {course_url}. Found {len(parsed_content)} weeks."
        )
        return parsed_content
    except Exception as e:
        logger.exception(
            f"Unexpected error scraping course content for {username} at {course_url}: {e}"
        )
        return None


# --- scrape_course_announcements --- (No changes needed)
def scrape_course_announcements(
    username: str, password: str, course_url: str
) -> dict | None:
    if not course_url:
        return {"error": "Missing course URL"}
    session = create_session(username, password)
    logger.info(f"Fetching CMS course announcements for {username} from {course_url}")
    response = make_request(session, course_url, method="GET")
    if not response:
        logger.error(f"Failed to fetch course page for announcements: {course_url}")
        return None
    try:
        if "login" in response.url.lower():
            from bs4 import BeautifulSoup

            soup_login_check = BeautifulSoup(response.text, "lxml")
            if soup_login_check.find(
                "form", action=lambda x: x and "login" in x.lower()
            ):
                return None
        tree = HTMLParser(response.text)
        announcement_div = tree.css_first(
            "div#ContentPlaceHolderright_ContentPlaceHoldercontent_desc"
        )
        if not announcement_div:
            announcement_div = tree.css_first("div.p-xl-2")
            if not announcement_div:
                logger.warning(
                    f"Course announcement section not found on {course_url} for {username}."
                )
                return {"error": "Announcement section not found"}
            else:
                logger.info(
                    f"Found potential announcement section using fallback selector 'div.p-xl-2' on {course_url}"
                )
        html_content = announcement_div.html.strip() if announcement_div.html else ""
        logger.info(f"Successfully scraped course announcements from {course_url}")
        return {"announcements_html": html_content}
    except Exception as e:
        logger.exception(
            f"Error scraping course announcements for {username} at {course_url}: {e}"
        )
        return {"error": f"Unexpected error during announcement scraping: {e}"}


# --- Combined CMS Scraper --- (No changes needed)
def cms_scraper(
    username: str, password: str, course_url: str = None, force_refresh: bool = False
) -> list | dict | None:
    if course_url:
        normalized_url = normalize_course_url(course_url)
        if not normalized_url:
            return {"error": "Invalid course URL provided."}
        logger.info(f"Scraping specific CMS course: {username} - {normalized_url}")
        content_list, announcement_result, fetch_success = None, None, False
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
                content_list = content_future.result()
                fetch_success = fetch_success or (content_list is not None)
            except Exception as e:
                logger.exception(f"Exception retrieving content future result: {e}")
            try:
                announcement_result = announcement_future.result()
                fetch_success = fetch_success or (announcement_result is not None)
            except Exception as e:
                logger.exception(
                    f"Exception retrieving announcement future result: {e}"
                )
        if not fetch_success:
            logger.error(
                f"Both content/announcement fetch critically failed: {normalized_url}"
            )
            return None
        course_data = {
            "course_url": normalized_url,
            "course_content": (content_list if content_list is not None else []),
            "course_announcement": announcement_result,
        }
        return course_data
    else:
        logger.info(f"Fetching CMS course list for user: {username}")
        courses = scrape_cms_courses(username, password)
        return courses
