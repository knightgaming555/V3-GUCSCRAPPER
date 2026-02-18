import logging
import requests
from bs4 import BeautifulSoup
import re
import json
import ast
import time
from threading import Lock
import base64

# Project-specific imports
from utils.cache import get_from_cache, set_in_cache  # For global staff list caching
from config import config  # For cache timeouts

logger = logging.getLogger(__name__)

BASE_URL = "https://apps.guc.edu.eg"
STAFF_SCHEDULE_URL = "https://apps.guc.edu.eg/student_ext/Scheduling/SearchAcademicScheduled_001.aspx"
# This URL is no longer used for POSTing, but kept for reference
STAFF_PROFILE_SEARCH_URL = "https://apps.guc.edu.eg/student_ext/UserProfile/UserProfileSearch.aspx"
STAFF_PROFILE_VIEW_URL_TEMPLATE = "https://apps.guc.edu.eg/student_ext/UserProfile/UserProfileView.aspx?id={}"


GLOBAL_STAFF_LIST_CACHE_KEY = "global_staff_list_v2"
GLOBAL_STAFF_LIST_CACHE_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 hours

SCHEDULE_SLOT_TIMINGS = {
    0: "8:30AM-9:40AM",
    1: "9:45AM-10:55AM",
    2: "11:00AM-12:10PM",
    3: "12:20PM-1:30PM",
    4: "1:35PM-2:45PM",
    5: "5:30PM-7:00PM",
    6: "7:15PM-8:45PM",
    7: "9:00PM-10:30PM"
}

# small lock to prevent cache stampede when many requests race on miss
_global_staff_lock = Lock()

# ---------------------------
# Utilities
# ---------------------------
def _extract_asp_tokens(soup: BeautifulSoup):
    tokens = {}
    try:
        tokens['viewstate'] = soup.find('input', {'name': '__VIEWSTATE'})['value']
        tokens['viewstate_generator'] = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value']
        tokens['event_validation'] = soup.find('input', {'name': '__EVENTVALIDATION'})['value']
    except TypeError:
        missing = [token for token in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']
                   if not soup.find('input', {'name': token})]
        logger.error(f"Could not find ASP tokens: {', '.join(missing)}")
        return None
    if not all(tokens.values()):
        empty_tokens = [name for name, value in tokens.items() if not value]
        logger.error(f"One or more ASP tokens were empty after extraction: {', '.join(empty_tokens)}")
        return None
    return tokens

def _normalize_staff_name(name: str) -> str:
    return " ".join(name.lower().split())

def _match_staff_name(name_to_find_normalized: str, option_name_normalized: str) -> bool:
    if option_name_normalized == name_to_find_normalized:
        return True
    s_name_parts = set(name_to_find_normalized.split())
    o_text_parts = set(option_name_normalized.split())
    if s_name_parts == o_text_parts:
        return True
    s_name_flex = name_to_find_normalized.replace('s', '').replace('u', '')
    o_text_flex = option_name_normalized.replace('s', '').replace('u', '')
    first_name_part_to_find = name_to_find_normalized.split()[0]
    if s_name_flex == o_text_flex and option_name_normalized.startswith(first_name_part_to_find):
        return True
    if s_name_parts.intersection(o_text_parts) and \
       name_to_find_normalized.split()[0] == option_name_normalized.split()[0]:
        if len(s_name_parts.intersection(o_text_parts)) >= max(1, min(len(s_name_parts), len(o_text_parts)) - 1):
            return True
    return False

def _extract_all_staff_details(soup: BeautifulSoup) -> list[dict[str, str]] | None:
    all_staff = []
    unique_staff_ids = set()
    html_content_str = str(soup)
    match = re.search(r'(?:var\s+)?tas\s*=\s*(\[[\s\S]*?\])\s*;?', html_content_str, re.DOTALL | re.IGNORECASE)
    if match:
        try:
            tas_list_data = ast.literal_eval(match.group(1).strip())
            for item in tas_list_data:
                if isinstance(item, dict) and 'id' in item and 'value' in item:
                    staff_id, staff_name = str(item['id']), item['value'].strip()
                    if staff_id.isdigit() and staff_name and staff_id not in unique_staff_ids:
                        all_staff.append({'id': staff_id, 'name': staff_name})
                        unique_staff_ids.add(staff_id)
        except Exception as e:
            logger.error(f"Could not parse 'var tas' from JS: {e}")
    if not all_staff:
        # fallback to scanning select tags
        select_tags = soup.find_all('select')
        for tag in select_tags:
            options = tag.find_all('option')
            if len(options) > 100:
                for option in options:
                    value, text = option.get('value'), option.text.strip()
                    if value and text and value.isdigit() and value not in unique_staff_ids:
                        all_staff.append({'id': value, 'name': text})
                        unique_staff_ids.add(value)
                break
    logger.info(f"Total unique staff details extracted: {len(all_staff)}")
    return all_staff if all_staff else None

# ---------------------------
# Public scraping helpers
# ---------------------------
def get_global_staff_list_and_tokens(session: requests.Session, force_refresh: bool = False) -> tuple[list[dict[str, str]] | None, dict | None]:
    """
    Fetch global staff list and ASP tokens. Keeps behavior same as before regarding token caching.
    Adds a tiny lock to prevent stampede on cache miss.
    """
    cached_staff_list = get_from_cache(GLOBAL_STAFF_LIST_CACHE_KEY) if not force_refresh else None
    if cached_staff_list:
        logger.info(f"Global staff list cache hit. Found {len(cached_staff_list)} staff members.")
        try:
            response = session.get(
                STAFF_SCHEDULE_URL, timeout=10, verify=config.VERIFY_SSL
            )
            response.raise_for_status()
            fresh_tokens = _extract_asp_tokens(BeautifulSoup(response.text, 'lxml'))
            return cached_staff_list, fresh_tokens
        except Exception as e:
            logger.warning(f"Could not refresh tokens on cache hit: {e}")
            return cached_staff_list, None

    if _global_staff_lock.acquire(blocking=False):
        try:
            logger.info("Cache miss: fetching fresh global staff list and tokens.")
            response = session.get(
                STAFF_SCHEDULE_URL, timeout=15, verify=config.VERIFY_SSL
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            asp_tokens = _extract_asp_tokens(soup)
            staff_list = _extract_all_staff_details(soup)
            if staff_list:
                set_in_cache(GLOBAL_STAFF_LIST_CACHE_KEY, staff_list, timeout=GLOBAL_STAFF_LIST_CACHE_TIMEOUT_SECONDS)
                logger.info(f"Successfully fetched and cached {len(staff_list)} staff members.")
            return staff_list, asp_tokens
        except Exception as e:
            logger.error(f"Request failed for initial schedule page for global staff list: {e}")
            return None, None
        finally:
            _global_staff_lock.release()
    else:
        wait_total = 0.0
        while wait_total < 2.0:  # wait up to 2 seconds
            time.sleep(0.08)
            wait_total += 0.08
            cached = get_from_cache(GLOBAL_STAFF_LIST_CACHE_KEY)
            if cached:
                try:
                    response = session.get(
                        STAFF_SCHEDULE_URL, timeout=6, verify=config.VERIFY_SSL
                    )
                    response.raise_for_status()
                    fresh_tokens = _extract_asp_tokens(BeautifulSoup(response.text, 'lxml'))
                    return cached, fresh_tokens
                except Exception:
                    return cached, None
        try:
            response = session.get(
                STAFF_SCHEDULE_URL, timeout=12, verify=config.VERIFY_SSL
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            asp_tokens = _extract_asp_tokens(soup)
            staff_list = _extract_all_staff_details(soup)
            if staff_list:
                set_in_cache(GLOBAL_STAFF_LIST_CACHE_KEY, staff_list, timeout=GLOBAL_STAFF_LIST_CACHE_TIMEOUT_SECONDS)
            return staff_list, asp_tokens
        except Exception as e:
            logger.error(f"Final fallback fetch failed: {e}")
            return None, None

def _find_staff_id_from_list(staff_list: list[dict[str, str]], staff_name: str) -> str | None:
    if not staff_list: return None
    staff_name_normalized = _normalize_staff_name(staff_name)
    exact_match_id, flexible_matches = None, []
    for member in staff_list:
        option_name_normalized = _normalize_staff_name(member['name'])
        if option_name_normalized == staff_name_normalized:
            exact_match_id = member['id']
            break
        if _match_staff_name(staff_name_normalized, option_name_normalized):
            flexible_matches.append(member['id'])
    if exact_match_id:
        logger.info(f"Found exact match for staff_id: {exact_match_id} for name: '{staff_name}'.")
        return exact_match_id
    if flexible_matches:
        if len(flexible_matches) > 1:
            logger.warning(f"Multiple flexible matches for '{staff_name}': {flexible_matches}. Returning first one.")
        return flexible_matches[0]
    logger.warning(f"Staff ID for '{staff_name}' not found.")
    return None

def parse_staff_schedule(html_content: str, requested_staff_ids: list[str] | None = None):
    all_schedules = {}
    try:
        m = re.search(
            r'(<table[^>]*id=["\']ContentPlaceHolderright_ContentPlaceHoldercontent_schedule["\'][\s\S]*?</table>)',
            html_content, re.IGNORECASE)
        table_html = m.group(1) if m else None
        if table_html:
            soup = BeautifulSoup(table_html, 'lxml')
            table = soup.find('table')
        else:
            soup = BeautifulSoup(html_content, 'lxml')
            table = soup.select_one('table#ContentPlaceHolderright_ContentPlaceHoldercontent_schedule')
        if not table:
            return {}
        days = ["Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        for i, row in enumerate(table.find_all('tr')[1:]):
            if i >= len(days): continue
            cells = row.find_all(['td', 'th'])
            for slot_idx, cell in enumerate(cells[1:]):
                if slot_idx >= len(SCHEDULE_SLOT_TIMINGS): continue
                for slot_div in cell.select('div.slot'):
                    staff_id = slot_div.get('data-staff-id')
                    if not staff_id: continue
                    schedule = all_schedules.setdefault(staff_id, {})
                    day_schedule = schedule.setdefault(days[i], {})
                    group_dd = slot_div.find('dt', string='Group')
                    group_val = 'N/A'
                    if group_dd:
                        dd = group_dd.find_next_sibling('dd')
                        if dd: group_val = dd.text.strip()
                    loc_dd = slot_div.find('dt', string='Location')
                    loc_val = 'N/A'
                    if loc_dd:
                        dd2 = loc_dd.find_next_sibling('dd')
                        if dd2: loc_val = dd2.text.strip()
                    details = {
                        'time': SCHEDULE_SLOT_TIMINGS.get(slot_idx),
                        'group': group_val,
                        'location': loc_val
                    }
                    day_schedule.setdefault(slot_idx, []).append(details)
        return all_schedules
    except Exception as e:
        logger.error(f"Error parsing staff schedule HTML: {e}", exc_info=True)
        return {}

def scrape_staff_schedule_only(session: requests.Session, staff_id: str, asp_tokens: dict) -> dict | None:
    logger.info(f"Making POST request for schedule of staff ID: {staff_id}")
    if not all(asp_tokens.get(k) for k in ['viewstate', 'viewstate_generator', 'event_validation']):
        logger.error("Payload for schedule scrape is missing essential ASP token fields.")
        return None
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; GUC-Scraper/1.0)",
        "Referer": STAFF_SCHEDULE_URL,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    payload = {
        '__EVENTTARGET': '', '__EVENTARGUMENT': '', '__LASTFOCUS': '',
        '__VIEWSTATE': asp_tokens.get('viewstate'),
        '__VIEWSTATEGENERATOR': asp_tokens.get('viewstate_generator'),
        '__EVENTVALIDATION': asp_tokens.get('event_validation'),
        'ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$DDLSession': '0',
        'ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$B_ShowSchedule': 'Show Schedule',
        'ta[]': staff_id
    }
    try:
        response = session.post(
            STAFF_SCHEDULE_URL, data=payload, timeout=15, verify=config.VERIFY_SSL
        )
        response.raise_for_status()
        parsed_schedules = parse_staff_schedule(response.text, requested_staff_ids=[staff_id])
        if not parsed_schedules or staff_id not in parsed_schedules:
            logger.warning(f"Parsing returned no schedule for staff ID {staff_id}. Page might be empty.")
            return {}
        logger.info(f"Successfully scraped and parsed schedule for staff ID {staff_id}.")
        return parsed_schedules
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to scrape schedule for staff ID {staff_id}: {e}")
        return None

def get_staff_profile_details(session: requests.Session, staff_name: str, staff_id: str) -> dict:
    """
    Fetches staff profile details directly using their ID, bypassing the problematic search form.
    This works for any staff member, regardless of their connection to the logged-in student.
    """
    logger.info(f"--- Starting DIRECT profile fetch for: '{staff_name}' (ID: {staff_id}) ---")
    if not staff_id or not staff_id.isdigit():
        logger.error(f"Invalid staff_id provided for '{staff_name}': {staff_id}")
        return {"error": "Invalid staff ID provided."}
    
    try:
        # Step 1: Base64-encode the staff ID
        encoded_id = base64.b64encode(staff_id.encode('ascii')).decode('ascii')
        
        # Step 2: Construct the direct URL to the profile page
        profile_url = STAFF_PROFILE_VIEW_URL_TEMPLATE.format(encoded_id)
        logger.info(f"Step 1: Making GET request to direct profile URL: {profile_url}")

        # Step 3: Make a GET request to the profile page
        profile_response = session.get(
            profile_url, timeout=15, verify=config.VERIFY_SSL
        )
        logger.info(f"Direct profile GET request completed with status: {profile_response.status_code}")
        profile_response.raise_for_status()

        # Step 4: Parse the response for email and office details
        logger.info("Step 2: Parsing final profile page for details.")
        profile_soup = BeautifulSoup(profile_response.text, 'lxml')
        email_tag = profile_soup.find('a', id='ContentPlaceHolderright_ContentPlaceHoldercontent_HyperLinkEmail')
        office_tag = profile_soup.find('span', id='ContentPlaceHolderright_ContentPlaceHoldercontent_LblOffice')

        email = email_tag.get_text(strip=True) if email_tag else "N/A"
        office = office_tag.get_text(strip=True) if office_tag and office_tag.get_text(strip=True) else "N/A"

        if email == "N/A" and office == "N/A":
            logger.warning(f"Profile page for '{staff_name}' was loaded, but no email or office details were found.")
        
        profile_data = {'email': email, 'office': office}
        logger.info(f"--- Successfully extracted profile details for '{staff_name}': {profile_data} ---")
        return profile_data

    except requests.exceptions.RequestException as e:
        logger.error(f"A network error occurred during direct profile fetch for '{staff_name}': {e}", exc_info=True)
        return {"error": f"Upstream network error while fetching profile: {e}"}
    except Exception as e:
        logger.error(f"An unexpected error occurred during direct profile scraping for '{staff_name}': {e}", exc_info=True)
        return {"error": "An internal error occurred during profile scraping."}
