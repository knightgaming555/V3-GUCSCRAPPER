import logging
import requests
from bs4 import BeautifulSoup
import re
import json
import ast # Moved import ast to top level

# Project-specific imports
from utils.cache import get_from_cache, set_in_cache # For global staff list caching
from config import config # For cache timeouts

logger = logging.getLogger(__name__)

BASE_URL = "https://apps.guc.edu.g" # Base URL for constructing full URLs if needed
STAFF_SCHEDULE_URL = "https://apps.guc.edu.eg/student_ext/Scheduling/SearchAcademicScheduled_001.aspx"

# Cache configuration for global staff list
GLOBAL_STAFF_LIST_CACHE_KEY = "global_staff_list_v2" # Added v2 for potential structure changes
GLOBAL_STAFF_LIST_CACHE_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 hours

# Slot timings from the main schedule endpoint, assuming they are consistent.
# These map to the 0-indexed column of the schedule table.
SCHEDULE_SLOT_TIMINGS = {
    0: "8:15AM-9:45AM",    # 1st Slot
    1: "10:00AM-11:30AM",  # 2nd Slot
    2: "11:45AM-1:15PM",   # 3rd Slot
    3: "1:45PM-3:15PM",    # 4th Slot
    4: "3:45PM-5:15PM",    # 5th Slot
    5: "Unknown Slot 6",   # 6th Slot - Placeholder, confirm if used/time
    6: "Unknown Slot 7",   # 7th Slot - Placeholder
    7: "Unknown Slot 8",   # 8th Slot - Placeholder
}

def _extract_asp_tokens(soup: BeautifulSoup):
    tokens = {}
    try:
        tokens['viewstate'] = soup.find('input', {'name': '__VIEWSTATE'})['value']
        tokens['viewstate_generator'] = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value']
        tokens['event_validation'] = soup.find('input', {'name': '__EVENTVALIDATION'})['value']
    except TypeError: # Handles case where find returns None, then tries to access ['value']
        missing = [token for token in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION'] if not soup.find('input', {'name': token})]
        logger.error(f"Could not find ASP tokens: {', '.join(missing)}")
        return None
    # Check for empty string values which are also problematic
    if not all(tokens.values()): 
        empty_tokens = [name for name, value in tokens.items() if not value]
        logger.error(f"One or more ASP tokens were empty after extraction: {', '.join(empty_tokens)}")
        return None
    return tokens

def _normalize_staff_name(name: str) -> str:
    """Normalizes a staff name for comparison."""
    return " ".join(name.lower().split())

def _match_staff_name(name_to_find_normalized: str, option_name_normalized: str) -> bool:
    """Performs flexible matching for staff names."""
    if option_name_normalized == name_to_find_normalized:
        return True

    s_name_parts = set(name_to_find_normalized.split())
    o_text_parts = set(option_name_normalized.split())
    if s_name_parts == o_text_parts: # handles different ordering if split by space
        return True
    
    s_name_flex = name_to_find_normalized.replace('s', '').replace('u', '')
    o_text_flex = option_name_normalized.replace('s', '').replace('u', '')
    first_name_part_to_find = name_to_find_normalized.split()[0]
    if s_name_flex == o_text_flex and option_name_normalized.startswith(first_name_part_to_find):
        return True
    
    if s_name_parts.intersection(o_text_parts) and \
       name_to_find_normalized.split()[0] == option_name_normalized.split()[0]:
        if len(s_name_parts.intersection(o_text_parts)) >= max(1, min(len(s_name_parts), len(o_text_parts)) -1):
            return True
    return False

def _extract_all_staff_details(soup: BeautifulSoup) -> list[dict[str, str]] | None:
    """Extracts all staff names and IDs from the page content."""
    all_staff = []
    unique_staff_ids = set() # To avoid duplicates if staff appear in multiple sources

    # Attempt 1: Find in <select name='ta[]'> options (primary target)
    select_tag = soup.find('select', {'name': 'ta[]'})
    if select_tag:
        options = select_tag.find_all('option')
        for option in options:
            option_text = option.text.strip()
            option_value = option.get('value')
            if option_value and option_text and option_value.isdigit() and option_value not in unique_staff_ids:
                all_staff.append({'id': option_value, 'name': option_text})
                unique_staff_ids.add(option_value)
        logger.info(f"Extracted {len(all_staff)} staff from <select name='ta[]'>.")
    else:
        logger.warning("Could not find <select name='ta[]'>. Will check other selects and JS var.")

    # Attempt 2: Find in any other <select> options that look like staff lists
    if not select_tag or not all_staff: # If primary select not found or yielded no staff
        potential_selects = soup.find_all('select')
        for potential_select in potential_selects:
            if potential_select.get('name') == 'ta[]': continue # Already processed

            current_select_options = potential_select.find_all('option')
            if len(current_select_options) > 5: 
                temp_staff_from_select = []
                temp_ids_from_select = set()
                for option in current_select_options:
                    option_text = option.text.strip()
                    option_value = option.get('value')
                    if option_value and option_text and option_value.isdigit() and option_value not in unique_staff_ids and option_value not in temp_ids_from_select:
                        temp_staff_from_select.append({'id': option_value, 'name': option_text})
                        temp_ids_from_select.add(option_value)
                
                if temp_staff_from_select:
                    plausible_names = [s['name'] for s in temp_staff_from_select if len(s['name'].split()) >=2]
                    if len(plausible_names) > len(temp_staff_from_select) * 0.7: 
                        logger.info(f"Found {len(temp_staff_from_select)} potential staff from another select tag (Name: {potential_select.get('name', 'N/A')}, ID: {potential_select.get('id', 'N/A')}). Adding them.")
                        all_staff.extend(temp_staff_from_select)
                        unique_staff_ids.update(temp_ids_from_select)
                    else:
                        logger.debug(f"Skipping select (Name: {potential_select.get('name', 'N/A')}) as names don't look like staff.")

    # Attempt 3: Fallback to 'var tas = [...]' JavaScript array parsing
    html_content_str = str(soup) 
    match = re.search(r'(?:var\s+)?tas\s*=\s*(\[[\s\S]*?\])\s*;?', html_content_str, re.DOTALL | re.IGNORECASE)
    if match:
        tas_js_array_string = match.group(1).strip()
        try:
            tas_list_data = ast.literal_eval(tas_js_array_string)
            parsed_count = 0
            for item in tas_list_data:
                if isinstance(item, dict) and 'id' in item and 'value' in item:
                    staff_id = str(item['id'])
                    staff_name_val = item['value'].strip()
                    if staff_id.isdigit() and staff_name_val and staff_id not in unique_staff_ids:
                        all_staff.append({'id': staff_id, 'name': staff_name_val})
                        unique_staff_ids.add(staff_id)
                        parsed_count +=1
            logger.info(f"Extracted {parsed_count} staff from 'var tas' JS array (ast.literal_eval).")
        except (ValueError, SyntaxError, TypeError) as e_ast:
            logger.warning(f"ast.literal_eval failed for 'var tas' during global list extraction: {e_ast}. Trying JSON.")
            try:
                json_string = tas_js_array_string.replace("'", "\"")
                tas_list_data_json = json.loads(json_string)
                parsed_count_json = 0
                for item in tas_list_data_json:
                    if isinstance(item, dict) and 'id' in item and 'value' in item:
                        staff_id = str(item['id'])
                        staff_name_val = item['value'].strip()
                        if staff_id.isdigit() and staff_name_val and staff_id not in unique_staff_ids:
                            all_staff.append({'id': staff_id, 'name': staff_name_val})
                            unique_staff_ids.add(staff_id)
                            parsed_count_json += 1
                logger.info(f"Extracted {parsed_count_json} staff from 'var tas' using json.loads fallback.")
            except json.JSONDecodeError as e_json:
                logger.error(f"Failed to parse 'tas' JavaScript array as JSON (global list extraction): {e_json}")
        except Exception as e_other_parse:
            logger.error(f"Unexpected error parsing 'var tas' array (global list extraction): {e_other_parse}")
    else:
        logger.warning("Could not find 'var tas' JavaScript array in the page content (global list extraction).")

    if not all_staff:
        logger.error("Failed to extract any staff details from the page after all attempts.")
        return None
    
    logger.info(f"Total unique staff details extracted: {len(all_staff)}")
    return all_staff

def get_global_staff_list_and_tokens(session: requests.Session, force_refresh: bool = False) -> tuple[list[dict[str, str]] | None, dict | None]:
    """
    Fetches the global staff list (names and IDs) and fresh ASP.NET tokens.
    The staff list is cached. Tokens are always fetched fresh if staff list is from cache.
    """
    cached_staff_list = None
    if not force_refresh:
        cached_staff_list = get_from_cache(GLOBAL_STAFF_LIST_CACHE_KEY)
        if cached_staff_list:
            logger.info(f"Global staff list cache hit. Found {len(cached_staff_list)} staff members.")
            try:
                logger.debug("Fetching fresh ASP.NET tokens for cached staff list.")
                response = session.get(STAFF_SCHEDULE_URL, timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                fresh_tokens = _extract_asp_tokens(soup)
                if fresh_tokens:
                    return cached_staff_list, fresh_tokens
                else:
                    logger.error("Failed to get fresh tokens for cached staff list. Will attempt full refresh.")
            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException while fetching fresh tokens for cached staff list: {e}. Will attempt full refresh.")
            except Exception as e_token_fetch:
                logger.error(f"Unexpected error fetching fresh tokens for cached staff list: {e_token_fetch}. Will attempt full refresh.")

    logger.info("Fetching fresh global staff list and tokens (cache miss, force_refresh, or token error).")
    try:
        response = session.get(STAFF_SCHEDULE_URL, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        asp_tokens = _extract_asp_tokens(soup)
        if not asp_tokens:
            logger.error("Failed to extract ASP.NET tokens during full refresh.")
            return None, None

        staff_list = _extract_all_staff_details(soup)
        if not staff_list:
            logger.error("Failed to extract staff list during full refresh.")
            return None, asp_tokens 

        set_in_cache(GLOBAL_STAFF_LIST_CACHE_KEY, staff_list, timeout=GLOBAL_STAFF_LIST_CACHE_TIMEOUT_SECONDS)
        logger.info(f"Successfully fetched and cached {len(staff_list)} staff members.")
        return staff_list, asp_tokens

    except requests.exceptions.Timeout:
        logger.error(f"Timeout while fetching initial schedule page for global staff list.")
        return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for initial schedule page for global staff list: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Unexpected error fetching initial page data for global staff list: {e}", exc_info=True)
        return None, None

def _find_staff_id_from_list(staff_list: list[dict[str,str]], staff_name: str) -> str | None:
    """Finds staff ID from a pre-fetched list of staff details using flexible name matching."""
    if not staff_list:
        logger.warning(f"Cannot search for staff '{staff_name}': staff list is empty or None.")
        return None

    staff_name_normalized = _normalize_staff_name(staff_name)
    
    found_staff_ids = [] # Store IDs of all matches
    exact_match_id = None

    for staff_member in staff_list:
        option_name_normalized = _normalize_staff_name(staff_member['name'])
        
        # Check for exact match first
        if option_name_normalized == staff_name_normalized:
            logger.info(f"Found exact match for staff_id: {staff_member['id']} for input name: '{staff_name}' (matched with: '{staff_member['name']}').")
            exact_match_id = staff_member['id']
            break # Exact match is preferred, stop searching
        
        if _match_staff_name(staff_name_normalized, option_name_normalized):
            logger.info(f"Found potential flexible match for staff_id: {staff_member['id']} for input name: '{staff_name}' (matched with: '{staff_member['name']}').")
            found_staff_ids.append(staff_member['id'])

    if exact_match_id:
        return exact_match_id

    if not found_staff_ids:
        logger.warning(f"Staff ID for '{staff_name}' not found in the pre-fetched list of {len(staff_list)} members.")
        if staff_list and len(staff_list) < 20:
            example_names = [s['name'] for s in staff_list[:5]]
            logger.debug(f"Example names from provided list for '{staff_name}' search: {example_names}")
        elif staff_list:
             example_names = [s['name'] for s in staff_list if staff_name_normalized.split()[0] in _normalize_staff_name(s['name'])][:5]
             if example_names:
                logger.debug(f"Example names from provided list (first name part match '{staff_name_normalized.split()[0]}') for '{staff_name}' search: {example_names}")
        return None
    
    if len(found_staff_ids) > 1:
        logger.warning(f"Multiple flexible matches found for '{staff_name}': {found_staff_ids}. Returning the first one: {found_staff_ids[0]}. Consider using a more specific name.")
    
    return found_staff_ids[0]

def _get_initial_schedule_page_data(session: requests.Session, staff_name: str):
    """
    Fetches initial schedule page data: staff ID for the given name and ASP.NET tokens.
    Uses the global staff list for ID lookup.
    """
    staff_list, asp_tokens = get_global_staff_list_and_tokens(session)

    if not asp_tokens:
        logger.error(f"Failed to get ASP tokens for staff '{staff_name}'.")
        # staff_list might still be valid from cache, but tokens are crucial.
        return None, None 

    if not staff_list:
        logger.error(f"Failed to get global staff list for staff '{staff_name}'.")
        return None, asp_tokens # Return tokens if we got them, but no list

    staff_id = _find_staff_id_from_list(staff_list, staff_name)
    if not staff_id:
        logger.warning(f"Could not find ID for staff: {staff_name} in the global list.")
        # Still return tokens, as they might be useful for other operations or a page that doesn't need a specific staff ID
        return None, asp_tokens 

    return staff_id, asp_tokens

def parse_staff_schedule(html_content: str, requested_staff_ids: list[str] | None = None):
    """
    Parses the HTML content of a staff schedule page, which might contain schedules
    for multiple staff members (from a batched request) or a single staff member.

    Args:
        html_content: The HTML string to parse.
        requested_staff_ids: Optional. A list of staff IDs that were requested.
                             If provided, the parser can be more targeted.
                             However, the primary method will be to discover staff IDs
                             from data-staff-id attributes in the slots.

    Returns:
        A dictionary where keys are staff IDs (str) and values are their parsed schedules.
        Each schedule is a dictionary: {'day': {'slot_index': [slot_details_list]}}.
        Returns an empty dictionary if no valid schedule data is found.
    """
    all_staff_schedules = {}

    try:
        soup = BeautifulSoup(html_content, 'lxml')

        schedule_table = soup.find('table', {'id': 'ContentPlaceHolderright_ContentPlaceHoldercontent_schedule'})
        if not schedule_table:
            logger.warning("Schedule table with id 'ContentPlaceHolderright_ContentPlaceHoldercontent_schedule' not found.")
            return {}

        days = ["Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        
        rows = schedule_table.find_all('tr')
        if not rows or len(rows) < 2:
            logger.warning("Schedule table has no data rows or is malformed.")
            return {}

        for i, row in enumerate(rows[1:]):
            day_index = i
            if day_index >= len(days):
                logger.warning(f"Row index {i} exceeds known days list. Skipping.")
                continue
            
            current_day_name = days[day_index]
            
            cells = row.find_all(['td', 'th'])
            if not cells or len(cells) < 2:
                logger.debug(f"Skipping row for day {current_day_name}, not enough cells.")
                continue

            for slot_index, cell in enumerate(cells[1:]):
                if slot_index >= len(SCHEDULE_SLOT_TIMINGS):
                    logger.warning(f"Slot index {slot_index} for day {current_day_name} exceeds known slot timings. Skipping cell.")
                    continue

                slot_divs = cell.find_all('div', class_='slot')
                if not slot_divs:
                    continue

                for slot_div in slot_divs:
                    slot_staff_id = slot_div.get('data-staff-id')
                    if not slot_staff_id:
                        logger.debug(f"Slot on {current_day_name} at slot index {slot_index} is missing 'data-staff-id'. Skipping.")
                        continue
                    
                    if slot_staff_id not in all_staff_schedules:
                        all_staff_schedules[slot_staff_id] = {day: {slot_idx: [] for slot_idx in SCHEDULE_SLOT_TIMINGS} for day in days}
                    
                    group_tag = slot_div.find('dt', string='Group')
                    location_tag = slot_div.find('dt', string='Location')
                    staff_name_tag = slot_div.find('dt', string='Staff')

                    group = group_tag.find_next_sibling('dd').text.strip() if group_tag and group_tag.find_next_sibling('dd') else "N/A"
                    location = location_tag.find_next_sibling('dd').text.strip() if location_tag and location_tag.find_next_sibling('dd') else "N/A"
                    
                    slot_staff_name = "N/A"
                    if staff_name_tag and staff_name_tag.find_next_sibling('dd'):
                        slot_staff_name_dd = staff_name_tag.find_next_sibling('dd')
                        slot_staff_name = slot_staff_name_dd.get_text(strip=True)

                    course_id_from_group_class = "N/A"
                    if group_tag and group_tag.find_next_sibling('dd'):
                        group_dd_tag = group_tag.find_next_sibling('dd')
                        group_class = group_dd_tag.get('class', [])
                        if group_class:
                            course_class_match = re.search(r'course-(\d+)', group_class[0])
                            if course_class_match:
                                course_id_from_group_class = course_class_match.group(1)
                    
                    slot_details = {
                        'time': SCHEDULE_SLOT_TIMINGS.get(slot_index, f"Unknown Slot Index {slot_index}"),
                        'group': group,
                        'location': location,
                        'staff_name_in_slot': slot_staff_name,
                        'course_id_from_slot': course_id_from_group_class,
                        'original_staff_id': slot_staff_id
                    }
                    
                    if current_day_name not in all_staff_schedules[slot_staff_id]:
                         all_staff_schedules[slot_staff_id][current_day_name] = {slot_idx: [] for slot_idx in SCHEDULE_SLOT_TIMINGS}
                    if slot_index not in all_staff_schedules[slot_staff_id][current_day_name]:
                         all_staff_schedules[slot_staff_id][current_day_name][slot_index] = []
                        
                    all_staff_schedules[slot_staff_id][current_day_name][slot_index].append(slot_details)

        final_schedules = {}
        for staff_id, schedule in all_staff_schedules.items():
            cleaned_schedule = {}
            for day, day_slots in schedule.items():
                active_slots_for_day = {slot_idx: entries for slot_idx, entries in day_slots.items() if entries}
                if active_slots_for_day:
                    cleaned_schedule[day] = active_slots_for_day
            if cleaned_schedule:
                final_schedules[staff_id] = cleaned_schedule
        
        if not final_schedules:
            logger.info("Parsing complete, but no schedule data found for any staff ID in the provided HTML.")
        else:
            logger.info(f"Successfully parsed schedules for staff IDs: {list(final_schedules.keys())}")
            
        return final_schedules

    except Exception as e:
        logger.error(f"Error parsing staff schedule HTML: {e}", exc_info=True)
        return {}

def scrape_staff_schedule(session: requests.Session, staff_name_or_id: str, asp_tokens_override: dict | None = None, staff_id_override: str | None = None):
    """
    Scrapes the schedule for a single staff member.
    Can accept either staff name (to lookup ID) or staff ID directly.
    Allows overriding ASP tokens and staff ID for more direct control, useful for batching later.

    Args:
        session: Authenticated requests.Session object.
        staff_name_or_id: The staff member's full name (str) or their ID (str).
        asp_tokens_override: Optional dictionary with ASP tokens to use directly.
        staff_id_override: Optional staff ID to use directly.

    Returns:
        A dictionary representing the staff member's schedule, or None if scraping fails.
        The schedule is keyed by staff ID if parsing was successful for multiple staff (even if one was requested).
        If a single staff schedule is definitively parsed for the requested staff, it might return that directly.
        For consistency with batch operations, it's better if this also returns {staff_id: schedule}.
    """
    logger.info(f"Starting schedule scrape for: {staff_name_or_id}")
    
    actual_staff_id = staff_id_override
    asp_tokens_to_use = asp_tokens_override

    if not actual_staff_id and staff_name_or_id.isdigit():
        logger.info(f"Input '{staff_name_or_id}' is an ID. Using it directly.")
        actual_staff_id = staff_name_or_id
    
    if not actual_staff_id or not asp_tokens_to_use:
        # Need to fetch ID and/or tokens if not fully overridden
        # If staff_name_or_id is an ID but tokens are missing, we can't use _get_initial_schedule_page_data
        # which expects a name for ID lookup.
        # This scenario (ID provided but tokens missing) should ideally be handled by the caller
        # or we fetch tokens using a generic GET. For now, assume _get_initial_schedule_page_data
        # is called if overrides are insufficient for a POST.
        
        # If only staff_id is overridden, but not tokens, we still need tokens.
        # If staff_name_or_id is a name, we need ID and tokens.
        if not asp_tokens_to_use: # Always true if asp_tokens_override is None
            fetched_id, fetched_tokens = _get_initial_schedule_page_data(session, staff_name_or_id if not actual_staff_id else staff_name_or_id) # Pass name if ID isn't known yet
            if not fetched_tokens:
                logger.error(f"Failed to get ASP tokens for {staff_name_or_id}.")
                return None # Cannot proceed without tokens
            asp_tokens_to_use = fetched_tokens
            if not actual_staff_id: # If ID wasn't overridden and wasn't an input ID
                if not fetched_id:
                    logger.error(f"Failed to find staff_id for {staff_name_or_id}.")
                    return None # Cannot proceed without ID
                actual_staff_id = fetched_id
        elif not actual_staff_id : # Tokens overridden, but ID is not. Input was a name.
             # This case is a bit tricky. If tokens are overridden, _get_initial_schedule_page_data
             # would re-fetch them. We only need the ID.
             # We assume if tokens are overridden, the ID should also be, or the input is an ID.
             # For simplicity, if staff_name_or_id is a name and tokens are overridden, ID must also be.
             logger.warning(f"Tokens overridden for '{staff_name_or_id}', but ID is not. This might lead to issues if name lookup is needed.")
             # Attempt to get ID if name_or_id is a name string.
             if not staff_name_or_id.isdigit():
                staff_list_for_id_lookup, _ = get_global_staff_list_and_tokens(session, force_refresh=False) # Don't force refresh if just for ID
                if staff_list_for_id_lookup:
                    actual_staff_id = _find_staff_id_from_list(staff_list_for_id_lookup, staff_name_or_id)
                if not actual_staff_id:
                    logger.error(f"Failed to find staff_id for {staff_name_or_id} even with token override scenario.")
                    return None


    if not actual_staff_id or not asp_tokens_to_use:
        logger.error(f"Critical error: Missing staff ID or ASP tokens for {staff_name_or_id} before POST.")
        return None

    payload = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': asp_tokens_to_use.get('viewstate'),
        '__VIEWSTATEGENERATOR': asp_tokens_to_use.get('viewstate_generator'),
        '__EVENTVALIDATION': asp_tokens_to_use.get('event_validation'),
        'ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$DDLSession': '0', # Default value
        'ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$B_ShowSchedule': 'Show Schedule',
        'ta[]': actual_staff_id # For single staff request
    }
    
    # Remove keys with None values, especially for tokens that might have failed extraction partially
    payload = {k: v for k, v in payload.items() if v is not None}
    if len(payload.keys()) < 7 : # Check if essential token fields are missing
        logger.error(f"Payload for {actual_staff_id} is missing essential ASP token fields after None filtering. Aborting POST.")
        logger.debug(f"Payload content: {payload}")
        return None


    logger.info(f"Making POST request for staff ID: {actual_staff_id}")
    try:
        response = session.post(STAFF_SCHEDULE_URL, data=payload, timeout=20)
        response.raise_for_status()
        
        # The new parse_staff_schedule returns a dict {staff_id: schedule_data}
        parsed_schedules_dict = parse_staff_schedule(response.text, requested_staff_ids=[actual_staff_id])

        if not parsed_schedules_dict:
            logger.warning(f"Parsing returned empty dict for staff ID {actual_staff_id} ({staff_name_or_id}). HTML might be empty or malformed.")
            # Consider saving response.text for debugging here if this happens often
            # with open(f"debug_staff_{actual_staff_id}_empty_parse.html", "w", encoding="utf-8") as f:
            #    f.write(response.text)
            return None
        
        # Even if we requested one staff, the parser might find others if the page structure is shared.
        # For this function, we are interested in the schedule of the *requested* staff ID.
        if actual_staff_id in parsed_schedules_dict:
            logger.info(f"Successfully scraped and parsed schedule for staff ID {actual_staff_id} ({staff_name_or_id}).")
            # Return in the format {staff_id: schedule} for consistency with batching
            return {actual_staff_id: parsed_schedules_dict[actual_staff_id]}
        else:
            # This case means the parser worked, found some schedules, but not for the one specifically requested.
            # This might happen if the requested_staff_ids argument to parse_staff_schedule was not used to filter,
            # or if the staff ID was genuinely not in the slots of the returned page.
            logger.warning(f"Schedule for requested staff ID {actual_staff_id} ({staff_name_or_id}) not found in parsed results, though other IDs might be present: {list(parsed_schedules_dict.keys())}")
            # If HTML/2.html is an example of a "batch" response, it might always be keyed by the *actual* staff_id in the slot
            # If any schedule was found, and we only requested one ID, it's likely the one we want or an error.
            # For now, if the primary ID is not there, consider it a failure for this specific request.
            # If the goal was to get *any* schedule from the page, the logic would be different.
            return None

    except requests.exceptions.Timeout:
        logger.error(f"Timeout during POST request for staff ID {actual_staff_id} ({staff_name_or_id}).")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during POST for staff ID {actual_staff_id} ({staff_name_or_id}): {e.response.status_code} - {e.response.reason}")
        # Log more details for certain errors
        if e.response.status_code == 500 or e.response.status_code == 403:
             logger.error(f"Response content for HTTP {e.response.status_code} error: {e.response.text[:500]}") # Log first 500 chars
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for staff ID {actual_staff_id} ({staff_name_or_id}): {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during scraping schedule for staff ID {actual_staff_id} ({staff_name_or_id}): {e}", exc_info=True)
        return None

def scrape_batch_staff_schedules(session: requests.Session, staff_ids: list[str], asp_tokens: dict) -> dict[str, dict]:
    """
    Scrapes schedules for a batch of staff IDs using a single POST request.

    Args:
        session: Authenticated requests.Session object.
        staff_ids: A list of staff IDs (strings) to scrape.
        asp_tokens: A dictionary containing the necessary ASP.NET tokens 
                    ('viewstate', 'viewstate_generator', 'event_validation').

    Returns:
        A dictionary where keys are staff IDs (str) and values are their parsed schedules.
        Each schedule is a dictionary: {'day': {'slot_index': [slot_details_list]}}.
        Returns an empty dictionary if the request or parsing fails comprehensively.
    """
    if not staff_ids:
        logger.warning("scrape_batch_staff_schedules called with an empty list of staff IDs.")
        return {}

    if not asp_tokens or not all(asp_tokens.get(k) for k in ['viewstate', 'viewstate_generator', 'event_validation']):
        logger.error("scrape_batch_staff_schedules called with missing or incomplete ASP tokens.")
        return {}

    logger.info(f"Starting batch schedule scrape for {len(staff_ids)} staff IDs: {staff_ids[:5]}...") # Log first 5 IDs for brevity

    payload = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': asp_tokens.get('viewstate'),
        '__VIEWSTATEGENERATOR': asp_tokens.get('viewstate_generator'),
        '__EVENTVALIDATION': asp_tokens.get('event_validation'),
        'ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$DDLSession': '0', # Default value
        'ctl00$ctl00$ContentPlaceHolderright$ContentPlaceHoldercontent$B_ShowSchedule': 'Show Schedule',
        'ta[]': staff_ids # Key part: list of staff IDs
    }
    
    payload = {k: v for k, v in payload.items() if v is not None}

    logger.info(f"Making POST request for {len(staff_ids)} staff IDs.")
    try:
        response = session.post(STAFF_SCHEDULE_URL, data=payload, timeout=30) # Slightly longer timeout for batch
        response.raise_for_status()
        
        parsed_schedules_dict = parse_staff_schedule(response.text, requested_staff_ids=staff_ids)

        if not parsed_schedules_dict:
            logger.warning(f"Batch parsing returned empty dict for staff IDs {staff_ids[:5]}...")
            # Example debug: 
            # if len(staff_ids) < 5: # Only for small batches to avoid huge logs/files
            #     with open(f"debug_batch_staff_{'_'.join(staff_ids)}_empty_parse.html", "w", encoding="utf-8") as f:
            #        f.write(response.text)
            return {}
        
        final_batch_schedules = {sid: sched for sid, sched in parsed_schedules_dict.items() if sid in staff_ids}
        
        found_ids_count = len(final_batch_schedules)
        if found_ids_count < len(staff_ids):
            missing_ids = [sid for sid in staff_ids if sid not in final_batch_schedules]
            logger.warning(f"Batch scrape for {len(staff_ids)} IDs: Did not find schedules for {len(missing_ids)} IDs (e.g., {missing_ids[:5]}...). Found {found_ids_count}.")
        
        logger.info(f"Successfully scraped and parsed schedules for {found_ids_count} staff IDs from batch request of {len(staff_ids)}.")
        return final_batch_schedules

    except requests.exceptions.Timeout:
        logger.error(f"Timeout during batch POST request for {len(staff_ids)} staff IDs: {staff_ids[:5]}...")
        return {}
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during batch POST for {len(staff_ids)} staff IDs: {e.response.status_code} - {e.response.reason}")
        if e.response.status_code == 500 or e.response.status_code == 403:
             logger.error(f"Response content for HTTP {e.response.status_code} error (batch): {e.response.text[:500]}")
        return {}
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for batch POST for {len(staff_ids)} staff IDs: {e}")
        return {}
    except Exception as e:
        logger.error(f"An unexpected error occurred during batch scraping for {len(staff_ids)} staff IDs: {e}", exc_info=True)
        return {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Starting direct test of staff schedule scraper with new caching logic...")

    # Test 1: Parse a known HTML response (remains the same)
    logger.info("\n--- Test 1: Parsing pre-saved HTML ---")
    try:
        # Adjust path if your test HTML is elsewhere or named differently
        with open("HTML/response.html", "r", encoding="utf-8") as f: 
            sample_html_content = f.read()
        
        parsed_data = parse_staff_schedule(sample_html_content)
        if "error" in parsed_data:
            logger.error(f"Error parsing sample HTML: {parsed_data['error']}")
        else:
            logger.info(f"Parsed Staff Name from HTML: {parsed_data.get('staff_name')}")
            logger.info(f"Parsed Schedule Days from HTML: {len(parsed_data.get('schedule_days', []))}")
            if parsed_data.get('schedule_days'):
                first_day_slots = parsed_data.get('schedule_days', [{}])[0].get('slots', [])
                logger.info(f"First day slots example (first 2 if any): {first_day_slots[:2]}")

    except FileNotFoundError:
        logger.error("../../HTML/response.html not found (relative to an assumed execution path). Adjust path if needed. Skipping Test 1.")
    except Exception as e:
        logger.error(f"Error during Test 1 (parsing sample HTML): {e}", exc_info=True)

    # Test 2: Live tests for new global list fetching and caching
    logger.info("\n--- Test 2: Live fetching of global staff list and tokens ---")
    
    # Simulating a session for standalone testing:
    test_session = requests.Session() 
    test_session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        # Add other headers if the GUC portal requires them for the initial GET
    })
    
    # Note: For these tests to properly verify caching, you need a functional cache setup.
    # If `utils.cache` uses an in-memory cache for testing, it will work per run.
    # If it uses Redis/Flask-Caching, ensure it's configured and accessible.

    # First call: should fetch from web and cache the list
    logger.info("--- Test 2a: First call to get_global_staff_list_and_tokens (expect fetch & cache) ---")
    staff_list_live, tokens_live = get_global_staff_list_and_tokens(test_session)
    
    if staff_list_live and tokens_live:
        logger.info(f"Successfully fetched global staff list ({len(staff_list_live)} members) and tokens.")
        logger.info(f"First 3 staff examples: {staff_list_live[:3]}")
        # It might be too verbose to log all tokens here.

        # Test _find_staff_id_from_list with a name from the fetched list
        if staff_list_live:
            test_search_name = staff_list_live[0]['name'] # Use the first staff member's name
            logger.info(f"--- Test 2b: Finding ID for '{test_search_name}' in the fetched list ---")
            found_id = _find_staff_id_from_list(staff_list_live, test_search_name)
            if found_id:
                logger.info(f"Found ID '{found_id}' for '{test_search_name}'.")
            else:
                logger.error(f"Could not find ID for '{test_search_name}' in the list.")
            
            # Test with a slightly different/normalized name if possible
            if len(staff_list_live) > 1: # Need at least two names for a different test
                original_name_for_flex = staff_list_live[1]['name']
                parts = original_name_for_flex.split()
                if len(parts) >= 2:
                    flex_test_name = f"{parts[0].lower()} {_normalize_staff_name(parts[-1])}" # e.g. "john DOE" -> "john doe"
                    logger.info(f"--- Test 2c: Finding ID for flex name '{flex_test_name}' (original: '{original_name_for_flex}') ---")
                    found_id_flex = _find_staff_id_from_list(staff_list_live, flex_test_name)
                    if found_id_flex:
                        logger.info(f"Found ID '{found_id_flex}' for flex name '{flex_test_name}'.")
                    else:
                        logger.error(f"Could not find ID for flex name '{flex_test_name}'.")

        # Second call: should primarily use cached list, but get fresh tokens
        logger.info("--- Test 2d: Second call to get_global_staff_list_and_tokens (expect list from cache, fresh tokens) ---")
        staff_list_cached, tokens_cached = get_global_staff_list_and_tokens(test_session)
        if staff_list_cached and tokens_cached:
            logger.info(f"Second call successful. List size: {len(staff_list_cached)} (should be same as first call if cache worked). Tokens fetched.")
            if tokens_live and tokens_cached and tokens_live.get('viewstate') != tokens_cached.get('viewstate'):
                logger.info("ViewState tokens are different between first and second call, as expected for fresh tokens.")
            elif tokens_live and tokens_cached and tokens_live.get('viewstate') == tokens_cached.get('viewstate'):
                logger.warning("ViewState tokens are THE SAME. This might be OK if page is static for a bit, or cache for tokens isn't bypassed as intended. Investigate if this is an issue.")
            else:
                logger.warning("Could not compare tokens (one or both missing).")
        else:
            logger.error("Failed on second call to get_global_staff_list_and_tokens.")

        # Third call: force refresh the list
        logger.info("--- Test 2e: Third call with force_refresh=True (expect full fetch of list and tokens) ---")
        staff_list_forced, tokens_forced = get_global_staff_list_and_tokens(test_session, force_refresh=True)
        if staff_list_forced and tokens_forced:
            logger.info(f"Force-refresh successful. List size: {len(staff_list_forced)}. Tokens fetched.")
        else:
            logger.error("Failed on force-refreshed call to get_global_staff_list_and_tokens.")

    else:
        logger.error("Initial fetch of global staff list and/or tokens failed in Test 2. Skipping further live sub-tests.")

    # Test 3: Full scrape_staff_schedule (requires an authenticated session and a known staff name)
    # This part is highly dependent on having a live, authenticated session.
    # You would need to integrate with `scraping.authenticate.authenticate_user_session`
    # and provide actual credentials for a real test.

    # logger.info("\n--- Test 3: Full scrape_staff_schedule (LIVE - requires GUC auth session) ---")
    # from scraping.authenticate import authenticate_user_session # Ensure this is available and configured
    # GUC_USERNAME = "your_username"  # Replace with actual credentials if testing live
    # GUC_PASSWORD = "your_password"
    # authenticated_session = authenticate_user_session(GUC_USERNAME, GUC_PASSWORD)
    # 
    # if authenticated_session and staff_list_live: # Re-use staff_list_live from Test 2
    #     if staff_list_live: # Check again to be sure
    #         staff_to_scrape_name = staff_list_live[0]['name'] # Pick the first staff from the list
    #         logger.info(f"Attempting live scrape for staff: '{staff_to_scrape_name}' using authenticated session.")
    #         
    #         schedule_result = scrape_staff_schedule(authenticated_session, staff_to_scrape_name)
    #         
    #         if isinstance(schedule_result, dict) and "error" in schedule_result:
    #             logger.error(f"Live scrape error for '{staff_to_scrape_name}': {schedule_result['error']}")
    #         elif isinstance(schedule_result, dict):
    #             logger.info(f"Live scrape success for '{staff_to_scrape_name}'. Staff Name in result: {schedule_result.get('staff_name')}")
    #             logger.info(f"Schedule days found: {len(schedule_result.get('schedule_days', []))}")
    #         else:
    #             logger.error(f"Live scrape for '{staff_to_scrape_name}' returned unexpected data type: {type(schedule_result)}")
    #     else:
    #         logger.warning("Cannot run Test 3: Staff list was not available from Test 2.")
    # else:
    #     logger.warning("Skipping Test 3 (live scrape_staff_schedule): Authenticated session not available or staff list not fetched.")
    #     logger.warning("To run Test 3, set up credentials and ensure authenticate_user_session works.")

    logger.info("\nDirect test of staff schedule scraper finished.")
