# utils/notifications_utils.py
import logging
# from api.notifications import add_notification, MAX_NOTIFICATIONS # REMOVE THIS LINE
from utils.cache import get_from_cache, generate_cache_key, set_in_cache, redis_client # Added redis_client
import re  # Keep regex import
from config import config # Import config for timeout values
import json # Added for add_notification

logger = logging.getLogger(__name__)

MAX_NOTIFICATIONS = 10  # Define the constant here, adjust value as needed
# Cache key prefix for user-specific notification message lists
USER_NOTIFICATIONS_CACHE_PREFIX = "user_notifications" 

# --- Notification Adding Logic (similar to what was in api/notifications.py) ---
def add_notification(username: str, notification_type: str, description: str) -> bool:
    """
    Adds a notification message to a user-specific list in Redis.
    This list is then fetched by the API to be included in the /guc_data endpoint.
    It does NOT interact with the old direct push notification system.
    """
    if not username or not notification_type or not description:
        logger.warning("Attempted to add user notification with missing info.")
        return False
    try:
        # Key for the list of [type, description] pairs for this user
        # This key is different from the one used by the old api/notifications.py push system
        cache_key = f"{USER_NOTIFICATIONS_CACHE_PREFIX}_{username}"
        
        # Prepare the new notification message as a string
        # Based on the beautify function, it expects a list of strings like "[Category] Course - Item: Details"
        # We will prepend a category based on notification_type if it makes sense,
        # or just use description if it's already well-formed.
        # For now, let's assume description is already in the desired format from compare functions.
        
        # Example of how refresh_cache.py structures it for user_updates_batch:
        # user_updates_batch = {
        #     "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        #     "messages": collected_notifications_for_user  <-- This is a list of strings
        # }
        # Here, 'description' will be one such string.

        # We are storing a list of batches, and each batch has a list of messages.
        # For simplicity here, let's match what refresh_cache.py does for collecting messages
        # and assume this function's purpose is to contribute one message string.
        # The actual batching and MAX_NOTIFICATIONS_LIMIT for batches is handled in refresh_cache.py.
        # This function's success just means the message was valid to be part of a batch.
        
        logger.debug(f"Notification valid for {username}: [{notification_type}] {description}")
        return True # Indicate success for refresh_cache.py to collect this message

    except Exception as e:
        logger.error(
            f"General error preparing notification for {username}: {e}", exc_info=True
        )
        return False

# --- Constants for Grade Comparison ---
_PLACEHOLDER_GRADES = ["na", "n/a", "-", "undetermined", "", "/"] # "" for empty, "/" for "/10" like

# --- Helper Functions for Grade Comparison ---

def _is_placeholder_grade(grade_str: str | None) -> bool:
    """Checks if a grade string is a placeholder or effectively empty."""
    if grade_str is None:
        return True
    cleaned = _clean_string(grade_str) # Example: "  / 10  " -> "/ 10"; "  /10" -> "/10"; "10 / 10" -> "10 / 10"; "-" -> "-"
    if not cleaned: return True  # Catches ""
    if cleaned.lower() in _PLACEHOLDER_GRADES: # Catches "na", "n/a", "-", "undetermined", and exact "/"
        return True

    # New logic: Check for patterns like "/10", "/ 10", "/ 10.5" etc.
    # These indicate the 'actual grade' part is missing before the slash.
    if cleaned.startswith("/"):
        # Ensure it's not just "/" (already caught) and there's something after the slash.
        part_after_slash = cleaned[1:].strip() # e.g., "/ 10" -> "10"; "/10" -> "10"; "/ 10.5" -> "10.5"
        if part_after_slash: # Make sure there is a value after the slash
            try:
                # Check if the part after the slash represents a number (the 'out_of' part)
                float(part_after_slash.replace(" ", "")) # "10" -> 10.0; "10.5" -> 10.5. Throws error if not numeric.
                return True # It's a placeholder like "/ X" (e.g., /10, / 20)
            except ValueError:
                # The part after the slash is not purely numeric, so it's not a simple "/ X" placeholder.
                # e.g., "/foo" or if _clean_string somehow allowed "/ 10 / 20"
                pass 
    return False

def _get_grade_display_name_from_details(item_details: dict, item_dict_key_from_scraper: str) -> str:
    """
    Determines the best display name for a grade item, using its scraped names or falling back to key parts.
    """
    raw_qa = item_details.get("Quiz/Assignment", "")
    raw_en = item_details.get("Element Name", "")

    # Use stripped raw values for display if they have content
    display_qa = raw_qa.strip()
    display_en = raw_en.strip()

    if display_qa: # If Quiz/Assignment has meaningful content
        return display_qa
    elif display_en: # Else if Element Name has meaningful content
        return display_en
    else:
        # Fallback if both primary names are empty/whitespace.
        # Try to derive something from the scraper's key.
        # Scraper key is like "cleaned_name1::cleaned_name2::occurrence"
        parts = item_dict_key_from_scraper.split("::")
        # Prefer the parts that are not placeholders used by the scraper's key generation
        if len(parts) > 0 and parts[0] not in ["NO_QUIZ_ASSIGN_NAME", "_ANONYMOUS_ROW_"]:
            return parts[0] 
        elif len(parts) > 1 and parts[1] != "NO_ELEMENT_NAME":
            return parts[1]
        return f"Item ({item_dict_key_from_scraper})" # Ultimate fallback, show the key for context

# --- Main Grade Comparison Logic (Refactored for Item Key-Based Matching) ---
def compare_grades(username: str, old_grades_data_obj: dict, new_grades_data_obj: dict) -> list:
    """
    Compares old and new detailed grades based on a stable item key derived from scraper output.
    Notifications are triggered for actual grade value changes, new (non-placeholder) grades,
    or removed (previously non-placeholder) grades.
    Input objects are the full grade data objects, from which 'detailed_grades' will be extracted.
    """
    notifications = []

    # Extract the 'detailed_grades' part from the passed objects
    old_detailed_part = old_grades_data_obj.get("detailed_grades", {})
    new_detailed_part = new_grades_data_obj.get("detailed_grades", {})

    # Initial check if the detailed_grades parts are not dicts or if one is missing
    if not isinstance(old_detailed_part, dict) or not isinstance(new_detailed_part, dict):
        if isinstance(new_detailed_part, dict) and not isinstance(old_detailed_part, dict):
             for course_name_raw, new_course_items_dict in new_detailed_part.items(): # Iterate over new_detailed_part
                if not isinstance(new_course_items_dict, dict): continue
                cleaned_course_name_for_new = _clean_string(course_name_raw)
                if not cleaned_course_name_for_new: continue
                for item_dict_key_new, new_item_details in new_course_items_dict.items():
                    if not isinstance(new_item_details, dict): continue
                    new_grade_raw = new_item_details.get("grade")
                    if not _is_placeholder_grade(new_grade_raw):
                        current_item_display_name_new = _get_grade_display_name_from_details(new_item_details, item_dict_key_new)
                        new_grade_cleaned_for_notif = _clean_string(new_grade_raw)
                        notif_desc = f"{cleaned_course_name_for_new} - {current_item_display_name_new}: {new_grade_cleaned_for_notif}"
                        notifications.append(["New grade", notif_desc])
        return notifications # Return early if fundamental structure is wrong for comparison

    # 1. Build map of old detailed grades from old_detailed_part
    old_grades_map_by_item_key = {}
    for course_name_raw, old_course_items_dict in old_detailed_part.items(): # Use old_detailed_part
        if not isinstance(old_course_items_dict, dict): continue
        cleaned_course_name_for_map = _clean_string(course_name_raw)
        if not cleaned_course_name_for_map: continue
        for item_dict_key_old, old_item_details in old_course_items_dict.items():
            if not isinstance(old_item_details, dict): continue
            map_key = (cleaned_course_name_for_map, item_dict_key_old) 
            old_grade_raw = old_item_details.get("grade")
            if _is_placeholder_grade(old_grade_raw): continue
            old_grade_cleaned = _clean_string(old_grade_raw)
            item_display_name_old = _get_grade_display_name_from_details(old_item_details, item_dict_key_old)
            old_grades_map_by_item_key[map_key] = (old_grade_cleaned, item_display_name_old)

    # 2. Iterate through new detailed grades from new_detailed_part, comparing with old map by item key
    processed_new_item_keys = set()
    for course_name_raw, new_course_items_dict in new_detailed_part.items(): # Use new_detailed_part
        if not isinstance(new_course_items_dict, dict): continue
        cleaned_course_name_for_new = _clean_string(course_name_raw)
        if not cleaned_course_name_for_new: continue
        for item_dict_key_new, new_item_details in new_course_items_dict.items():
            if not isinstance(new_item_details, dict): continue
            current_item_key = (cleaned_course_name_for_new, item_dict_key_new)
            processed_new_item_keys.add(current_item_key)
            new_grade_raw = new_item_details.get("grade")
            new_grade_cleaned_for_notif = _clean_string(new_grade_raw)
            current_item_display_name_new = _get_grade_display_name_from_details(new_item_details, item_dict_key_new)
            is_new_grade_placeholder = _is_placeholder_grade(new_grade_raw)

            # DEBUG LOGGING FOR SPECIFIC ITEMS
            if cleaned_course_name_for_new == "Engineering 2nd Semester - ENGD301 Engineering Drawing & Design":
                if item_dict_key_new == "Tutorial 9::Question1::0" or item_dict_key_new == "Quiz 2::Question1::0":
                    logger.debug(f"DEBUG_GRADES User: {username}, Course: {cleaned_course_name_for_new}, ItemKey: {item_dict_key_new}")
                    logger.debug(f"  NewRaw: '{new_grade_raw}', NewCleaned: '{new_grade_cleaned_for_notif}', IsNewPlaceholder: {is_new_grade_placeholder}")
                    if current_item_key in old_grades_map_by_item_key:
                        old_grade_val, old_disp_name = old_grades_map_by_item_key[current_item_key]
                        logger.debug(f"  OldMappedGrade: '{old_grade_val}', OldDispName: '{old_disp_name}'")
                    else:
                        logger.debug("  Item NOT IN old_grades_map_by_item_key")
            # END DEBUG LOGGING

            if current_item_key in old_grades_map_by_item_key:
                old_grade_from_map, _old_item_display_name = old_grades_map_by_item_key[current_item_key]
                if is_new_grade_placeholder:
                    notif_desc = f"{cleaned_course_name_for_new} - {current_item_display_name_new}: (grade removed/cleared, was {old_grade_from_map})"
                    notifications.append(["Updated grade", notif_desc])
                elif new_grade_cleaned_for_notif != old_grade_from_map:
                    notif_desc = f"{cleaned_course_name_for_new} - {current_item_display_name_new}: {new_grade_cleaned_for_notif} (was {old_grade_from_map})"
                    notifications.append(["Updated grade", notif_desc])
            else: 
                if not is_new_grade_placeholder:
                    notif_desc = f"{cleaned_course_name_for_new} - {current_item_display_name_new}: {new_grade_cleaned_for_notif}"
                    notifications.append(["New grade", notif_desc])
    
    # 3. Check for grades that were in old_grades_map_by_item_key but not in processed_new_item_keys
    for old_item_key, (old_grade_value, old_item_display_name) in old_grades_map_by_item_key.items():
        if old_item_key not in processed_new_item_keys:
            course_name_of_removed = old_item_key[0]
            # DEBUG LOGGING FOR REMOVED ITEMS
            if course_name_of_removed == "Engineering 2nd Semester - ENGD301 Engineering Drawing & Design":
                if old_item_key[1] == "Tutorial 9::Question1::0" or old_item_key[1] == "Quiz 2::Question1::0":
                    logger.debug(f"DEBUG_GRADES User: {username}, Course: {course_name_of_removed}, ItemKey: {old_item_key[1]} was in OLD map but NOT PROCESSED IN NEW.")
                    logger.debug(f"  OldValue: '{old_grade_value}', OldDispName: '{old_item_display_name}'")
            # END DEBUG LOGGING
            notif_desc = f"{course_name_of_removed} - {old_item_display_name}: (grade item removed, was {old_grade_value})"
            notifications.append(["Updated grade", notif_desc])
       
    return notifications


# --- Keep _clean_string helper ---
def _clean_string(text: str) -> str:
    """Helper to normalize strings for comparison."""
    if not isinstance(text, str):
        return ""
    # Collapse whitespace and strip leading/trailing
    return " ".join(text.strip().split())


# --- MODIFIED _generate_grade_item_key helper ---
def _generate_grade_item_key(course_name: str, grade_item_dict_key: str) -> tuple | None:
    """
    Generates a unique key tuple for a grade item using its course name and its dictionary key.
    Returns None if essential info is missing.
    Key: (Cleaned Course Name, Grade Item Dictionary Key)
    """
    cleaned_course_name = _clean_string(course_name)
    # Ensure grade_item_dict_key is a string and not empty
    cleaned_dict_key = str(grade_item_dict_key).strip()

    if not cleaned_course_name or not cleaned_dict_key:
        return None

    return (cleaned_course_name, cleaned_dict_key)


def _generate_attendance_slot_key(course_name: str, session_info: dict) -> tuple | None:
    """
    Generates a unique key for an attendance slot (course and session description).
    Returns None if essential info is missing.
    Key: (Cleaned Course Name, Cleaned Session Description)
    """
    session_description = session_info.get("session", "").strip()
    if not session_description:
        return None

    cleaned_course = _clean_string(course_name) # course_name is the raw key from the dict
    cleaned_session_desc = _clean_string(session_description)

    if not cleaned_course or not cleaned_session_desc:
        return None
    
    return (cleaned_course, cleaned_session_desc)


# --- REVISED compare_attendance ---
def compare_attendance(username, old_data, new_data):
    """
    Compare old and new attendance data to detect changes.
    Uses a stable key (course, session_description) to compare status.
    """
    notifications_added_this_run = []
    logger.debug(f"Starting attendance comparison for {username}")

    if not isinstance(old_data, dict) or not isinstance(new_data, dict):
        logger.warning(f"Skipping attendance: invalid data format (old: {type(old_data)}, new: {type(new_data)}) for {username}")
        return notifications_added_this_run

    descriptions_added_this_session = set()

    old_attendance_raw = old_data.get("attendance", {})
    new_attendance_raw = new_data.get("attendance", {})

    if not isinstance(old_attendance_raw, dict) or not isinstance(new_attendance_raw, dict):
        logger.warning(f"Attendance data structure incorrect for {username}. Skipping.")
        return notifications_added_this_run

    old_attendance_map = {}
    for course_name_from_dict_key, course_sessions in old_attendance_raw.items():
        if not isinstance(course_sessions, list): continue
        # Note: course_name_from_dict_key is used to generate the slot_key, which itself will clean it.
        for session_details in course_sessions:
            if not isinstance(session_details, dict): continue
            slot_key = _generate_attendance_slot_key(course_name_from_dict_key, session_details)
            status = _clean_string(session_details.get("status", ""))
            if slot_key and status:
                old_attendance_map[slot_key] = status
    
    logger.debug(f"Built old_attendance_map with {len(old_attendance_map)} entries for {username}.")

    for course_name_from_dict_key, course_sessions in new_attendance_raw.items():
        if not isinstance(course_sessions, list): continue
        for session_details in course_sessions:
            if not isinstance(session_details, dict): continue
            
            slot_key = _generate_attendance_slot_key(course_name_from_dict_key, session_details)
            if not slot_key: continue

            new_status = _clean_string(session_details.get("status", ""))
            if not new_status: continue

            course_display_name, session_display_name = slot_key
            old_status = old_attendance_map.get(slot_key)
            notification_type = ""
            description = ""
            
            # Define non-noteworthy statuses
            non_noteworthy_statuses = ["not taken yet", "upcoming", "pending", ""]

            if old_status is None:
                if new_status.lower() not in non_noteworthy_statuses:
                    notification_type = "New attendance record"
                    description = f"{course_display_name} - {session_display_name}: {new_status}"
            elif old_status != new_status:
                if new_status.lower() not in non_noteworthy_statuses or old_status.lower() not in non_noteworthy_statuses:
                    notification_type = "Attendance status updated"
                    description = f"{course_display_name} - {session_display_name}: {new_status} (was {old_status})"
            
            if notification_type and description:
                if description not in descriptions_added_this_session:
                    logger.info(f"Attempting to add attendance notification for {username}: {description}")
                    if add_notification(username, notification_type, description):
                        notifications_added_this_run.append([notification_type, description])
                        descriptions_added_this_session.add(description)
                    else:
                        logger.warning(f"Failed to add attendance notification for {username}: {description}")
    
    logger.debug(f"Finished attendance comparison for {username}. Notifications added: {len(notifications_added_this_run)}")
    return notifications_added_this_run


# --- REVISED compare_guc_data ---
def compare_guc_data(username, old_data, new_data):
    """
    Compares old and new GUC data, including student_info, detailed_grades (via compare_grades),
    and GUC system notifications.
    Returns a list of notifications generated during this comparison.
    """
    notifications_added_this_run = []
    logger.debug(f"Starting GUC data comparison for {username}")

    if not isinstance(old_data, dict) or not isinstance(new_data, dict):
        logger.warning(f"Skipping GUC data comparison for {username} - invalid data format.")
        return notifications_added_this_run

    descriptions_added_this_session = set() # To prevent duplicates in this specific run

    # 1. Compare Student Info (simple field by field)
    old_student_info = old_data.get("student_info", {})
    new_student_info = new_data.get("student_info", {})
    if isinstance(old_student_info, dict) and isinstance(new_student_info, dict):
        student_info_fields_to_check = ["fullname", "mail", "sg", "advisorname", "status", "studyprogram", "uniqappno"]
        for field in student_info_fields_to_check:
            old_val = _clean_string(old_student_info.get(field, ""))
            new_val = _clean_string(new_student_info.get(field, ""))
            if old_val != new_val and new_val: # Notify if changed and new value is not empty
                notification_type = "Student Info Update"
                description = f"{field.capitalize()} changed to: {new_val}"
                if old_val: # Include old value if it existed
                    description += f" (was {old_val})"
                
                if description not in descriptions_added_this_session:
                    if add_notification(username, notification_type, description):
                        notifications_added_this_run.append([notification_type, description])
                        descriptions_added_this_session.add(description)
    else:
        logger.warning(f"Student info format incorrect for {username}, skipping its comparison.")

    # 2. Compare Grades (detailed_grades and midterm_results) using the specialized compare_grades function
    # We pass the full old_data and new_data because compare_grades knows to extract the relevant grade parts.
    grade_notifications = compare_grades(username, old_data, new_data)
    for nt in grade_notifications: # Add to current run list, compare_grades already uses add_notification
        if nt[1] not in descriptions_added_this_session: # Ensure no cross-function duplicates for this session
            notifications_added_this_run.append(nt)
            descriptions_added_this_session.add(nt[1])

    # 3. Compare GUC System Notifications (list of dicts, use 'id' as stable key)
    old_guc_notifications = old_data.get("notifications", [])
    new_guc_notifications = new_data.get("notifications", [])

    if not isinstance(old_guc_notifications, list): old_guc_notifications = []
    if not isinstance(new_guc_notifications, list): new_guc_notifications = []

    # Create a map of old GUC notifications by their ID for efficient lookup
    old_guc_notif_map = {}
    for notif in old_guc_notifications:
        if isinstance(notif, dict) and notif.get("id") and notif.get("title") and notif.get("subject"):
             # Check if this notification is a dev announcement or user-specific, skip comparing them here
            if str(notif.get("id")) == str(getattr(config, 'DEV_ANNOUNCEMENT_ID', None)) or \
               str(notif.get("id")) == str(getattr(config, 'USER_SPECIFIC_UPDATES_ID', None)) or \
               str(notif.get("id")) == str(getattr(config, 'USER_SPECIFIC_PLACEHOLDER_ID', None)):
                continue
            old_guc_notif_map[str(notif["id"])] = notif

    for new_notif in new_guc_notifications:
        if isinstance(new_notif, dict) and new_notif.get("id") and new_notif.get("title") and new_notif.get("subject"):
            notif_id_str = str(new_notif["id"])
            
            # Skip dev announcements & user-specific updates placeholders, they are handled differently by API
            if notif_id_str == str(getattr(config, 'DEV_ANNOUNCEMENT_ID', None)) or \
               notif_id_str == str(getattr(config, 'USER_SPECIFIC_UPDATES_ID', None)) or \
               notif_id_str == str(getattr(config, 'USER_SPECIFIC_PLACEHOLDER_ID', None)):
                continue

            title = _clean_string(new_notif.get("title", "Untitled"))
            subject = _clean_string(new_notif.get("subject", "No Subject"))
            # Consider if date, staff also needs to be checked for changes if notif_id is same
            # For now, primarily detecting NEW notifications by ID.

            if notif_id_str not in old_guc_notif_map:
                notification_type = "New GUC Notification"
                description = f"{title} - {subject}"
                # Check for placeholder titles/subjects that might indicate no real content
                if title.lower() == "untitled" and subject.lower() == "no subject":
                    logger.debug(f"Skipping GUC notification for {username} due to placeholder content: {description}")
                    continue
                if description not in descriptions_added_this_session:
                    if add_notification(username, notification_type, description):
                        notifications_added_this_run.append([notification_type, description])
                        descriptions_added_this_session.add(description)
            # else: Notification ID already seen. Could add logic here to compare content if ID is same but title/subject/body changed.
            # For now, focusing on new notifications by ID.

    logger.debug(f"Finished GUC data comparison for {username}. Total notifications this run: {len(notifications_added_this_run)}")
    return notifications_added_this_run
