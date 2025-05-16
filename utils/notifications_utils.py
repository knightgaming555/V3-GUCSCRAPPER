# utils/notifications_utils.py
import logging
# from api.notifications import add_notification, MAX_NOTIFICATIONS # REMOVE THIS LINE
from utils.cache import get_from_cache, generate_cache_key, set_in_cache # Added set_in_cache
import re  # Keep regex import
from config import config # Import config for timeout values

logger = logging.getLogger(__name__)

MAX_NOTIFICATIONS = 10  # Define the constant here, adjust value as needed

# --- Constants for Grade Comparison ---
_PLACEHOLDER_GRADES = ["na", "n/a", "-", "undetermined", "", "/"] # "" for empty, "/" for "/10" like

# --- Helper Functions for Grade Comparison ---

def _is_placeholder_grade(grade_str: str | None) -> bool:
    """Checks if a grade string is a placeholder or effectively empty."""
    if grade_str is None:
        return True
    cleaned = _clean_string(grade_str) # _clean_string handles None by returning ""
    if not cleaned: return True # Empty after cleaning
    if cleaned == "/": return True # Common for empty grades like " /10" which clean to "/"
    # Add more specific GUC placeholders if known, e.g., "Not Graded", "Pending"
    return cleaned.lower() in _PLACEHOLDER_GRADES

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

# --- Main Grade Comparison Logic (Refactored for Index-Based Matching) ---
def compare_grades(username: str, old_detailed_grades_raw: dict, new_detailed_grades_raw: dict) -> list:
    """
    Compares old and new detailed grades based on item order within each course.
    Notifications are triggered for actual grade value changes or new (non-placeholder) grades.
    Name changes alone at the same position do not trigger notifications if the grade value is identical.
    """
    notifications = []
    if not isinstance(old_detailed_grades_raw, dict) or not isinstance(new_detailed_grades_raw, dict):
        # Handle cases where entire grade sets might be missing (e.g., initial scrape error)
        # Depending on desired behavior, could log or create a general notification.
        # For now, if either is not a dict, assume no comparison possible or no old data.
        # If new_detailed_grades_raw IS a dict, all its non-placeholder items would appear "New".
        if isinstance(new_detailed_grades_raw, dict) and not isinstance(old_detailed_grades_raw, dict):
             for course_name_raw, new_course_items_dict in new_detailed_grades_raw.items():
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
        return notifications


    # 1. Build map of old detailed grades: {(course_name, list_index): (cleaned_grade_value, display_name_of_old_item)}
    old_grades_map_by_index = {}
    for course_name_raw, old_course_items_dict in old_detailed_grades_raw.items():
        if not isinstance(old_course_items_dict, dict): continue
        cleaned_course_name_for_map = _clean_string(course_name_raw)
        if not cleaned_course_name_for_map: continue

        # old_course_items_dict.items() gives (item_dict_key_from_scraper, item_details)
        # We rely on dict insertion order being preserved from scraping.
        for list_idx, (item_dict_key_old, old_item_details) in enumerate(old_course_items_dict.items()):
            if not isinstance(old_item_details, dict): continue
            
            map_key = (cleaned_course_name_for_map, list_idx)
            
            old_grade_raw = old_item_details.get("grade")
            if _is_placeholder_grade(old_grade_raw): # Skip adding old placeholders to the map
                continue

            # Store the *cleaned* grade in the map if it's not a placeholder
            old_grade_cleaned = _clean_string(old_grade_raw)
            item_display_name_old = _get_grade_display_name_from_details(old_item_details, item_dict_key_old)
            old_grades_map_by_index[map_key] = (old_grade_cleaned, item_display_name_old)

    # 2. Iterate through new detailed grades, comparing with old map by course and list index
    for course_name_raw, new_course_items_dict in new_detailed_grades_raw.items():
        if not isinstance(new_course_items_dict, dict): continue
        cleaned_course_name_for_new = _clean_string(course_name_raw)
        if not cleaned_course_name_for_new: continue

        for list_idx, (item_dict_key_new, new_item_details) in enumerate(new_course_items_dict.items()):
            if not isinstance(new_item_details, dict): continue

            current_comparison_key = (cleaned_course_name_for_new, list_idx)
            
            new_grade_raw = new_item_details.get("grade")
            new_grade_cleaned_for_notif = _clean_string(new_grade_raw) # Cleaned version for display & comparison
            current_item_display_name_new = _get_grade_display_name_from_details(new_item_details, item_dict_key_new)

            is_new_grade_placeholder = _is_placeholder_grade(new_grade_raw)

            if current_comparison_key in old_grades_map_by_index:
                old_grade_from_map, _old_item_display_name = old_grades_map_by_index[current_comparison_key]
                # old_grade_from_map is already cleaned and verified not to be a placeholder.

                if is_new_grade_placeholder:
                    # New grade is placeholder, old grade (from map) was real. This is a change: grade cleared.
                    notif_desc = f"{cleaned_course_name_for_new} - {current_item_display_name_new}: (grade removed/cleared, was {old_grade_from_map})"
                    notifications.append(["Updated grade", notif_desc])
                elif new_grade_cleaned_for_notif != old_grade_from_map:
                    # New grade is real, old grade was real. Compare values.
                    notif_desc = f"{cleaned_course_name_for_new} - {current_item_display_name_new}: {new_grade_cleaned_for_notif} (was {old_grade_from_map})"
                    notifications.append(["Updated grade", notif_desc])
                # If new_grade_cleaned_for_notif == old_grade_from_map (and new is not placeholder), no change in value, no notification.
                    
            else: # New item at this list index (e.g., course is new, or list of grades for this course is longer)
                if not is_new_grade_placeholder: # Only notify if the new item has a real, non-placeholder grade
                    notif_desc = f"{cleaned_course_name_for_new} - {current_item_display_name_new}: {new_grade_cleaned_for_notif}"
                    notifications.append(["New grade", notif_desc])
       
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
    grade_notifications = compare_grades(username, old_data.get("detailed_grades", {}), new_data.get("detailed_grades", {}))
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
