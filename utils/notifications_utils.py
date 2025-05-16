# utils/notifications_utils.py
import logging
# from api.notifications import add_notification, MAX_NOTIFICATIONS # REMOVE THIS LINE
from utils.cache import get_from_cache, generate_cache_key, set_in_cache # Added set_in_cache
import re  # Keep regex import
from config import config # Import config for timeout values

logger = logging.getLogger(__name__)

MAX_NOTIFICATIONS = 10  # Define the constant here, adjust value as needed

def add_notification(username: str, notification_type: str, description: str) -> bool:
    """
    Adds a new notification for a user and stores it in the cache.
    Notifications are stored as a list of lists: [[type, description], ...]
    Returns True if successfully added, False otherwise.
    """
    if not username or not notification_type or not description:
        logger.warning("add_notification called with missing parameters.")
        return False

    cache_key = generate_cache_key("notifications", username) # This is for the *general* notifications
    existing_notifications = get_from_cache(cache_key) or []

    if not isinstance(existing_notifications, list):
        logger.warning(f"Invalid existing notifications format for {username} (key: {cache_key}). Resetting to empty list.")
        existing_notifications = []

    # Avoid duplicate descriptions for the same type if desired (simple check here)
    # More sophisticated would be to check type AND description
    # For now, let's assume compare functions handle not re-adding identical items based on their logic.

    new_notification_item = [str(notification_type), str(description)]

    # Add to the beginning (most recent)
    updated_notifications = [new_notification_item] + existing_notifications

    # Limit the number of notifications
    updated_notifications = updated_notifications[:MAX_NOTIFICATIONS]

    # Cache with default timeout
    if set_in_cache(cache_key, updated_notifications, timeout=config.CACHE_DEFAULT_TIMEOUT):
        logger.info(f"Added notification for {username}: '{notification_type} - {description}'. Total: {len(updated_notifications)}")
        return True
    else:
        logger.error(f"Failed to cache notification for {username}: '{notification_type} - {description}'")
        return False


# --- Keep _clean_string helper ---
def _clean_string(text: str) -> str:
    """Helper to normalize strings for comparison."""
    if not isinstance(text, str):
        return ""
    # Collapse whitespace and strip leading/trailing
    return " ".join(text.strip().split())


# --- Keep _generate_grade_item_key helper ---
def _generate_grade_item_key(course_name: str, grade_info: dict) -> tuple | None:
    """
    Generates a unique key tuple for a grade item (course and assignment/element name).
    Returns None if essential info is missing.
    Key: (Cleaned Course Name, Cleaned Item Name)
    """
    item_name = grade_info.get("Element Name", "").strip()
    if not item_name: # Fallback to Quiz/Assignment if Element Name is empty
        item_name = grade_info.get("Quiz/Assignment", "").strip()

    # An item name is essential for a key
    if not item_name:
        return None

    cleaned_course = _clean_string(course_name)
    cleaned_item_name = _clean_string(item_name)

    if not cleaned_course or not cleaned_item_name:
        return None

    return (cleaned_course, cleaned_item_name)


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


# --- MODIFIED compare_grades ---
def compare_grades(username, old_data, new_data):
    """
    Compare old and new grades data to detect changes, focusing on content.
    Uses a stable key (course, item_name) to compare grade values.
    """
    notifications_added_this_run = []
    logger.debug(f"Starting grade comparison for {username}")

    if not isinstance(old_data, dict) or not isinstance(new_data, dict):
        logger.warning(
            f"Skipping grades comparison for {username} - invalid data format (old: {type(old_data)}, new: {type(new_data)})"
        )
        return notifications_added_this_run

    existing_notifications = get_from_cache(generate_cache_key("notifications", username)) or []
    if not isinstance(existing_notifications, list):
        logger.warning(f"Invalid notification cache format for {username}. Resetting.")
        existing_notifications = []
    
    # Store descriptions of notifications already sent in this session to avoid duplicates from this specific run.
    # The main add_notification might also have its own de-duplication based on its cache if needed.
    descriptions_added_this_session = set()

    # --- Midterm Comparison (largely unchanged, ensure _clean_string is robust) ---
    old_midterms_normalized = {
        _clean_string(course): _clean_string(grade_info.get("grade", "") if isinstance(grade_info, dict) else str(grade_info))
        for course, grade_info in old_data.get("midterm_results", {}).items()
    }
    new_midterms_normalized = {
        _clean_string(course): _clean_string(grade_info.get("grade", "") if isinstance(grade_info, dict) else str(grade_info))
        for course, grade_info in new_data.get("midterm_results", {}).items()
    }

    for cleaned_course_name, new_grade_val in new_midterms_normalized.items():
        old_grade_val = old_midterms_normalized.get(cleaned_course_name)
        is_new_or_changed = old_grade_val != new_grade_val
        is_meaningful_grade = new_grade_val and new_grade_val != "0" and not new_grade_val.startswith("/") and new_grade_val.strip() != ""

        if is_new_or_changed and is_meaningful_grade:
            notification_type = "New midterm grade" if not old_grade_val or (old_grade_val and old_grade_val.startswith("/")) else "Updated midterm grade"
            description = f"{cleaned_course_name}: {new_grade_val}"
            if description not in descriptions_added_this_session:
                if add_notification(username, notification_type, description):
                    notifications_added_this_run.append([notification_type, description])
                    descriptions_added_this_session.add(description)

    # --- Detailed Grades Comparison (Content-Based with Stable Item Key) ---
    old_detailed_grades_raw = old_data.get("detailed_grades", {})
    new_detailed_grades_raw = new_data.get("detailed_grades", {})

    if not isinstance(old_detailed_grades_raw, dict) or not isinstance(new_detailed_grades_raw, dict):
        logger.warning(f"Detailed grades data format mismatch for {username}. Skipping.")
        return notifications_added_this_run

    # 1. Build map of old detailed grades: {(course, item_name): cleaned_grade_value}
    old_grades_map = {}
    for course_name_key, course_grades in old_detailed_grades_raw.items():
        if not isinstance(course_grades, dict): continue
        cleaned_course_name = _clean_string(course_name_key)
        if not cleaned_course_name: continue
        for grade_item_details in course_grades.values(): # Iterating dict values
            if not isinstance(grade_item_details, dict): continue
            item_key = _generate_grade_item_key(cleaned_course_name, grade_item_details)
            grade_value = _clean_string(grade_item_details.get("grade", ""))
            if item_key and grade_value and not grade_value.startswith("/") and grade_value.strip() != "": # Only store if key and meaningful grade exists
                old_grades_map[item_key] = grade_value
    
    logger.debug(f"Built old_grades_map with {len(old_grades_map)} entries for {username}.")

    # 2. Compare new detailed grades against the old map
    for course_name_key, course_grades in new_detailed_grades_raw.items():
        if not isinstance(course_grades, dict): continue
        cleaned_course_name = _clean_string(course_name_key)
        if not cleaned_course_name: continue
        for grade_item_details in course_grades.values(): # Iterating dict values
            if not isinstance(grade_item_details, dict): continue
            
            item_key = _generate_grade_item_key(cleaned_course_name, grade_item_details)
            if not item_key: continue # Skip if no valid item key can be generated

            new_grade_value = _clean_string(grade_item_details.get("grade", ""))
            is_meaningful_new_grade = new_grade_value and not new_grade_value.startswith("/") and new_grade_value.strip() != ""

            if not is_meaningful_new_grade:
                continue # Skip unreleased/empty grades

            course_display_name, item_display_name = item_key # item_key is (cleaned_course, cleaned_item_name)

            old_grade_value = old_grades_map.get(item_key)

            notification_type = ""
            description = ""

            if old_grade_value is None: # Grade item is entirely new
                notification_type = "New grade"
                description = f"{course_display_name} - {item_display_name}: {new_grade_value}"
            elif old_grade_value != new_grade_value: # Grade item existed, but value changed
                notification_type = "Updated grade"
                description = f"{course_display_name} - {item_display_name}: {new_grade_value} (was {old_grade_value})"
            
            if notification_type and description:
                 # Avoid duplicate notifications within the same processing run
                if description not in descriptions_added_this_session:
                    logger.info(f"Attempting to add detailed grade notification for {username}: {description}")
                    if add_notification(username, notification_type, description):
                        notifications_added_this_run.append([notification_type, description])
                        descriptions_added_this_session.add(description)
                    else:
                        logger.warning(f"Failed to add detailed grade notification for {username}: {description}")
    
    logger.debug(f"Finished grade comparison for {username}. Notifications added this run: {len(notifications_added_this_run)}")
    return notifications_added_this_run


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
            if str(notif.get("id")) == str(config.DEV_ANNOUNCEMENT_ID) or \
               str(notif.get("id")) == str(config.USER_SPECIFIC_UPDATES_ID) or \
               str(notif.get("id")) == str(config.USER_SPECIFIC_PLACEHOLDER_ID):
                continue
            old_guc_notif_map[str(notif["id"])] = notif

    for new_notif in new_guc_notifications:
        if isinstance(new_notif, dict) and new_notif.get("id") and new_notif.get("title") and new_notif.get("subject"):
            notif_id_str = str(new_notif["id"])
            
            # Skip dev announcements & user-specific updates placeholders, they are handled differently by API
            if notif_id_str == str(config.DEV_ANNOUNCEMENT_ID) or \
               notif_id_str == str(config.USER_SPECIFIC_UPDATES_ID) or \
               notif_id_str == str(config.USER_SPECIFIC_PLACEHOLDER_ID):
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
