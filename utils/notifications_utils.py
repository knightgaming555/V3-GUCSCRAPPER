# utils/notifications_utils.py
import logging
from api.notifications import add_notification, MAX_NOTIFICATIONS
from utils.cache import get_from_cache, generate_cache_key
import re  # Keep regex import

logger = logging.getLogger(__name__)


# --- Keep _clean_string helper ---
def _clean_string(text: str) -> str:
    """Helper to normalize strings for comparison."""
    if not isinstance(text, str):
        return ""
    # Collapse whitespace and strip leading/trailing
    return " ".join(text.strip().split())


# --- Keep _generate_grade_identifier helper ---
def _generate_grade_identifier(course_name: str, grade_info: dict) -> tuple | None:
    """
    Generates a unique identifier tuple for a grade based on its content.
    Returns None if essential info is missing.
    Identifier: (Cleaned Course Name, Cleaned Quiz/Assignment Name, Cleaned Grade Value)
    """
    quiz_name = grade_info.get("Quiz/Assignment", "")
    grade_value = grade_info.get("grade", "")

    # Skip if essential info is missing or grade is just a placeholder "/" or starts with "/"
    if (
        not quiz_name
        or not grade_value
        or grade_value.strip() == "/"
        or grade_value.strip().startswith("/")
    ):
        # logger.debug(f"Skipping grade identifier: Course='{course_name}', Quiz='{quiz_name}', Grade='{grade_value}'")
        return None

    cleaned_course = _clean_string(course_name)
    cleaned_quiz = _clean_string(quiz_name)
    cleaned_grade = _clean_string(grade_value)  # Clean the grade string itself

    # Ensure critical components are present after cleaning
    if not cleaned_course or not cleaned_quiz or not cleaned_grade:
        # logger.debug(f"Skipping grade identifier due to empty cleaned info: Course='{cleaned_course}', Quiz='{cleaned_quiz}', Grade='{cleaned_grade}'")
        return None

    return (cleaned_course, cleaned_quiz, cleaned_grade)


# --- Keep _generate_attendance_identifier helper ---
def _generate_attendance_identifier(
    course_name: str, session_info: dict
) -> tuple | None:
    """
    Generates a unique identifier tuple for an attendance record based on its content.
    Accepts an already cleaned course_name.
    Returns None if essential info is missing.
    Identifier: (Cleaned Course Name, Cleaned Session Description, Cleaned Status)
    """
    # Assume course_name is already cleaned by the caller
    cleaned_course = course_name

    # The 'session' key holds the descriptive string (e.g., "S25 - CSEN 202...Slot5 - 2h")
    session_description = session_info.get("session", "")
    # The 'status' key holds the attendance status (e.g., "Attended", "Absent")
    status = session_info.get("status", "")

    # Skip if essential info is missing
    if not session_description or not status:
        # logger.debug(f"Skipping attendance identifier generation due to missing info: {session_info}")
        return None

    # Clean the whole session description string AND the status separately
    cleaned_session_desc = _clean_string(session_description)
    cleaned_status = _clean_string(status)

    # Ensure critical components are present after cleaning
    if not cleaned_course or not cleaned_session_desc or not cleaned_status:
        # logger.debug(f"Skipping attendance identifier generation due to empty cleaned info: course='{cleaned_course}', desc='{cleaned_session_desc}', status='{cleaned_status}'")
        return None

    # Return the tuple containing all three parts
    return (cleaned_course, cleaned_session_desc, cleaned_status)


# --- Keep UPDATED compare_grades ---
def compare_grades(username, old_data, new_data):
    """
    Compare old and new grades data to detect changes, focusing on content.
    """
    notifications_added_this_run = []
    logger.debug(f"Starting grade comparison for {username}")

    if not isinstance(old_data, dict) or not isinstance(new_data, dict):
        logger.warning(
            f"Skipping grades comparison for {username} - invalid data format (old: {type(old_data)}, new: {type(new_data)})"
        )
        return notifications_added_this_run

    # --- Notification Cache Setup ---
    cache_key = generate_cache_key("notifications", username)
    existing_notifications = get_from_cache(cache_key) or []
    if not isinstance(existing_notifications, list):
        logger.warning(f"Invalid notification cache format for {username}. Resetting.")
        existing_notifications = []
    existing_descriptions_in_cache = {
        notification[1]
        for notification in existing_notifications
        if len(notification) == 2
    }
    descriptions_added_this_run = set()
    logger.debug(
        f"{len(existing_descriptions_in_cache)} existing grade notification descriptions loaded from cache."
    )

    # --- Midterm Comparison (Normalize course name key) ---
    old_midterms_normalized = {
        _clean_string(course): _clean_string(grade)
        for course, grade in old_data.get("midterm_results", {}).items()
        if isinstance(grade, str)
    }  # Ensure grade is string
    new_midterms_normalized = {
        _clean_string(course): _clean_string(grade)
        for course, grade in new_data.get("midterm_results", {}).items()
        if isinstance(grade, str)
    }

    for cleaned_course_name, new_grade in new_midterms_normalized.items():
        old_grade = old_midterms_normalized.get(
            cleaned_course_name
        )  # Lookup using cleaned key
        # Check if grade is new or different, and meaningful (not empty, not '0', not just '/')
        if (
            old_grade != new_grade
            and new_grade
            and new_grade != "0"
            and not new_grade.startswith("/")
        ):
            notification_type = "New midterm grade"
            description = (
                f"{cleaned_course_name}: {new_grade}"  # Use cleaned components
            )
            if (
                description not in existing_descriptions_in_cache
                and description not in descriptions_added_this_run
            ):
                logger.info(
                    f"Attempting to add midterm grade notification for {username}: {description}"
                )
                if add_notification(username, notification_type, description):
                    notifications_added_this_run.append(
                        [notification_type, description]
                    )
                    descriptions_added_this_run.add(description)
                else:
                    logger.warning(
                        f"Failed to add midterm grade notification for {username}: {description}"
                    )

    # --- Detailed Grades Comparison (Content-Based with Normalized Keys) ---
    old_detailed = old_data.get("detailed_grades", {})
    new_detailed = new_data.get("detailed_grades", {})
    if not isinstance(old_detailed, dict) or not isinstance(new_detailed, dict):
        logger.warning(
            f"Detailed grades data format mismatch for {username}. Skipping detailed comparison."
        )
        return notifications_added_this_run  # Return any midterm notifications

    # 1. Build set of old grade identifiers {(cleaned_course, cleaned_quiz, cleaned_grade)}
    old_grade_identifiers = set()
    logger.debug("Building old_grade_identifiers set...")
    for course_name_key, course_grades in old_detailed.items():
        if not isinstance(course_grades, dict):
            continue
        cleaned_course_key = _clean_string(course_name_key)
        if not cleaned_course_key:
            continue
        for grade_info in course_grades.values():  # Iterate values
            if not isinstance(grade_info, dict):
                continue
            # Pass cleaned course key to helper
            identifier = _generate_grade_identifier(cleaned_course_key, grade_info)
            if identifier:
                old_grade_identifiers.add(identifier)
                # logger.debug(f"Stored old grade identifier: {identifier}")

    logger.debug(
        f"Built old_grade_identifiers set with {len(old_grade_identifiers)} entries."
    )

    # 2. Compare new identifiers against old set
    processed_new_identifiers_this_run = set()
    logger.debug("Processing new detailed grades...")
    for course_name_key, course_grades in new_detailed.items():
        if not isinstance(course_grades, dict):
            continue
        cleaned_course_key = _clean_string(course_name_key)
        if not cleaned_course_key:
            continue
        # logger.debug(f"Processing new grades for cleaned course key: '{cleaned_course_key}'")
        for grade_info in course_grades.values():
            if not isinstance(grade_info, dict):
                continue
            identifier = _generate_grade_identifier(cleaned_course_key, grade_info)
            if not identifier:
                continue  # Skip placeholders or missing info

            # Skip if already processed this exact identifier from new data
            if identifier in processed_new_identifiers_this_run:
                continue
            processed_new_identifiers_this_run.add(identifier)

            # Check if this content-based identifier existed previously
            if identifier not in old_grade_identifiers:
                logger.info(
                    f"New grade detected: Course='{identifier[0]}', Quiz='{identifier[1]}', Grade='{identifier[2]}'"
                )
                notification_type = "New grade"
                description = f"{identifier[0]} - {identifier[1]}: {identifier[2]}"  # Use cleaned components

                if (
                    description not in existing_descriptions_in_cache
                    and description not in descriptions_added_this_run
                ):
                    logger.info(
                        f"Attempting to add detailed grade notification for {username}: {description}"
                    )
                    if add_notification(username, notification_type, description):
                        notifications_added_this_run.append(
                            [notification_type, description]
                        )
                        descriptions_added_this_run.add(description)
                    else:
                        logger.warning(
                            f"Failed to add detailed grade notification for {username}: {description}"
                        )
                else:
                    logger.debug(
                        f"Skipping grade notification (already exists in cache or added this run): {description}"
                    )

    logger.info(
        f"Finished grade comparison for {username}. Added {len(notifications_added_this_run)} notifications this run."
    )
    return notifications_added_this_run


# --- REVISED compare_attendance ---
def compare_attendance(username, old_data, new_data):
    """
    Compare old and new attendance data to detect changes, focusing on content
    and normalizing keys/strings consistently.
    """
    notifications_added_this_run = []
    logger.debug(f"Starting attendance comparison for {username}")

    # --- Basic Type/Structure Validation ---
    if not isinstance(old_data, dict) or not isinstance(new_data, dict):
        logger.warning(
            f"Skipping attendance comparison for {username} - invalid data format (old: {type(old_data)}, new: {type(new_data)})"
        )
        return notifications_added_this_run

    # --- Notification Cache Setup ---
    cache_key = generate_cache_key("notifications", username)
    existing_notifications = get_from_cache(cache_key) or []
    if not isinstance(existing_notifications, list):
        logger.warning(f"Invalid notification cache format for {username}. Resetting.")
        existing_notifications = []
    existing_descriptions_in_cache = {
        notification[1]
        for notification in existing_notifications
        if len(notification) == 2
    }
    descriptions_added_this_run = set()
    logger.debug(
        f"{len(existing_descriptions_in_cache)} existing attendance notification descriptions loaded from cache."
    )

    # --- Absence Level Comparison (Normalize course name key) ---
    old_absence_levels_normalized = {
        _clean_string(course): data.get("absence_level", "")
        for course, data in old_data.items()
        if isinstance(data, dict) and isinstance(data.get("absence_level"), str)
    }
    new_absence_levels_normalized = {
        _clean_string(course): data.get("absence_level", "")
        for course, data in new_data.items()
        if isinstance(data, dict) and isinstance(data.get("absence_level"), str)
    }

    for cleaned_course_name, new_level in new_absence_levels_normalized.items():
        old_level = old_absence_levels_normalized.get(
            cleaned_course_name, ""
        )  # Use cleaned key for lookup
        new_level_cleaned = _clean_string(new_level)

        if (
            old_level != new_level_cleaned
            and new_level_cleaned
            and new_level_cleaned != "No Warning Level"
        ):
            notification_type = "Attendance warning"
            description = f"{cleaned_course_name}: {new_level_cleaned}"
            if (
                description not in existing_descriptions_in_cache
                and description not in descriptions_added_this_run
            ):
                logger.info(
                    f"Attempting to add absence level notification for {username}: {description}"
                )
                if add_notification(username, notification_type, description):
                    notifications_added_this_run.append(
                        [notification_type, description]
                    )
                    descriptions_added_this_run.add(description)
                else:
                    logger.warning(
                        f"Failed to add absence level notification for {username}: {description}"
                    )

    # --- Individual Session Comparison (Content-Based with Normalized Keys) ---
    # 1. Build a map of OLD identifiers: {(cleaned_course, cleaned_session_desc): cleaned_status}
    old_session_statuses = {}
    logger.debug("Building old_session_statuses map...")
    for course_name_key, course_data in old_data.items():
        if not isinstance(course_data, dict):
            continue
        sessions_list = course_data.get("sessions", [])
        if not isinstance(sessions_list, list):
            continue
        cleaned_course_key = _clean_string(course_name_key)
        if not cleaned_course_key:
            continue

        for session_info in sessions_list:
            if not isinstance(session_info, dict):
                continue
            # Pass cleaned course key to helper
            identifier = _generate_attendance_identifier(
                cleaned_course_key, session_info
            )
            if identifier:
                session_key = (identifier[0], identifier[1])
                cleaned_status = identifier[2]
                old_session_statuses[session_key] = cleaned_status
                # logger.debug(f"Stored old attendance: Key=('{session_key[0]}', '{session_key[1]}'), Status='{cleaned_status}'")
            # else:
            # logger.debug(f"Failed to generate old attendance identifier for session: {session_info} under course key: '{course_name_key}'")

    logger.debug(
        f"Built old_session_statuses map with {len(old_session_statuses)} entries."
    )

    # 2. Iterate through NEW data and compare identifiers/statuses
    processed_new_session_keys_this_run = (
        set()
    )  # Track (course, session_desc) processed from new data
    logger.debug("Processing new attendance data...")
    for course_name_key, course_data in new_data.items():
        if not isinstance(course_data, dict):
            continue
        sessions_list = course_data.get("sessions", [])
        if not isinstance(sessions_list, list):
            continue
        cleaned_course_key = _clean_string(course_name_key)
        if not cleaned_course_key:
            continue

        # logger.debug(f"Processing {len(sessions_list)} new sessions for cleaned course key: '{cleaned_course_key}' (Original: '{course_name_key}')")
        for session_info in sessions_list:
            if not isinstance(session_info, dict):
                continue
            # Pass cleaned course key to helper
            identifier = _generate_attendance_identifier(
                cleaned_course_key, session_info
            )
            if not identifier:
                # logger.debug(f"Failed to generate new attendance identifier for session: {session_info} under course key: '{course_name_key}'")
                continue

            session_key = (identifier[0], identifier[1])
            current_cleaned_status = identifier[2]
            # logger.debug(f"Processing new attendance: Key=('{session_key[0]}', '{session_key[1]}'), Status='{current_cleaned_status}'")

            # Skip if already processed this exact session key in this run
            if session_key in processed_new_session_keys_this_run:
                # logger.debug(f"Skipping already processed session this run: Key=('{session_key[0]}', '{session_key[1]}')")
                continue
            processed_new_session_keys_this_run.add(session_key)

            # 3. Check against old statuses using the generated session_key
            old_cleaned_status = old_session_statuses.get(session_key)
            # logger.debug(f"Comparing Status: Old='{old_cleaned_status}', New='{current_cleaned_status}' for Key=('{session_key[0]}', '{session_key[1]}')")

            if (
                old_cleaned_status is None
                or old_cleaned_status != current_cleaned_status
            ):
                change_reason = (
                    "New session"
                    if old_cleaned_status is None
                    else f"Status changed from '{old_cleaned_status}' to '{current_cleaned_status}'"
                )
                logger.info(
                    f"Change detected for attendance Key=('{session_key[0]}', '{session_key[1]}'): {change_reason}"
                )
                notification_type = "Attendance update"
                description = f"{identifier[1]}: {identifier[2]}"  # Use cleaned session desc and status
                # logger.debug(f"Potential notification description: '{description}'")

                # 4. Check against cached notifications and those added this run
                if description not in existing_descriptions_in_cache:
                    if description not in descriptions_added_this_run:
                        logger.info(
                            f"Attempting to add notification for {username}: {description}"
                        )
                        if add_notification(username, notification_type, description):
                            notifications_added_this_run.append(
                                [notification_type, description]
                            )
                            descriptions_added_this_run.add(description)
                        else:
                            logger.warning(
                                f"Failed to add attendance notification via add_notification for {username}: {description}"
                            )
                    # else:
                    # logger.debug(f"Skipping notification (already added this run): {description}")
                # else:
                # logger.debug(f"Skipping notification (already exists in cache): {description}")
            # else:
            # logger.debug(f"No change detected for attendance Key=('{session_key[0]}', '{session_key[1]}')")

    logger.info(
        f"Finished attendance comparison for {username}. Added {len(notifications_added_this_run)} notifications this run."
    )
    return notifications_added_this_run


# --- Keep UPDATED compare_guc_data ---
def compare_guc_data(username, old_data, new_data):
    """
    Compare old and new GUC data (specifically notifications section) to detect changes.
    """
    notifications_added_this_run = []
    logger.debug(f"Starting GUC data comparison for {username}")

    if not isinstance(old_data, dict) or not isinstance(new_data, dict):
        logger.warning(
            f"Skipping GUC data comparison for {username} - invalid data format."
        )
        return notifications_added_this_run

    # --- Notification Cache Setup ---
    cache_key = generate_cache_key("notifications", username)
    existing_notifications = get_from_cache(cache_key) or []
    if not isinstance(existing_notifications, list):
        logger.warning(f"Invalid notification cache format for {username}. Resetting.")
        existing_notifications = []
    existing_descriptions_in_cache = {
        notification[1]
        for notification in existing_notifications
        if len(notification) == 2
    }
    descriptions_added_this_run = set()
    logger.debug(
        f"{len(existing_descriptions_in_cache)} existing GUC notification descriptions loaded from cache."
    )

    # --- GUC Notification Comparison ---
    old_guc_notifications = old_data.get("notifications", [])
    new_guc_notifications = new_data.get("notifications", [])

    if not isinstance(old_guc_notifications, list):
        old_guc_notifications = []
    if not isinstance(new_guc_notifications, list):
        new_guc_notifications = []

    # Build set of old identifiers (id or title)
    old_notification_identifiers = set()
    for n in old_guc_notifications:
        if not isinstance(n, dict):
            continue
        notif_id = str(n.get("id", "")).strip()
        # Clean title *before* using it as an identifier
        title = _clean_string(n.get("title", "").replace("Notification System:", ""))
        if notif_id:
            old_notification_identifiers.add(f"id:{notif_id}")
        elif title:
            old_notification_identifiers.add(f"title:{title}")  # Use cleaned title

    logger.debug(
        f"Built old GUC notification identifiers set with {len(old_notification_identifiers)} entries."
    )

    # Compare new notifications against old set
    processed_new_guc_notification_ids = set()
    for notification in new_guc_notifications:
        if not isinstance(notification, dict):
            continue
        notification_id = str(notification.get("id", "")).strip()
        # Clean title *before* creating description or identifier
        title = _clean_string(
            notification.get("title", "").replace("Notification System:", "")
        )

        if not title:
            continue  # Skip if no title after cleaning

        current_identifier_this_run = (
            f"id:{notification_id}" if notification_id else f"title:{title}"
        )
        if current_identifier_this_run in processed_new_guc_notification_ids:
            continue
        processed_new_guc_notification_ids.add(current_identifier_this_run)

        if current_identifier_this_run not in old_notification_identifiers:
            logger.info(
                f"New GUC notification detected: Identifier='{current_identifier_this_run}', Title='{title}'"
            )
            notification_type = "GUC notification"
            description = title  # Use the cleaned title

            if (
                description not in existing_descriptions_in_cache
                and description not in descriptions_added_this_run
            ):
                logger.info(
                    f"Attempting to add GUC notification for {username}: {description}"
                )
                if add_notification(username, notification_type, description):
                    notifications_added_this_run.append(
                        [notification_type, description]
                    )
                    descriptions_added_this_run.add(description)
                else:
                    logger.warning(
                        f"Failed to add GUC notification for {username}: {description}"
                    )
            else:
                logger.debug(
                    f"Skipping GUC notification (already exists in cache or added this run): {description}"
                )

    logger.info(
        f"Finished GUC data comparison for {username}. Added {len(notifications_added_this_run)} notifications this run."
    )
    return notifications_added_this_run
