"""
Gemini AI integration for extracting upcoming events from GUC data and CMS content.
"""

import logging
import json
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from google import genai
from google.genai import types
from google.genai.errors import ServerError

logger = logging.getLogger(__name__)

# Initialize the Gemini AI client with the API key
API_KEY = ""

# Maximum retries for API calls
MAX_RETRIES = 3
# Backoff time between retries (in seconds)
BACKOFF_TIME = 2


def initialize_gemini_client():
    """Initialize the Gemini AI client."""
    try:
        client = genai.Client(api_key=API_KEY)
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Gemini AI client: {e}", exc_info=True)
        return None


def extract_upcoming_events(
    guc_data: Dict[str, Any],
    cms_courses: List[Dict[str, Any]],
    cms_content: Dict[str, List[Dict[str, Any]]],
    days_ahead: int = 30,
) -> List[Dict[str, Any]]:
    """
    Extract upcoming events from GUC data and CMS content using Gemini AI.

    Args:
        guc_data: The GUC data containing notifications
        cms_courses: List of CMS courses
        cms_content: Dictionary mapping course URLs to their content
        days_ahead: Number of days ahead to consider for upcoming events

    Returns:
        List of upcoming events in the required format
    """
    try:
        client = initialize_gemini_client()
        if not client:
            logger.error("Failed to initialize Gemini AI client")
            return []

        # Prepare the current date information
        current_date = datetime.now()
        cutoff_date = current_date + timedelta(days=days_ahead)

        # Prepare the input for the AI model
        input_data = {
            "current_date": current_date.isoformat(),
            "cutoff_date": cutoff_date.isoformat(),
            "guc_notifications": _prepare_guc_notifications(guc_data),
            "cms_courses": _prepare_cms_data(cms_courses, cms_content),
        }

        # Convert to JSON string for the AI model
        input_json = json.dumps(input_data, default=str)

        # Create the prompt for the AI model
        prompt = f"""
        You are an AI assistant that extracts upcoming deadlines and events from university data.

        Your task is to analyze the provided data and extract all upcoming deadlines and events that occur between the current date and the cutoff date.

        Focus on extracting:
        1. Quizzes, assignments, exams, and projects with specific dates
        2. Compensation tutorials/lectures
        3. Any other important academic deadlines

        Ignore:
        1. Regular lectures and tutorials (unless they are compensation sessions)
        2. Events without clear dates
        3. Past events

        For each event, extract:
        - upcoming_title: A clear title for the event
        - upcoming_date: The date of the event in a readable format (e.g., "Monday, April 15, 2025")
        - upcoming_type: The type of event (Quiz, Exam, Assignment, Project, Compensation, Other)
        - upcoming_source: The source of the event (course name or department)
        - upcoming_description: A brief description of the event

        Here is the data to analyze (in JSON format):
        {input_json}

        Return ONLY a JSON array of events in the specified format. Do not include any explanations or additional text.
        Each event must have all the required fields.
        """

        # Generate content using the Gemini AI model
        response = client.models.generate_content(
            model="gemini-2.0-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.1,  # Low temperature for more deterministic results
            ),
        )

        # Parse the response
        if hasattr(response, "text"):
            try:
                events = json.loads(response.text)
                logger.info(
                    f"Successfully extracted {len(events)} upcoming events using Gemini AI"
                )
                return events
            except json.JSONDecodeError as e:
                logger.error(
                    f"Failed to parse Gemini AI response as JSON: {e}", exc_info=True
                )
                logger.error(f"Response text: {response.text}")
                return []
        else:
            logger.error("Gemini AI response does not contain text")
            return []

    except Exception as e:
        logger.error(f"Error in extract_upcoming_events: {e}", exc_info=True)
        return []


def _prepare_guc_notifications(guc_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Prepare GUC notifications for the AI model.

    Args:
        guc_data: The GUC data containing notifications

    Returns:
        List of prepared notifications
    """
    notifications = []

    # Check if guc_data contains notifications
    if not guc_data or "notifications" not in guc_data:
        return notifications

    # Get notifications from the last month
    one_month_ago = datetime.now() - timedelta(days=30)

    for notification in guc_data.get("notifications", []):
        # Try to parse the notification date
        notification_date = None
        date_str = notification.get("date", "")
        if date_str:
            try:
                notification_date = datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                pass

        # Skip notifications older than one month
        if notification_date and notification_date < one_month_ago:
            continue

        # Add the notification to the list
        notifications.append(
            {
                "title": notification.get("title", ""),
                "subject": notification.get("subject", ""),
                "body": notification.get("body", ""),
                "date": date_str,
                "staff": notification.get("staff", ""),
                "department": notification.get("department", ""),
            }
        )

    return notifications


def _prepare_cms_data(
    cms_courses: List[Dict[str, Any]], cms_content: Dict[str, List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    """
    Prepare CMS data for the AI model.

    Args:
        cms_courses: List of CMS courses
        cms_content: Dictionary mapping course URLs to their content

    Returns:
        List of prepared CMS data
    """
    prepared_data = []

    for course in cms_courses:
        course_name = course.get("course_name", "")
        course_url = course.get("course_url", "")

        # Skip if course name or URL is missing
        if not course_name or not course_url:
            continue

        # Get content for this course
        content_list = cms_content.get(course_url, [])

        course_data = {"course_name": course_name, "content": []}

        # Process each content item
        for item in content_list:
            # Skip if the item doesn't have a title
            if "title" not in item:
                continue

            content_item = {
                "title": item.get("title", ""),
                "week_name": item.get("week_name", ""),
                "description": item.get("description", ""),
            }

            course_data["content"].append(content_item)

        prepared_data.append(course_data)

    return prepared_data


def process_course_with_ai(
    username: str,
    password: str,
    course: Dict[str, Any],
    guc_data: Dict[str, Any],
    days_ahead: int,
) -> List[Dict[str, Any]]:
    """
    Process a single course with AI to extract upcoming events.
    This function is meant to be used with concurrent.futures.

    Args:
        username: The username for authentication
        password: The password for authentication
        course: The course data
        guc_data: The GUC data
        days_ahead: Number of days ahead to consider for upcoming events

    Returns:
        List of upcoming events for this course
    """
    from scraping.cms import scrape_course_content, scrape_course_announcements
    from utils.helpers import normalize_course_url

    try:
        course_name = course.get("course_name", "Unknown")
        course_url = course.get("course_url", "")

        if not course_url:
            logger.warning(f"Missing course URL for {course_name}")
            return []

        normalized_url = normalize_course_url(course_url)

        # Fetch course content
        content_list = scrape_course_content(username, password, normalized_url)

        # Fetch course announcements
        announcement_result = scrape_course_announcements(
            username, password, normalized_url
        )

        # Prepare the data for the AI model
        course_data = {"course_name": course_name, "content": []}

        # Add content items
        if content_list:
            for item in content_list:
                content_item = {
                    "title": item.get("title", ""),
                    "week_name": item.get("week_name", ""),
                    "description": item.get("description", ""),
                }
                course_data["content"].append(content_item)

        # Add announcements
        if announcement_result and isinstance(announcement_result, dict):
            announcements = announcement_result.get("announcements", [])
            for announcement in announcements:
                announcement_item = {
                    "title": announcement.get("title", ""),
                    "content": announcement.get("content", ""),
                    "type": "announcement",
                }
                course_data["content"].append(announcement_item)

        # Use the AI model to extract events
        client = initialize_gemini_client()
        if not client:
            logger.error("Failed to initialize Gemini AI client")
            return []

        # Prepare the current date information
        current_date = datetime.now()
        cutoff_date = current_date + timedelta(days=days_ahead)

        # Create the prompt for the AI model
        prompt = f"""
        You are an AI assistant that extracts upcoming deadlines and events from university course data.

        Your task is to analyze the provided course data and extract all upcoming deadlines and events that occur between the current date ({current_date.strftime('%Y-%m-%d')}) and the cutoff date ({cutoff_date.strftime('%Y-%m-%d')}).

        IMPORTANT RULES:
        1. ONLY extract events that are actual deadlines or special sessions
        2. DO NOT include regular lectures or tutorials in your results
        3. DO include compensation lectures/tutorials (these are special make-up sessions)
        4. ONLY include events with clear dates mentioned
        5. DO NOT include past events

        Focus on extracting:
        1. Quizzes, assignments, exams, and projects with specific dates
        2. Compensation tutorials/lectures (explicitly mentioned as "compensation")
        3. Any other important academic deadlines (submissions, due dates)

        Completely ignore:
        1. Regular lectures and tutorials (unless explicitly marked as compensation sessions)
        2. Events without clear dates mentioned
        3. Past events
        4. Regular course materials or resources

        For each event, extract:
        - upcoming_title: A clear title for the event
        - upcoming_date: The date of the event in a readable format (e.g., "Monday, April 15, 2025")
        - upcoming_type: The type of event (Quiz, Exam, Assignment, Project, Compensation, Other)
        - upcoming_source: The source of the event (course name)
        - upcoming_description: A brief description of the event

        Here is the course data to analyze:
        Course Name: {course_data['course_name']}
        Content Items: {json.dumps(course_data['content'], default=str)}

        Return ONLY a JSON array of events in the specified format. Do not include any explanations or additional text.
        Each event must have all the required fields.
        If no valid events are found, return an empty array [].
        """

        # Generate content using the Gemini AI model with retries
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(
                    f"Attempt {attempt + 1} to call Gemini AI for {course_name}"
                )
                response = client.models.generate_content(
                    model="gemini-2.0-flash-lite",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1,  # Low temperature for more deterministic results
                    ),
                )

                # Parse the response
                if hasattr(response, "text"):
                    try:
                        events = json.loads(response.text)
                        logger.info(
                            f"Successfully extracted {len(events)} upcoming events for {course_name} using Gemini AI"
                        )
                        return events
                    except json.JSONDecodeError as e:
                        logger.error(
                            f"Failed to parse Gemini AI response as JSON for {course_name}: {e}",
                            exc_info=True,
                        )
                        logger.error(f"Response text: {response.text}")

                        # Try to extract JSON from the response if it contains extra text
                        try:
                            # Look for JSON array in the response
                            import re

                            json_match = re.search(
                                r"\[\s*{.*}\s*\]", response.text, re.DOTALL
                            )
                            if json_match:
                                events = json.loads(json_match.group(0))
                                logger.info(
                                    f"Successfully extracted {len(events)} upcoming events from partial JSON for {course_name}"
                                )
                                return events
                        except Exception:
                            pass

                        # If we're on the last attempt, return empty list
                        if attempt == MAX_RETRIES - 1:
                            return []
                else:
                    logger.error(
                        f"Gemini AI response does not contain text for {course_name}"
                    )
                    # If we're on the last attempt, return empty list
                    if attempt == MAX_RETRIES - 1:
                        return []

                # If we get here, we need to retry
                break

            except ServerError as e:
                logger.warning(
                    f"Server error from Gemini AI for {course_name} (attempt {attempt + 1}): {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    # Add jitter to backoff time to prevent thundering herd
                    backoff = BACKOFF_TIME * (2**attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {backoff:.2f} seconds...")
                    time.sleep(backoff)
                else:
                    logger.error(
                        f"Max retries exceeded for Gemini AI for {course_name}"
                    )
                    return []
            except Exception as e:
                logger.error(
                    f"Error calling Gemini AI for {course_name}: {e}", exc_info=True
                )
                # If we're on the last attempt, return empty list
                if attempt == MAX_RETRIES - 1:
                    return []
                # Add jitter to backoff time
                backoff = BACKOFF_TIME * (2**attempt) + random.uniform(0, 1)
                logger.info(f"Retrying in {backoff:.2f} seconds...")
                time.sleep(backoff)

    except Exception as e:
        logger.error(f"Error in process_course_with_ai: {e}", exc_info=True)
        return []
