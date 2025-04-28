"""
Gemini AI integration for extracting upcoming events from GUC data and CMS content.
This module provides a unified approach that processes all courses at once.
"""
import logging
import json
import time
import random
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from google import genai
from google.genai import types
from google.genai.errors import ServerError

logger = logging.getLogger(__name__)

# Initialize the Gemini AI client with the API key
API_KEY = "AIzaSyAzSzm1L2ECUy_5Dm5hkMnvB-hozyMw5RI"

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

def extract_upcoming_events_unified(
    username: str,
    password: str,
    courses: List[Dict[str, Any]],
    guc_data: Dict[str, Any],
    days_ahead: int = 30
) -> List[Dict[str, Any]]:
    """
    Extract upcoming events from GUC data and CMS content using Gemini AI.
    Processes all courses in a single request to avoid overloading the model.
    
    Args:
        username: The username for authentication
        password: The password for authentication
        courses: List of CMS courses
        guc_data: The GUC data containing notifications
        days_ahead: Number of days ahead to consider for upcoming events
    
    Returns:
        List of upcoming events in the required format
    """
    from scraping.cms import scrape_course_content, scrape_course_announcements
    from utils.helpers import normalize_course_url
    
    try:
        client = initialize_gemini_client()
        if not client:
            logger.error("Failed to initialize Gemini AI client")
            return []
        
        # Prepare the current date information
        current_date = datetime.now()
        cutoff_date = current_date + timedelta(days=days_ahead)
        
        # Prepare GUC notifications
        guc_notifications = _prepare_guc_notifications(guc_data)
        
        # Gather all course data
        all_course_data = []
        for course in courses:
            course_name = course.get("course_name", "Unknown")
            course_url = course.get("course_url", "")
            
            if not course_url:
                logger.warning(f"Missing course URL for {course_name}")
                continue
            
            try:
                normalized_url = normalize_course_url(course_url)
                
                # Fetch course content
                content_list = scrape_course_content(username, password, normalized_url)
                
                # Fetch course announcements
                announcement_result = scrape_course_announcements(username, password, normalized_url)
                
                # Prepare the data for the AI model
                course_data = {
                    "course_name": course_name,
                    "content": []
                }
                
                # Add content items
                if content_list:
                    for item in content_list:
                        content_item = {
                            "title": item.get("title", ""),
                            "week_name": item.get("week_name", ""),
                            "description": item.get("description", "")
                        }
                        course_data["content"].append(content_item)
                
                # Add announcements
                if announcement_result and isinstance(announcement_result, dict):
                    announcements = announcement_result.get("announcements", [])
                    for announcement in announcements:
                        announcement_item = {
                            "title": announcement.get("title", ""),
                            "content": announcement.get("content", ""),
                            "type": "announcement"
                        }
                        course_data["content"].append(announcement_item)
                
                all_course_data.append(course_data)
                
            except Exception as e:
                logger.error(f"Error processing course {course_name}: {e}", exc_info=True)
        
        # Prepare the input for the AI model
        input_data = {
            "current_date": current_date.isoformat(),
            "cutoff_date": cutoff_date.isoformat(),
            "guc_notifications": guc_notifications,
            "cms_courses": all_course_data
        }
        
        # Create the prompt for the AI model
        prompt = f"""
        You are an AI assistant that extracts upcoming deadlines and events from university data.
        
        Your task is to analyze the provided data and extract all upcoming deadlines and events that occur between the current date ({current_date.strftime('%Y-%m-%d')}) and the cutoff date ({cutoff_date.strftime('%Y-%m-%d')}).
        
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
        - upcoming_source: The source of the event (course name or department)
        - upcoming_description: A brief description of the event
        
        Return ONLY a JSON array of events in the specified format. Do not include any explanations or additional text.
        Each event must have all the required fields.
        If no valid events are found, return an empty array [].
        """
        
        # Generate content using the Gemini AI model with retries
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Attempt {attempt + 1} to call Gemini AI with unified approach")
                
                # Send the data to the AI model
                response = client.models.generate_content(
                    model="gemini-2.0-flash-lite",
                    contents=[
                        prompt,
                        {
                            "role": "user",
                            "parts": [{"text": json.dumps(input_data, default=str)}]
                        }
                    ],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1,  # Low temperature for more deterministic results
                    )
                )
                
                # Parse the response
                if hasattr(response, 'text'):
                    try:
                        events = json.loads(response.text)
                        logger.info(f"Successfully extracted {len(events)} upcoming events using unified Gemini AI approach")
                        return events
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse Gemini AI response as JSON: {e}", exc_info=True)
                        logger.error(f"Response text: {response.text}")
                        
                        # Try to extract JSON from the response if it contains extra text
                        try:
                            # Look for JSON array in the response
                            json_match = re.search(r'\[\s*{.*}\s*\]', response.text, re.DOTALL)
                            if json_match:
                                events = json.loads(json_match.group(0))
                                logger.info(f"Successfully extracted {len(events)} upcoming events from partial JSON")
                                return events
                        except Exception:
                            pass
                        
                        # If we're on the last attempt, return empty list
                        if attempt == MAX_RETRIES - 1:
                            return []
                else:
                    logger.error("Gemini AI response does not contain text")
                    # If we're on the last attempt, return empty list
                    if attempt == MAX_RETRIES - 1:
                        return []
                
                # If we get here, we need to retry
                break
                
            except ServerError as e:
                logger.warning(f"Server error from Gemini AI (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    # Add jitter to backoff time to prevent thundering herd
                    backoff = BACKOFF_TIME * (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {backoff:.2f} seconds...")
                    time.sleep(backoff)
                else:
                    logger.error("Max retries exceeded for Gemini AI")
                    return []
            except Exception as e:
                logger.error(f"Error calling Gemini AI: {e}", exc_info=True)
                # If we're on the last attempt, return empty list
                if attempt == MAX_RETRIES - 1:
                    return []
                # Add jitter to backoff time
                backoff = BACKOFF_TIME * (2 ** attempt) + random.uniform(0, 1)
                logger.info(f"Retrying in {backoff:.2f} seconds...")
                time.sleep(backoff)
            
    except Exception as e:
        logger.error(f"Error in extract_upcoming_events_unified: {e}", exc_info=True)
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
        notifications.append({
            "title": notification.get("title", ""),
            "subject": notification.get("subject", ""),
            "body": notification.get("body", ""),
            "date": date_str,
            "staff": notification.get("staff", ""),
            "department": notification.get("department", "")
        })
    
    return notifications
