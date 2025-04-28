# utils/date_parser.py
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Common date formats to try when parsing
DATE_FORMATS = [
    # Standard formats
    "%Y-%m-%d",  # 2023-01-15
    "%d-%m-%Y",  # 15-01-2023
    "%m-%d-%Y",  # 01-15-2023
    "%d/%m/%Y",  # 15/01/2023
    "%m/%d/%Y",  # 01/15/2023
    "%Y/%m/%d",  # 2023/01/15
    # With time
    "%Y-%m-%d %H:%M",  # 2023-01-15 14:30
    "%d-%m-%Y %H:%M",  # 15-01-2023 14:30
    "%m-%d-%Y %H:%M",  # 01-15-2023 14:30
    "%d/%m/%Y %H:%M",  # 15/01/2023 14:30
    "%m/%d/%Y %H:%M",  # 01/15/2023 14:30
    # Month name formats
    "%d %B %Y",  # 15 January 2023
    "%d %b %Y",  # 15 Jan 2023
    "%B %d, %Y",  # January 15, 2023
    "%b %d, %Y",  # Jan 15, 2023
    # Month name with time
    "%d %B %Y %H:%M",  # 15 January 2023 14:30
    "%d %b %Y %H:%M",  # 15 Jan 2023 14:30
    "%B %d, %Y %H:%M",  # January 15, 2023 14:30
    "%b %d, %Y %H:%M",  # Jan 15, 2023 14:30
    # Special formats for GUC
    "%d - %B - %Y",  # 15 - January - 2023
    "%d - %b - %Y",  # 15 - Jan - 2023
    # Non-standard formats (with hyphens or dots)
    "%d-%m-%y",  # 15-01-23
    "%m-%d-%y",  # 01-15-23
    "%d.%m.%Y",  # 15.01.2023
    "%m.%d.%Y",  # 01.15.2023
    "%Y.%m.%d",  # 2023.01.15
]

# Month name mapping for custom parsing
MONTH_NAMES = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Attempts to parse a date string in various formats.
    Returns a datetime object if successful, None otherwise.
    """
    if not date_str or not isinstance(date_str, str):
        return None

    # Clean the date string
    cleaned_date = date_str.strip().replace("\r", "").replace("\n", " ")

    # Try standard formats first
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned_date, fmt)
        except ValueError:
            continue

    # Try custom parsing for non-standard formats
    return parse_custom_date_format(cleaned_date)


def parse_custom_date_format(date_str: str) -> Optional[datetime]:
    """
    Attempts to parse non-standard date formats using regex and custom logic.
    Returns a datetime object if successful, None otherwise.
    """
    # Clean and normalize the string
    date_str = date_str.lower().strip()

    # Try to extract day, month, year using regex patterns

    # Pattern for formats like "15-6-2-205" (day-month-day-year with typo)
    pattern1 = r"(\d{1,2})[/-](\d{1,2})[/-](\d{1,2})[/-](\d{2,4})"
    match = re.search(pattern1, date_str)
    if match:
        try:
            day, month, day2, year = map(int, match.groups())
            # Assume the second "day" is part of the year
            if year < 100:
                year += 2000  # Assume 21st century for two-digit years
            return datetime(year, month, day)
        except (ValueError, OverflowError):
            pass

    # Pattern for "day month year" format (e.g., "15 June 2023")
    pattern2 = r"(\d{1,2})(?:st|nd|rd|th)?\s+([a-zA-Z]+)(?:\s+|,\s+)(\d{2,4})"
    match = re.search(pattern2, date_str)
    if match:
        try:
            day, month_name, year = match.groups()
            day = int(day)
            month_name = month_name.lower()
            year = int(year)

            if month_name in MONTH_NAMES:
                month = MONTH_NAMES[month_name]
                if year < 100:
                    year += 2000  # Assume 21st century for two-digit years
                return datetime(year, month, day)
        except (ValueError, OverflowError):
            pass

    # Pattern for "month day, year" format (e.g., "June 15, 2023")
    pattern3 = r"([a-zA-Z]+)(?:\s+|,\s+)(\d{1,2})(?:st|nd|rd|th)?(?:\s+|,\s+)(\d{2,4})"
    match = re.search(pattern3, date_str)
    if match:
        try:
            month_name, day, year = match.groups()
            month_name = month_name.lower()
            day = int(day)
            year = int(year)

            if month_name in MONTH_NAMES:
                month = MONTH_NAMES[month_name]
                if year < 100:
                    year += 2000  # Assume 21st century for two-digit years
                return datetime(year, month, day)
        except (ValueError, OverflowError):
            pass

    # Pattern for dates with just numbers separated by delimiters
    pattern4 = r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})"
    match = re.search(pattern4, date_str)
    if match:
        try:
            # Try both DMY and MDY formats
            g1, g2, g3 = map(int, match.groups())

            # Adjust year if it's a 2-digit year
            if g3 < 100:
                g3 += 2000  # Assume 21st century

            # Try day-month-year
            if 1 <= g1 <= 31 and 1 <= g2 <= 12:
                try:
                    return datetime(g3, g2, g1)
                except (ValueError, OverflowError):
                    pass

            # Try month-day-year
            if 1 <= g2 <= 31 and 1 <= g1 <= 12:
                try:
                    return datetime(g3, g1, g2)
                except (ValueError, OverflowError):
                    pass
        except (ValueError, OverflowError):
            pass

    # If all parsing attempts fail
    return None


def is_future_date(date_obj: Optional[datetime], days_ahead: int = 30) -> bool:
    """
    Checks if a date is in the future within the specified number of days.
    Returns True if the date is in the future and within the days_ahead limit.
    """
    if not date_obj:
        return False

    now = datetime.now()

    # If the date has no time component, set it to end of day
    if date_obj.hour == 0 and date_obj.minute == 0 and date_obj.second == 0:
        date_obj = date_obj.replace(hour=23, minute=59, second=59)

    # Check if the date is in the future
    if date_obj <= now:
        return False

    # Check if the date is within the days_ahead limit
    max_date = now + timedelta(days=days_ahead)
    return date_obj <= max_date


def extract_date_from_week(
    week_number: int, day_of_week: str = None
) -> Optional[datetime]:
    """
    Calculates a date based on a week number in the semester.
    Assumes the semester starts on April 1, 2025.
    Returns a datetime object for the specified week.
    """
    # Define semester start date
    semester_start = datetime(2025, 4, 1)  # April 1, 2025

    # Calculate the date for the beginning of the specified week
    week_start = semester_start + timedelta(days=(week_number - 1) * 7)

    # If a specific day of the week is provided, adjust to that day
    if day_of_week:
        days = {
            "monday": 0,
            "mon": 0,
            "tuesday": 1,
            "tue": 1,
            "tues": 1,
            "wednesday": 2,
            "wed": 2,
            "thursday": 3,
            "thu": 3,
            "thurs": 3,
            "friday": 4,
            "fri": 4,
            "saturday": 5,
            "sat": 5,
            "sunday": 6,
            "sun": 6,
        }

        day_idx = days.get(day_of_week.lower(), 0)
        current_day_idx = week_start.weekday()
        days_to_add = (day_idx - current_day_idx) % 7
        return week_start + timedelta(days=days_to_add)

    return week_start


def extract_date_from_text(text: str) -> Optional[datetime]:
    """
    Attempts to extract a date from a text string.
    Looks for common date patterns and tries to parse them.
    Returns the first valid date found, or None if no date is found.
    """
    if not text or not isinstance(text, str):
        return None

    # Clean the text
    text = text.replace("\r", " ").replace("\n", " ")

    # Special case for compensation lectures/tutorials with specific dates
    compensation_pattern = r"(?:compensation|make-up|makeup|rescheduled).*?(?:will be held on|on|this|next|coming)\s+([A-Za-z]+day)(?:[,\s]+(\d{1,2})(?:st|nd|rd|th)?(?:\s+[A-Za-z]+)?|\s+\((\d{1,2}/\d{1,2}/\d{2,4})\))?"
    comp_match = re.search(compensation_pattern, text, re.IGNORECASE)
    if comp_match:
        day_of_week = comp_match.group(1)
        day_num = comp_match.group(2) if comp_match.group(2) else None
        date_str = comp_match.group(3) if comp_match.group(3) else None

        if date_str:  # If we have a date in parentheses like (28/4/2025)
            date_obj = parse_date(date_str)
            if date_obj:
                return date_obj

        if day_of_week:  # If we have a day of week like "Monday"
            # Map day of week to a date in the near future
            days = {
                "monday": 0,
                "mon": 0,
                "tuesday": 1,
                "tue": 1,
                "tues": 1,
                "wednesday": 2,
                "wed": 2,
                "thursday": 3,
                "thu": 3,
                "thurs": 3,
                "friday": 4,
                "fri": 4,
                "saturday": 5,
                "sat": 5,
                "sunday": 6,
                "sun": 6,
            }

            day_idx = days.get(day_of_week.lower(), -1)
            if day_idx >= 0:
                # Find the next occurrence of this day
                now = datetime.now()
                days_ahead = (day_idx - now.weekday()) % 7
                if days_ahead == 0:  # Today
                    days_ahead = 7  # Next week
                next_day = now + timedelta(days=days_ahead)

                # If we also have a day number, use it to set the day of month
                if day_num:
                    try:
                        day = int(day_num)
                        # Try to create a date with the same month and year but specified day
                        try:
                            return datetime(next_day.year, next_day.month, day)
                        except (ValueError, OverflowError):
                            # If that fails, it might be for next month
                            if next_day.month == 12:
                                return datetime(next_day.year + 1, 1, day)
                            else:
                                return datetime(next_day.year, next_day.month + 1, day)
                    except (ValueError, OverflowError):
                        pass

                return next_day

    # Special case for "will be on Wed May 7th" format
    day_month_pattern = r"\b(?:on|for|at|by)\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:uary|ruary|ch|il|e|y|ust|tember|ober|ember)?\s+(\d{1,2})(?:st|nd|rd|th)?\b"
    day_month_matches = re.findall(day_month_pattern, text, re.IGNORECASE)

    if day_month_matches:
        # Extract month and day
        month_match = re.search(
            r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:uary|ruary|ch|il|e|y|ust|tember|ober|ember)?\b",
            text,
            re.IGNORECASE,
        )
        if month_match:
            month_name = month_match.group(1).lower()
            month_num = {
                "jan": 1,
                "feb": 2,
                "mar": 3,
                "apr": 4,
                "may": 5,
                "jun": 6,
                "jul": 7,
                "aug": 8,
                "sep": 9,
                "oct": 10,
                "nov": 11,
                "dec": 12,
            }.get(month_name[:3].lower())

            if month_num and day_month_matches[0]:
                day = int(day_month_matches[0])
                # Assume current year, or next year if the date is in the past
                year = datetime.now().year
                try:
                    date_obj = datetime(year, month_num, day)
                    if date_obj < datetime.now():
                        date_obj = datetime(year + 1, month_num, day)
                    return date_obj
                except (ValueError, OverflowError):
                    pass

    # Special case for "this coming Sunday, April 27th" format
    coming_day_pattern = r"(?:this|next|coming)\s+([A-Za-z]+day),?\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?"
    coming_match = re.search(coming_day_pattern, text, re.IGNORECASE)
    if coming_match:
        day_of_week = coming_match.group(1)
        month_name = coming_match.group(2)
        day = coming_match.group(3)

        month_num = {
            "january": 1,
            "jan": 1,
            "february": 2,
            "feb": 2,
            "march": 3,
            "mar": 3,
            "april": 4,
            "apr": 4,
            "may": 5,
            "june": 6,
            "jun": 6,
            "july": 7,
            "jul": 7,
            "august": 8,
            "aug": 8,
            "september": 9,
            "sep": 9,
            "sept": 9,
            "october": 10,
            "oct": 10,
            "november": 11,
            "nov": 11,
            "december": 12,
            "dec": 12,
        }.get(month_name.lower(), 0)

        if month_num and day:
            try:
                day_num = int(day)
                # Assume current year, or next year if the date is in the past
                year = datetime.now().year
                try:
                    date_obj = datetime(year, month_num, day_num)
                    if date_obj < datetime.now():
                        date_obj = datetime(year + 1, month_num, day_num)
                    return date_obj
                except (ValueError, OverflowError):
                    pass
            except (ValueError, OverflowError):
                pass

    # Special case for specific date mentions like "will be held on Wednesday, 23 April"
    specific_date_pattern = (
        r"will be held on\s+([A-Za-z]+day),?\s+(\d{1,2})\s+([A-Za-z]+)"
    )
    specific_match = re.search(specific_date_pattern, text, re.IGNORECASE)
    if specific_match:
        day_of_week = specific_match.group(1)
        day = specific_match.group(2)
        month_name = specific_match.group(3)

        month_num = {
            "january": 1,
            "jan": 1,
            "february": 2,
            "feb": 2,
            "march": 3,
            "mar": 3,
            "april": 4,
            "apr": 4,
            "may": 5,
            "june": 6,
            "jun": 6,
            "july": 7,
            "jul": 7,
            "august": 8,
            "aug": 8,
            "september": 9,
            "sep": 9,
            "sept": 9,
            "october": 10,
            "oct": 10,
            "november": 11,
            "nov": 11,
            "december": 12,
            "dec": 12,
        }.get(month_name.lower(), 0)

        if month_num and day:
            try:
                day_num = int(day)
                # Assume current year, or next year if the date is in the past
                year = datetime.now().year
                try:
                    date_obj = datetime(year, month_num, day_num)
                    if date_obj < datetime.now():
                        date_obj = datetime(year + 1, month_num, day_num)
                    return date_obj
                except (ValueError, OverflowError):
                    pass
            except (ValueError, OverflowError):
                pass

    # Special case for "Week X" format (e.g., "Quiz 3 due Week 9")
    week_pattern = r"(?:week|wk)\s+(\d+)"
    week_match = re.search(week_pattern, text, re.IGNORECASE)
    if week_match:
        try:
            week_num = int(week_match.group(1))
            # Use our week-based date extraction function
            return extract_date_from_week(week_num)
        except (ValueError, OverflowError):
            pass

    # Look for date patterns
    date_patterns = [
        # Specific GUC Data formats
        r"(?:Date|Time|When):\s*([^\n,]+)",
        r"will be held on[:\s]*([^\n,]+)",
        r"on\s+([A-Za-z]+day),?\s+([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?)",
        r"on\s+([A-Za-z]+day)\s+\((\d{1,2}/\d{1,2}/\d{2,4})\)",
        # DD/MM/YYYY or MM/DD/YYYY
        r"\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b",
        # Month name patterns
        r"\b\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)(?:\s+|,\s+)\d{2,4}\b",
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)(?:\s+|,\s+)\d{1,2}(?:st|nd|rd|th)?(?:\s+|,\s+)\d{2,4}\b",
        # Special GUC format
        r"\b\d{1,2}\s*-\s*(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s*-\s*\d{2,4}\b",
        # Day and month only (e.g., "20th May")
        r"\b\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\b",
        # Month and day only (e.g., "May 20th")
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{1,2}(?:st|nd|rd|th)?\b",
        # Week range format (e.g., "Week 10 Tue 20th May – Mon 26th May")
        r"\bWeek\s+\d+\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\s+\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:uary|ruary|ch|il|e|y|ust|tember|ober|ember)?\b",
        # Specific course announcement formats
        r"Quiz\s+\d+\s+will\s+be\s+on\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:uary|ruary|ch|il|e|y|ust|tember|ober|ember)?\s+\d{1,2}(?:st|nd|rd|th)?",
        r"Week\s+\d+\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\s+\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:uary|ruary|ch|il|e|y|ust|tember|ober|ember)?(?:\s+[–-]\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?\s+\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:uary|ruary|ch|il|e|y|ust|tember|ober|ember)?)?",
        r"Quiz\s+\d+\s+due\s+Week\s+\d+",
    ]

    for pattern in date_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            # Handle patterns with multiple groups
            if len(match.groups()) > 1:
                # For patterns like "on Monday, April 27th"
                if match.group(1) and match.group(2):
                    date_str = match.group(
                        2
                    )  # Use the second group (the actual date part)
                    date_obj = parse_date(date_str)
                    if date_obj:
                        return date_obj
            else:
                # For single group patterns
                date_str = match.group(1) if match.groups() else match.group(0)
                date_obj = parse_date(date_str)
                if date_obj:
                    return date_obj

    # If no date is found using patterns, try parsing the entire text
    return parse_date(text)


def format_date_for_display(date_obj: Optional[datetime]) -> str:
    """
    Formats a datetime object for display in a user-friendly format.
    Returns a string in the format "DD-MM-YYYY".
    """
    if not date_obj:
        return "Date not specified"

    return date_obj.strftime("%d-%m-%Y")
