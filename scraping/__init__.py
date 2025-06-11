# scraping/__init__.py
# Make core scraping functions easily importable

# Import specific functions you want to expose directly
from .authenticate import authenticate_user
from .guc_data import scrape_guc_data
from .schedule import scrape_schedule, filter_schedule_details
from .cms import (
    cms_scraper,
    scrape_course_content,
    scrape_course_announcements,
    scrape_cms_courses,
)
from .grades import scrape_grades
from .attendance import scrape_attendance
from .exams import scrape_exam_seats

# Optionally define __all__ to control `from scraping import *`
__all__ = [
    "authenticate_user",
    "scrape_guc_data",
    "scrape_schedule",
    "filter_schedule_details",
    "cms_scraper",
    "scrape_course_content",
    "scrape_course_announcements",
    "scrape_cms_courses",
    "scrape_grades",
    "scrape_attendance",
    "scrape_exam_seats",
]

# You can also import submodules if preferred
# from . import core
# from . import guc_data
# ... etc.
