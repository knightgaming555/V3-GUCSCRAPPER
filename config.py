# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Flask configuration variables."""

    # General Config
    SECRET_KEY = os.environ.get("SECRET_KEY", "a_default_secret_key_for_dev")
    DEBUG = os.environ.get("FLASK_DEBUG", "False").lower() in ("true", "1", "t")

    # Redis
    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    # Encryption
    ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
    if not ENCRYPTION_KEY:
        raise ValueError("ENCRYPTION_KEY environment variable is not set.")

    # Cache
    CACHE_DEFAULT_TIMEOUT = 18000  # Default cache timeout in seconds (1 hour)
    CACHE_LONG_TIMEOUT = 5184000  # Longer cache for semi-static data (e.g., schedule)
    CACHE_LONG_CMS_CONTENT_TIMEOUT = 18000  # CMS content cache timeout (1 hour)
    CACHE_STAFF_SCHEDULE_TIMEOUT = 18000

    # Admin / Secrets
    CACHE_REFRESH_SECRET = os.environ.get(
        "CACHE_REFRESH_SECRET", "default_refresh_secret"
    )
    ADMIN_SECRET = os.environ.get(
        "ADMIN_SECRET", "default_admin_secret"
    )  # For admin endpoints

    # GUC Specific URLs (Provide sensible defaults)
    GUC_INDEX_URL = os.environ.get(
        "GUC_INDEX_URL", "https://apps.guc.edu.eg/student_ext/index.aspx"
    )
    GUC_NOTIFICATIONS_URL = os.environ.get(
        "GUC_NOTIFICATIONS_URL",
        "https://apps.guc.edu.eg/student_ext/Main/Notifications.aspx",
    )
    BASE_SCHEDULE_URL = os.environ.get(
        "BASE_SCHEDULE_URL",
        "https://apps.guc.edu.eg/student_ext/Scheduling/GroupSchedule.aspx",
    )
    BASE_ATTENDANCE_URL = os.environ.get(
        "BASE_ATTENDANCE_URL",
        "https://apps.guc.edu.eg/student_ext/Attendance/ClassAttendance_ViewStudentAttendance_001.aspx",
    )
    BASE_GRADES_URL = os.environ.get(
        "BASE_GRADES_URL",
        "https://apps.guc.edu.eg/student_ext/Grade/CheckGrade_01.aspx",
    )
    BASE_EXAM_SEATS_URL = os.environ.get(
        "BASE_EXAM_SEATS_URL",
        "https://apps.guc.edu.eg/student_ext/Exam/ViewExamSeat_01.aspx",
    )
    BASE_CMS_URL = os.environ.get("BASE_CMS_URL", "https://cms.guc.edu.eg")
    CMS_HOME_URL = os.environ.get(
        "CMS_HOME_URL", "https://cms.guc.edu.eg/apps/student/HomePageStn.aspx"
    )

    GUC_DATA_URLS = [GUC_INDEX_URL, GUC_NOTIFICATIONS_URL]  # Used by guc_data scraper

    # Scraping Config
    VERIFY_SSL = os.environ.get("VERIFY_SSL", "True").lower() == "true"
    DEFAULT_REQUEST_TIMEOUT = 15  # Default timeout for individual requests (seconds)
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 2  # Base delay for retries (seconds)
    SCRAPE_TIMEOUT = 30  # Overall timeout for a full scraping operation (seconds)

    # Logging
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
    API_LOG_KEY = "api_logs"
    MAX_LOG_ENTRIES = 5000

    # Proxy / Extractor
    PROXY_CHUNK_SIZE = 262144  # 256KB
    PROXY_CACHE_CHUNK_SIZE = 1024 * 1024  # 1MB for Redis storage
    PROXY_CACHE_EXPIRY = 1800  # 30 minutes for proxied files

    # Default Dev Announcement (moved here for central config)
    DEFAULT_DEV_ANNOUNCEMENT = {
        "body": "Hello Unisight user,\n\nThank you for choosing Unisight. Our development team is working to improve your experience. We invite you to rate our app and share your feedback. Please use the link below to let us know your thoughts:\nhttps://forms.gle/Fm8sRmJbVx6utgFu8\n\nThank you for your support.",
        "date": "4/4/2025",  # Consider updating or making dynamic
        "email_time": "2025-03-27T00:00:00",
        "id": "150999",
        "importance": "High",
        "staff": "Unisight Team",
        "subject": "We'd love your feedback on Unisight",
        "title": "Rate our app",
    }
    REDIS_DEV_ANNOUNCEMENT_KEY = "dev_announcement"


# Create a singleton instance for easy access
config = Config()
