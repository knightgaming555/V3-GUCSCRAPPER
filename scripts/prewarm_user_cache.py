# scripts/prewarm_user_cache.py
import os
import sys
import asyncio
import logging
from dotenv import load_dotenv

# --- Setup Paths and Load Env ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, ".env"))  # Load .env for local runs

# --- Import Config and Utils ---
try:
    from config import config
    from utils.auth import get_stored_password  # To get password from username
except ImportError as e:
    print(f"Error importing config/utils: {e}.", file=sys.stderr)
    sys.exit(1)

# --- Import Specific Scraping Functions ---
# Import only the functions needed for pre-warming
try:
    from scraping import (
        scrape_cms_courses,  # Sync (fetches course list)
        scrape_grades,  # Sync
        scrape_exam_seats,  # Sync
    )
except ImportError as e:
    print(f"Error importing scraping functions: {e}.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # Log to console for Actions
)
logger = logging.getLogger("prewarm_user_cache")

# --- Data types to prewarm ---
# We will run these scrapers, relying on their internal logic to cache results
DATA_TYPES_TO_PREWARM = {
    "cms_courses": scrape_cms_courses,
    "grades": scrape_grades,
    "exam_seats": scrape_exam_seats,
}


# --- Main Prewarm Logic ---
async def prewarm_user(username: str, password: str):
    """Runs scraping functions to populate cache for a specific user."""
    logger.info(f"Starting cache prewarm for user: {username}")
    results = {}
    tasks = []
    loop = asyncio.get_running_loop()

    for data_type, scrape_func in DATA_TYPES_TO_PREWARM.items():
        logger.debug(f"Scheduling prewarm task for {data_type}")
        # Run each synchronous scraper in a separate thread
        coro = asyncio.to_thread(scrape_func, username, password)
        task = asyncio.create_task(coro, name=f"{username}_{data_type}")
        tasks.append(task)

    # Await all scraping tasks
    logger.info(f"Awaiting {len(tasks)} prewarm tasks for {username}...")
    task_results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"Finished awaiting tasks for {username}.")

    # Log the outcome of each task (success or failure)
    for i, result_or_exc in enumerate(task_results):
        task_name = tasks[i].get_name()
        data_type = task_name.split("_")[-1]

        if isinstance(result_or_exc, Exception):
            logger.error(
                f"Prewarm task {task_name} failed with exception: {result_or_exc}",
                exc_info=False,
            )
            results[data_type] = "failed"
        elif isinstance(result_or_exc, dict) and "error" in result_or_exc:
            logger.warning(
                f"Prewarm task {task_name} scraper returned error: {result_or_exc['error']}"
            )
            results[data_type] = "scraper_error"
        elif result_or_exc is None:
            logger.warning(f"Prewarm task {task_name} scraper returned None.")
            results[data_type] = "scraper_none"
        else:
            # Scraper succeeded (data is returned but we don't use it, caching happened inside scraper)
            logger.info(
                f"Prewarm task {task_name} completed successfully (cache updated by scraper)."
            )
            results[data_type] = "success"

    logger.info(f"Prewarm Results for {username}: {results}")
    # Check if any task failed
    if any(status != "success" for status in results.values()):
        logger.error(f"One or more prewarm tasks failed for {username}.")
        # Exit with error code for GitHub Actions
        sys.exit(1)
    else:
        logger.info(f"All prewarm tasks completed successfully for {username}.")


# --- Script Entry Point ---
if __name__ == "__main__":
    # --- Argument Parsing ---
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <username>", file=sys.stderr)
        print("  <username>: The GUC username to prewarm cache for.", file=sys.stderr)
        print(
            "  Password should be provided via USER_PASSWORD environment variable.",
            file=sys.stderr,
        )
        sys.exit(1)

    target_username = sys.argv[1]

    # --- Get Password from Environment Variable ---
    # In GitHub Actions, this will be set from the secret.
    # For local testing, set it in your terminal: export USER_PASSWORD='your_password'
    # Or add it temporarily to your .env file (less secure)
    user_password = os.environ.get("USER_PASSWORD")

    if not user_password:
        # Attempt to get from stored credentials as a fallback (requires Redis & key)
        logger.warning(
            "USER_PASSWORD env var not set. Attempting fallback to stored credentials."
        )
        user_password = get_stored_password(target_username)
        if not user_password:
            logger.critical(
                f"Could not find password for {target_username} via env var or stored credentials."
            )
            sys.exit(1)
        else:
            logger.info("Using password from stored credentials.")
    else:
        logger.info("Using password from USER_PASSWORD environment variable.")

    # --- Run the Prewarm Task ---
    try:
        asyncio.run(prewarm_user(target_username, user_password))
    except Exception as e:
        logger.critical(f"Critical error during script execution: {e}", exc_info=True)
        sys.exit(1)
