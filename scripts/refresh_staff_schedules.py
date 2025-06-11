# scripts/refresh_staff_schedules.py
import os
import sys
import logging
import time # For delays
from dotenv import load_dotenv

# --- Setup Paths and Load Env ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, ".env")) # Load .env for local runs

# --- Import Config and Utils ---
try:
    from config import config
    from utils.cache import set_in_cache 
    from scraping.authenticate import authenticate_user_session 
    # Import the new batch function and keep existing ones needed
    from scraping.staff_schedule_scraper import (
        get_global_staff_list_and_tokens, 
        # scrape_staff_schedule, # No longer used directly in the loop
        scrape_batch_staff_schedules, # New import
        _normalize_staff_name
    )
except ImportError as e:
    print(f"Error importing modules: {e}. Ensure all dependencies are installed and paths are correct.", file=sys.stderr)
    sys.exit(1)

# --- Logging Setup ---
log_level_str = os.environ.get("REFRESH_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout), 
        # Optional: Add a FileHandler if persistent logs for the script are needed
        # logging.FileHandler("refresh_staff_schedules.log", mode='a') 
    ],
)
logger = logging.getLogger("refresh_staff_schedules_batch") # Changed logger name slightly

# --- Constants ---
PREWARMED_STAFF_SCHEDULE_TIMEOUT_SECONDS = int(os.environ.get("PREWARM_STAFF_TTL_HOURS", "42")) * 60 * 60 # 42 hours default
STAFF_SCHEDULE_CACHE_PREFIX = "staff_schedule_PREWARM" 
DEFAULT_REQUEST_DELAY_SECONDS = 5 # Potentially increase default delay for batches
MAX_STAFF_TO_PROCESS_DEBUG = int(os.environ.get("MAX_STAFF_DEBUG", "0")) # 0 means process all
STAFF_BATCH_SIZE = int(os.environ.get("STAFF_REFRESH_BATCH_SIZE", "20")) # New constant for batch size

def refresh_all_staff_schedules():
    logger.info(f"Starting BATCH refresh process for all staff schedules. Batch size: {STAFF_BATCH_SIZE}")

    guc_username = os.environ.get("GUC_REFRESH_USERNAME")
    guc_password = os.environ.get("GUC_REFRESH_PASSWORD")

    if not guc_username or not guc_password:
        logger.critical("GUC_REFRESH_USERNAME or GUC_REFRESH_PASSWORD environment variables not set. Aborting.")
        sys.exit(1)
    
    logger.info(f"Using GUC username: {guc_username} for refreshing staff schedules.")

    logger.info("Attempting to authenticate GUC user...")
    session = authenticate_user_session(guc_username, guc_password)
    if not session:
        logger.critical("Failed to authenticate GUC user. Aborting refresh process.")
        sys.exit(1)
    logger.info("Successfully authenticated GUC user.")

    logger.info("Fetching global staff list and ASP tokens (force refresh)...")
    all_staff_details, asp_tokens = get_global_staff_list_and_tokens(session, force_refresh=True) 

    if not all_staff_details:
        logger.critical("Failed to retrieve the global staff list. Aborting refresh process.")
        sys.exit(1)
    if not asp_tokens:
        logger.critical("Failed to retrieve ASP tokens with the global staff list. Aborting.")
        sys.exit(1) 
    
    logger.info(f"Successfully retrieved {len(all_staff_details)} staff members and ASP tokens.")
    
    staff_id_to_name_map = {staff['id']: staff['name'] for staff in all_staff_details if 'id' in staff and 'name' in staff}

    staff_to_process_list = all_staff_details
    if MAX_STAFF_TO_PROCESS_DEBUG > 0:
        logger.warning(f"DEBUG MODE: Will only process up to {MAX_STAFF_TO_PROCESS_DEBUG} staff members.")
        staff_to_process_list = all_staff_details[:MAX_STAFF_TO_PROCESS_DEBUG]

    successful_scrapes = 0
    failed_scrapes = 0
    total_staff_targeted = len(staff_to_process_list)

    try:
        if hasattr(config, 'get') and callable(config.get):
            request_delay_seconds = config.get("REFRESH_SCRIPT_STAFF_DELAY_SECONDS", DEFAULT_REQUEST_DELAY_SECONDS)
        elif isinstance(config, dict) and "REFRESH_SCRIPT_STAFF_DELAY_SECONDS" in config:
             request_delay_seconds = config["REFRESH_SCRIPT_STAFF_DELAY_SECONDS"]
        else:
            request_delay_seconds = int(os.environ.get("REFRESH_SCRIPT_STAFF_DELAY_SECONDS", str(DEFAULT_REQUEST_DELAY_SECONDS)))
        logger.info(f"Using request delay from env/default: {request_delay_seconds}s per batch")
    except Exception:
        logger.warning(f"Could not retrieve REFRESH_SCRIPT_STAFF_DELAY_SECONDS. Using default {DEFAULT_REQUEST_DELAY_SECONDS}s per batch")
        request_delay_seconds = DEFAULT_REQUEST_DELAY_SECONDS

    num_batches = (total_staff_targeted + STAFF_BATCH_SIZE - 1) // STAFF_BATCH_SIZE
    logger.info(f"Total staff to process: {total_staff_targeted}, divided into {num_batches} batches of size {STAFF_BATCH_SIZE}.")

    for i in range(num_batches):
        batch_start_index = i * STAFF_BATCH_SIZE
        batch_end_index = min((i + 1) * STAFF_BATCH_SIZE, total_staff_targeted)
        current_batch_details = staff_to_process_list[batch_start_index:batch_end_index]
        
        if not current_batch_details:
            logger.info(f"Batch {i+1}/{num_batches} is empty. Skipping.")
            continue

        current_batch_ids = [staff['id'] for staff in current_batch_details if staff.get('id')]
        if not current_batch_ids:
            logger.warning(f"Batch {i+1}/{num_batches} contains no valid staff IDs. Skipping.")
            failed_scrapes += len(current_batch_details)
            continue

        logger.info(f"Processing Batch {i+1}/{num_batches} with {len(current_batch_ids)} staff IDs: {current_batch_ids[:3]}...")

        try:
            batch_schedules_data = scrape_batch_staff_schedules(session, current_batch_ids, asp_tokens)

            if not batch_schedules_data:
                logger.error(f"Batch {i+1}/{num_batches} (IDs: {current_batch_ids[:3]}...) failed to return any schedule data.")
                failed_scrapes += len(current_batch_ids)
            else:
                for staff_id_in_batch in current_batch_ids:
                    original_staff_name = staff_id_to_name_map.get(staff_id_in_batch, f"Unknown Name (ID: {staff_id_in_batch})")
                    
                    if staff_id_in_batch in batch_schedules_data:
                        schedule_data_for_staff = batch_schedules_data[staff_id_in_batch]
                        
                        if not schedule_data_for_staff:
                            logger.warning(f"No schedule content found for '{original_staff_name}' (ID: {staff_id_in_batch}) within the successful batch response.")
                            failed_scrapes +=1
                            continue

                        normalized_name_for_key = "_".join(_normalize_staff_name(original_staff_name).split())
                        cache_key = f"{STAFF_SCHEDULE_CACHE_PREFIX}_{normalized_name_for_key}"
                        
                        set_in_cache(cache_key, schedule_data_for_staff, timeout=PREWARMED_STAFF_SCHEDULE_TIMEOUT_SECONDS)
                        logger.info(f"Successfully scraped and cached schedule for '{original_staff_name}' (ID: {staff_id_in_batch}). Key: {cache_key}")
                        successful_scrapes += 1
                    else:
                        logger.error(f"Schedule for '{original_staff_name}' (ID: {staff_id_in_batch}) was expected but not found in batch response.")
                        failed_scrapes += 1
        
        except Exception as e:
            logger.error(f"Unexpected error processing batch {i+1}/{num_batches} (IDs: {current_batch_ids[:3]}...): {e}", exc_info=True)
            failed_scrapes += len(current_batch_ids)
        
        if i < num_batches - 1:
            if request_delay_seconds > 0:
                logger.debug(f"Waiting for {request_delay_seconds}s before next batch...")
                time.sleep(request_delay_seconds)

    logger.info("--- Staff Schedule Refresh Summary (Batch Mode) ---")
    logger.info(f"Total staff members targeted: {total_staff_targeted}")
    logger.info(f"Successfully scraped and cached: {successful_scrapes}")
    logger.info(f"Failed to scrape/cache: {failed_scrapes}")

    if total_staff_targeted == 0 and successful_scrapes == 0 and failed_scrapes == 0:
        logger.info("No staff members were processed (initial list might have been empty or filtered by MAX_STAFF_DEBUG=0).")
    elif failed_scrapes > 0:
        logger.warning(f"{failed_scrapes} staff schedules could not be refreshed. Check logs for details.")
        if failed_scrapes > total_staff_targeted * 0.5 and total_staff_targeted > 0:
             logger.error("More than 50% of staff schedule refreshes failed. Exiting with error.")
             sys.exit(1)
    else:
        logger.info("All targeted staff schedules refreshed successfully.")

# --- Script Entry Point ---
if __name__ == "__main__":
    print_separator = lambda: logger.info("=" * 80)
    
    print_separator()
    logger.info("           STARTING GUC STAFF SCHEDULE PRE-WARM SCRIPT (BATCH MODE)           ")
    print_separator()
    start_time = time.time()
    
    try:
        refresh_all_staff_schedules()
    except Exception as e:
        logger.critical(f"Critical unhandled error during script execution: {e}", exc_info=True)
        sys.exit(1)
    finally:
        end_time = time.time()
        logger.info(f"Script finished in {end_time - start_time:.2f} seconds.")
        print_separator()
        logger.info("            GUC STAFF SCHEDULE PRE-WARM SCRIPT (BATCH MODE) ENDED             ")
        print_separator()

        # Explicitly exit with 0 if no major errors caused an early sys.exit()
        # This is helpful if there were minor failures but not enough to warrant a script error code.
        # However, refresh_all_staff_schedules already sys.exit(1) on major credential/list failures or >50% scrape fails.
        # So, if we reach here, it means either success or minor failures.
        # No explicit sys.exit(0) needed as Python scripts exit with 0 by default on normal termination. 