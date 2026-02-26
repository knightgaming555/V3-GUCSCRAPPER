import json
import logging
import os
import re
import sys
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from openai import OpenAI


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, ".env"))

from config import config
from utils.auth import get_all_stored_users_decrypted
from utils.cache import generate_cache_key, set_in_cache
from utils.helpers import get_version_number_cached


logging.basicConfig(
    level=getattr(config, "LOG_LEVEL", logging.INFO),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("refresh_ai_deadlines_cache")


TARGET_USERS = ("mohamed.elsaadi", "seif.elkady")
AI_DEADLINES_CACHE_PREFIX = "ai_upcoming_deadlines"
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openrouter/free")
MAX_RETRIES = 3
REQUEST_TIMEOUT = 45
DEFAULT_API_BASE_URL = "https://v3-gucscrapper.vercel.app"


def _resolve_api_base_url() -> str:
    raw = (os.getenv("UNISIGHT_API_BASE_URL") or "").strip()
    if not raw:
        return DEFAULT_API_BASE_URL
    if not re.match(r"^https?://", raw, flags=re.IGNORECASE):
        raw = f"https://{raw.lstrip('/')}"
    return raw.rstrip("/")


API_BASE_URL = _resolve_api_base_url()


def get_season_weight(season_str):
    if not season_str:
        return 0
    match = re.search(r"([A-Za-z]+)\s+(\d{4})", season_str)
    if not match:
        return 0
    season, year = match.groups()
    season = season.lower()
    weights = {"winter": 1, "spring": 2, "summer": 3, "fall": 4, "autumn": 4}
    return (int(year) * 10) + weights.get(season, 0)


def clean_text_data(data):
    if isinstance(data, str):
        text = re.sub(r"<[^>]+>", " ", data)
        return " ".join(text.split())
    if isinstance(data, dict):
        return {k: clean_text_data(v) for k, v in data.items()}
    if isinstance(data, list):
        return [clean_text_data(item) for item in data]
    return data


def _get_json(path, params):
    response = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _build_system_prompt(course_names_list, current_date_str):
    return f"""You are a highly reliable, strict academic data extraction AI.

YOUR STRICT RULES:
1. TWO DISTINCT SOURCES: You are receiving data from TWO sources:
   - Source A: STUDENT NOTIFICATIONS.
   - Source B: COURSE CONTENT.
   You MUST thoroughly scan BOTH sources. Do not ignore the notifications. Do not ignore the course content. Extract from both.
2. ZERO HALLUCINATION POLICY: Extract ONLY from the provided text. Never invent dates or tasks.
3. VALID COURSES ONLY: Here is the strict list of valid courses: {json.dumps(course_names_list)}. Every event MUST have the "course" field filled with one of these exact names. Match assignments found in the notifications to the closest course name in this list.
4. EXTRACT UPCOMING TASKS: Quizzes, homework, assignments, projects, exams.
5. DATE HANDLING: If a deadline says "the week starting 28th of February", record exactly "Week starting Feb 28". Use today's date ({current_date_str}) to ignore past events.
6. JSON ONLY: Return ONLY a valid JSON object. No conversational text. No markdown blocks.
If there are no upcoming events, return exactly: {{"upcoming_events": []}}

REQUIRED JSON FORMAT:
{{
  "upcoming_events": [
    {{
      "course": "Exact Course Name",
      "type": "Assignment / Quiz / Exam / Project / Announcement",
      "title": "Exact title",
      "due_date": "Extracted date or timeframe",
      "details": "Brief summary"
    }}
  ]
}}"""


def _extract_upcoming_events_for_user(client, username, password, version_number):
    auth_params = {
        "username": username,
        "password": password,
        "version_number": version_number,
    }

    logger.info(f"[{username}] Fetching GUC notifications...")
    raw_guc = _get_json("/api/guc_data", auth_params)
    notifications_only = raw_guc.get("notifications", raw_guc)
    clean_notifications = clean_text_data(notifications_only)

    logger.info(f"[{username}] Fetching CMS courses...")
    cms_courses = _get_json("/api/cms_data", auth_params)
    if not cms_courses:
        return {"upcoming_events": []}

    max_weight = max(get_season_weight(course.get("season_name", "")) for course in cms_courses)
    latest_courses = [c for c in cms_courses if get_season_weight(c.get("season_name", "")) == max_weight]
    course_names_list = [course.get("course_name", "Unknown Course") for course in latest_courses]

    logger.info(f"[{username}] Fetching latest CMS content ({len(latest_courses)} courses)...")
    course_details = {}
    for course in latest_courses:
        c_name = course.get("course_name")
        c_url = course.get("course_url")
        if not c_name or not c_url:
            continue
        content_params = {**auth_params, "course_url": c_url}
        try:
            course_details[c_name] = clean_text_data(_get_json("/api/cms_content", content_params))
        except Exception as content_exc:
            logger.warning(f"[{username}] Failed to fetch CMS content for {c_name}: {content_exc}")

    current_date_str = datetime.now().strftime("%A, %B %d, %Y")
    system_prompt = _build_system_prompt(course_names_list, current_date_str)
    user_payload = (
        f"Today's actual date is: {current_date_str}.\n\n"
        f"--- SOURCE A: STUDENT NOTIFICATIONS ---\n{json.dumps(clean_notifications, separators=(',', ':'))}\n\n"
        f"--- SOURCE B: CURRENT SEMESTER COURSE CONTENT ---\n{json.dumps(course_details, separators=(',', ':'))}"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[{username}] OpenRouter request attempt {attempt}/{MAX_RETRIES}...")
            response = client.chat.completions.create(
                model=OPENROUTER_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_payload},
                ],
                temperature=0.1,
            )
            raw_output = (response.choices[0].message.content or "").strip()
            if not raw_output:
                raise ValueError("Model returned an empty response.")
            if raw_output.startswith("```"):
                raw_output = re.sub(r"^```[a-zA-Z]*\n", "", raw_output)
                raw_output = re.sub(r"\n```$", "", raw_output)

            parsed = json.loads(raw_output)
            if not isinstance(parsed, dict):
                raise ValueError("Model output is not a JSON object.")
            if "upcoming_events" not in parsed or not isinstance(parsed.get("upcoming_events"), list):
                parsed = {"upcoming_events": []}
            return parsed
        except Exception as model_exc:
            logger.warning(f"[{username}] AI parsing attempt failed: {model_exc}")
            if attempt < MAX_RETRIES:
                time.sleep(2)

    return {"upcoming_events": []}


def _cache_result(username, payload):
    cache_key = generate_cache_key(AI_DEADLINES_CACHE_PREFIX, username)
    wrapped_payload = {
        "generated_at": datetime.utcnow().isoformat(),
        "result": payload,
    }
    return set_in_cache(cache_key, wrapped_payload, timeout=config.CACHE_DEFAULT_TIMEOUT)


def main():
    if not OPENROUTER_API_KEY:
        logger.error("OPENROUTER_API_KEY/OPENAI_API_KEY is missing. Aborting.")
        sys.exit(1)

    version_number = get_version_number_cached()
    if version_number in ("Error Fetching", "Redis Unavailable"):
        version_number = os.getenv("VERSION_NUMBER", "").strip()
    if not version_number:
        logger.error("Could not determine a valid version number. Aborting.")
        sys.exit(1)

    all_users = get_all_stored_users_decrypted()
    if not all_users:
        logger.error("No stored credentials found.")
        sys.exit(1)

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=OPENROUTER_API_KEY,
    )

    succeeded = 0
    for username in TARGET_USERS:
        password = all_users.get(username)
        if not password or password == "DECRYPTION_ERROR":
            logger.error(f"[{username}] Missing/decryption-error credentials. Skipping.")
            continue

        try:
            result = _extract_upcoming_events_for_user(client, username, password, version_number)
            if _cache_result(username, result):
                logger.info(f"[{username}] AI upcoming deadlines cached successfully.")
                succeeded += 1
            else:
                logger.error(f"[{username}] Failed to write AI upcoming deadlines cache.")
        except Exception as exc:
            logger.error(f"[{username}] Unhandled failure: {exc}", exc_info=True)

    if succeeded == 0:
        logger.error("No user cache entries were updated.")
        sys.exit(1)

    logger.info(f"Completed AI deadline cache refresh for {succeeded}/{len(TARGET_USERS)} target users.")


if __name__ == "__main__":
    main()
