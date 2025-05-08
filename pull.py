#!/usr/bin/env python3
import logging
import re
import time
import json
import os
from atproto import Client
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from categorize import categorize_text, set_openai_api_key

# === Absolute base path (for cron safety) ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# === Editable Throttling Config ===
DELAY_BETWEEN_API_CALLS = 3  # seconds between API calls

# === Load config ===
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
with open(CONFIG_PATH, 'r') as f:
    CONFIG = json.load(f)

USERNAME = CONFIG['username']
PASSWORD = CONFIG['password']
SPREADSHEET_ID = CONFIG['spreadsheet_id']
SHEET_NAME = CONFIG['sheet_name']
USER_LIST_FILE = os.path.join(BASE_DIR, CONFIG['user_list_file'])
POST_LIMIT = CONFIG['post_limit']
DELAY_BETWEEN_USERS = CONFIG['delay_between_users']
OPENAI_API_KEY = CONFIG['openai_api_key']
SCRAPED_DATA_FILE = os.path.join(BASE_DIR, 'scraped_posts.json')
CREDENTIALS_PATH = os.path.join(BASE_DIR, 'credentials.json')

# === Set OpenAI API Key ===
set_openai_api_key(OPENAI_API_KEY)

# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "bluesky_fetch.log")),
        logging.StreamHandler()
    ]
)

def safe_request(api_call_fn, *args, max_retries=5, initial_delay=5, **kwargs):
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            result = api_call_fn(*args, **kwargs)
            time.sleep(DELAY_BETWEEN_API_CALLS)
            return result
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                logging.warning(f"⚠️ Rate limit hit, sleeping {delay}s (attempt {attempt + 1})...")
                time.sleep(delay)
                delay = min(delay * 2, 120)
            else:
                logging.error(f"❌ API call failed: {e}")
                break
    return None

def clean_text(text: str) -> str:
    text = re.sub(r'[\U00010000-\U0010FFFF]+', '', text)
    text = re.sub(r'[^\x00-\x7F]+', '', text)
    return text.strip()

def send_to_google_sheets(rows):
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        body = {
            'values': rows  # Only data rows, no headers
        }

        result = sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()

        logging.info(f"✅ Appended {len(rows)} new posts to Google Sheets.")
    except Exception as e:
        logging.error(f"❌ Failed to write to Google Sheets: {e}")

def load_existing_post_uris():
    if not os.path.exists(SCRAPED_DATA_FILE):
        return set()
    with open(SCRAPED_DATA_FILE, 'r') as f:
        try:
            data = json.load(f)
            return set(data.get("uris", []))
        except json.JSONDecodeError:
            return set()

def save_post_uris(uris):
    with open(SCRAPED_DATA_FILE, 'w') as f:
        json.dump({"uris": list(uris)}, f, indent=2)

def get_posts_for_user(client: Client, handle: str):
    profile = safe_request(client.com.atproto.identity.resolve_handle, {'handle': handle})
    if not profile or not hasattr(profile, 'did'):
        logging.error(f"❌ Could not resolve DID for {handle}")
        return []

    did = profile.did
    logging.info(f"🔍 Fetching posts for {handle} ({did})...")
    feed = safe_request(client.app.bsky.feed.get_author_feed, {'actor': did, 'limit': POST_LIMIT})
    if not feed or not hasattr(feed, 'feed'):
        logging.error(f"❌ Failed to fetch feed for {handle}")
        return []

    rows = []
    for idx, post in enumerate(feed.feed, start=1):
        try:
            record = post.post.record

            # ✅ Filter out reposts (quotes)
            if post.reason is not None:
                continue

            # ✅ Filter out replies
            if hasattr(record, 'reply') and record.reply is not None:
                continue

            # ✅ Filter out textless or embed-only posts
            if not hasattr(record, 'text') or not record.text.strip():
                continue
            if hasattr(record, 'embed') and not record.text.strip():
                continue

            text = clean_text(record.text)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            uri = post.post.uri
            category, controversy = categorize_text(text)
            rows.append([timestamp, text, uri, handle, category, controversy])
        except Exception as e:
            logging.warning(f"⚠️ Error processing post #{idx} from {handle}: {e}")

    return rows

def get_user_list(filepath):
    try:
        with open(filepath, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logging.error(f"❌ Could not read user list: {e}")
        return []

def main():
    client = Client()
    try:
        logging.info("🔐 Logging in...")
        safe_request(client.login, USERNAME, PASSWORD)
    except Exception as e:
        logging.error(f"❌ Login failed: {e}")
        return

    users = get_user_list(USER_LIST_FILE)
    if not users:
        logging.warning("⚠️ No users to scrape.")
        return

    existing_uris = load_existing_post_uris()
    updated_uris = set(existing_uris)

    for user in users:
        new_rows = []
        rows = get_posts_for_user(client, user)
        for row in rows:
            uri = row[2]
            if uri not in updated_uris:
                new_rows.append(row)
                updated_uris.add(uri)

        if new_rows:
            logging.info(f"📤 Sending {len(new_rows)} new posts from {user} to Google Sheets...")
            send_to_google_sheets(new_rows)
            save_post_uris(updated_uris)
        else:
            logging.info(f"📭 No new posts found for {user}.")

        logging.info(f"⏱ Waiting {DELAY_BETWEEN_USERS}s before next user...")
        time.sleep(DELAY_BETWEEN_USERS)

    logging.info("✅ Finished scraping all users.")

if __name__ == "__main__":
    main()
