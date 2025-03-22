import os
import praw
import gspread
import hashlib
import random
import time
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# ========================
# CONFIGURATION
# ========================
REDDIT_CREDS = {
    "client_id": os.getenv('REDDIT_CLIENT_ID'),
    "client_secret": os.getenv('REDDIT_CLIENT_SECRET'),
    "username": os.getenv('REDDIT_USERNAME'),
    "password": os.getenv('REDDIT_PASSWORD'),
    "user_agent": "RedditMatchBot/1.0"
}

GOOGLE_CREDS_FILE = "credentials.json"
SHEET_NAME = "Reddit_Nepal _Matchmaker-2-0"
MIN_ACCOUNT_AGE_DAYS = 90
MAX_DM_RETRIES = 3

REQUIRED_COLUMNS = {
    'username': 'Reddit Username:',  # Note the space after colon seen in your sheet
    'dm_pref': 'Do You Accept DM Notifications About Your Match?',
    'code': 'Code',
    'status': 'Status',
    'dm_status': 'DM Status'
}


HUMOR = {
    "dm_messages": [
        "Your code survived 3 rounds of load shedding ⚡",
        "Approved by 9/10 street cows of Kathmandu 🐄",
        "Contains pure momo energy - handle with care! 🥟"
    ],
    "rejections": [
        "Account younger than Nepal's average power cut ⏳",
        "Come back after your account survives TIA WiFi ☕",
        "Age requirement: Survived 5+ internet shutdowns"
    ]
}

# ========================
# SHEET MANAGEMENT
# ========================
def setup_sheet(sheet):
    """Robust column handling with whitespace normalization"""
    headers = [h.strip() for h in sheet.row_values(1)]
    col_map = {}

    # Normalize column names
    normalized_headers = {h.strip().lower(): h for h in headers}
    required_normalized = {
        v.strip().lower(): k for k, v in REQUIRED_COLUMNS.items()
    }

    # Find existing columns
    for norm_header, orig_header in normalized_headers.items():
        if norm_header in required_normalized:
            key = required_normalized[norm_header]
            col_map[key] = headers.index(orig_header) + 1

    # Create missing columns
    new_columns = [
        col for col in REQUIRED_COLUMNS.values()
        if col.strip().lower() not in normalized_headers
    ]

    if new_columns:
        sheet.add_cols(len(new_columns))
        end_col = len(headers)
        update_range = f"{gspread.utils.rowcol_to_a1(1, end_col+1)}1"
        sheet.update(update_range, [new_columns], value_input_option='USER_ENTERED')
        headers += new_columns

    # Final verification
    for key, col_name in REQUIRED_COLUMNS.items():
        try:
            col_map[key] = headers.index(col_name) + 1
        except ValueError:
            raise RuntimeError(f"Missing critical column: '{col_name}'")

    return col_map


# ========================
# CORE FUNCTIONS
# ========================
def is_eligible(reddit, username):
    """Check account age requirement"""
    try:
        user = reddit.redditor(username.split('u/')[-1].strip())
        age = datetime.utcnow() - datetime.fromtimestamp(user.created_utc)
        return age.days >= MIN_ACCOUNT_AGE_DAYS
    except Exception:
        return False

def generate_code(username):
    """Generate unique 8-character code"""
    return hashlib.sha256(f"{username}{datetime.now()}".encode()).hexdigest()[:8].upper()

def send_dm(reddit, username, message, is_retry=False):
    """Send DM with rate limiting"""
    try:
        subject = "🔐 Your Match Code"
        if is_retry:
            subject = "🚨 Code Redelivery!"
            message += "\n\n⚠️ Final attempt! Lose this and you get AutoModerator 🤖"

        reddit.redditor(username.split('u/')[-1].strip()).message(
            subject=subject,
            message=message
        )
        time.sleep(20)  # Conservative rate limiting
        return True
    except Exception as e:
        print(f"DM failed for {username}: {str(e)}")
        return False

# ========================
# MAIN PROCESSING
# ========================
def process_responses():
    reddit = praw.Reddit(**REDDIT_CREDS)

    # Connect to Google Sheets
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).sheet1

    try:
        col = setup_sheet(sheet)
    except RuntimeError as e:
        print(f"Sheet setup failed: {str(e)}")
        return

    records = sheet.get_all_records()

    for idx, row in enumerate(records, start=2):
        try:
            # Skip processed rows
            current_status = sheet.cell(idx, col['status']).value
            if current_status in ['Processed', 'Rejected', 'Opted Out']:
                continue

            username = str(row.get(REQUIRED_COLUMNS['username'], '')).strip()
            dm_pref = str(row[REQUIRED_COLUMNS['dm_pref']]).strip()

            # Check DM preference
            if dm_pref not in [
                "Yes, I want to receive a DM with my match's username, code, and the scientific reason why we were paired.",
                "Maybe, but only if my match is cool."
            ]:
                sheet.update_cell(idx, col['status'], 'Opted Out')
                continue

            # Validate username format
            if not username.lower().startswith('u/'):
                sheet.update_cell(idx, col['status'], 'Invalid Format')
                continue

            # Check eligibility
            if not is_eligible(reddit, username):
                rejection_msg = f"""🚨 Disqualified!

{random.choice(HUMOR['rejections'])}

Requirements:
- Account age ≥ {MIN_ACCOUNT_AGE_DAYS} days
- Minimum 1 meme posted (optional but encouraged)"""
                send_dm(reddit, username, rejection_msg)
                sheet.update_cell(idx, col['status'], 'Rejected')
                continue

            # Generate/send code
            code = generate_code(username)
            if not sheet.cell(idx, col['code']).value:
                sheet.update_cell(idx, col['code'], code)

            # Prepare DM
            message = f"""🎉 Your Match Code: {code}

🔮 How this works:
1. Results in 3-5 days (need to consult Bagmati River dolphins 🐬)
2. Lose code = match with r/Nepal AutoModerator
3. Valid during strikes, protests, and NTC outages

{random.choice(HUMOR['dm_messages'])}"""

            # Send with retries
            success = False
            for attempt in range(MAX_DM_RETRIES):
                if send_dm(reddit, username, message, attempt>0):
                    success = True
                    break
                time.sleep(120)

            # Update status
            sheet.update_cell(idx, col['status'], 'Processed' if success else 'Failed')
            sheet.update_cell(idx, col['dm_status'], '✅ Sent' if success else '❌ Failed')

        except Exception as e:
            print(f"Error processing row {idx}: {str(e)}")
            sheet.update_cell(idx, col['status'], 'Error')

if __name__ == "__main__":
    process_responses()