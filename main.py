import os
import base64
import json
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
    "user_agent": "RedditMatchBot/1.0 (GitHub Actions)"
}

SHEET_NAME = "Reddit_Nepal _Matchmaker-2-0"
MIN_ACCOUNT_AGE_DAYS = 90
MAX_DM_RETRIES = 3

COLUMN_MAP = {
    'username': 'Reddit Username:',
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
# GOOGLE SHEETS SETUP
# ========================
def get_google_sheets():
    """Initialize Google Sheets API with base64 encoded credentials"""
    creds_b64 = os.getenv('GOOGLE_CREDS_BASE64')
    creds_json = json.loads(base64.b64decode(creds_b64).decode())
    
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    return gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
)

# ========================
# CORE FUNCTIONS
# ========================
def setup_sheet(sheet):
    """Ensure required columns exist and return column indices"""
    headers = [h.strip() for h in sheet.row_values(1)]
    col_indices = {}
    new_columns = []

    # Find existing columns
    for key, col_name in COLUMN_MAP.items():
        try:
            col_indices[key] = headers.index(col_name) + 1
        except ValueError:
            new_columns.append(col_name)

    # Add missing columns
    if new_columns:
        sheet.add_cols(len(new_columns))
        end_col = len(headers)
        update_range = f"{gspread.utils.rowcol_to_a1(1, end_col+1)}1"
        sheet.update(update_range, [new_columns], value_input_option='USER_ENTERED')
        headers += new_columns

    # Get final indices
    for key, col_name in COLUMN_MAP.items():
        col_indices[key] = headers.index(col_name) + 1
    
    return col_indices

def is_eligible(reddit, username):
    """Check if account meets age requirements"""
    try:
        user = reddit.redditor(username.split('u/')[-1].strip())
        age = datetime.utcnow() - datetime.fromtimestamp(user.created_utc)
        return age.days >= MIN_ACCOUNT_AGE_DAYS
    except Exception:
        return False

def generate_code(username):
    """Create unique match code"""
    return hashlib.sha256(f"{username}{datetime.now()}".encode()).hexdigest()[:8].upper()

def send_dm(reddit, username, message, is_retry=False):
    """Send Reddit DM with rate limiting"""
    try:
        subject = "🔐 Your Match Code" 
        if is_retry:
            subject = "🚨 Code Redelivery!"
            message += "\n\n⚠️ Final attempt! Lose this and you get AutoModerator 🤖"

        reddit.redditor(username.split('u/')[-1].strip()).message(subject=subject, message=message)
        time.sleep(20)  # Rate limit protection
        return True
    except Exception as e:
        print(f"DM failed for {username}: {str(e)}")
        return False

# ========================
# MAIN PROCESSING
# ========================
def process_responses():
    """Main processing function"""
    print(f"\n=== Starting processing at {datetime.now()} ===")
    
    try:
        # Initialize APIs
        reddit = praw.Reddit(**REDDIT_CREDS)
        gc = get_google_sheets()
        sheet = gc.open(SHEET_NAME).sheet1
        col = setup_sheet(sheet)

        # Process rows
        records = sheet.get_all_records()
        for idx, row in enumerate(records, start=2):
            try:
                # Skip processed rows
                current_status = sheet.cell(idx, col['status']).value
                if current_status in ['Processed', 'Rejected', 'Opted Out']:
                    continue

                # Get user data
                username = str(row.get(COLUMN_MAP['username'], '')).strip()
                dm_pref = str(row.get(COLUMN_MAP['dm_pref'], '')).strip()

                # DM preference check
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
                    rejection_msg = f"""🚨 Disqualified!\n\n{random.choice(HUMOR['rejections'])}\n\nRequirements:\n- Account age ≥ {MIN_ACCOUNT_AGE_DAYS} days"""
                    send_dm(reddit, username, rejection_msg)
                    sheet.update_cell(idx, col['status'], 'Rejected')
                    continue

                # Generate/send code
                code = generate_code(username)
                if not sheet.cell(idx, col['code']).value:
                    sheet.update_cell(idx, col['code'], code)

                # Prepare DM
                message = f"""🎉 Your Match Code: {code}\n\n🔮 How this works:\n1. Results in 3-5 days\n2. Lose code = match with AutoMod\n3. Valid during outages\n\n{random.choice(HUMOR['dm_messages'])}"""

                # Send with retries
                success = any(
                    send_dm(reddit, username, message, attempt>0)
                    for attempt in range(MAX_DM_RETRIES)
                )

                # Update status
                sheet.update_cell(idx, col['status'], 'Processed' if success else 'Failed')
                sheet.update_cell(idx, col['dm_status'], '✅ Sent' if success else '❌ Failed')

            except Exception as e:
                print(f"Error processing row {idx}: {str(e)}")
                sheet.update_cell(idx, col['status'], 'Error')

        print(f"=== Completed processing at {datetime.now()} ===\n")

    except Exception as e:
        print(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    process_responses()