"""
Processes Gmail reply notifications from Pub/Sub.
Can run in two modes:

1. Webhook mode (called by Lambda with Pub/Sub push payload):
   Called with the raw Pub/Sub JSON body as stdin or as argument.

2. Poll mode (for testing / GitHub Actions fallback):
   python process_replies.py --poll
   Checks recent Gmail inbox messages directly.
"""

import os
import sys
import json
import base64
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

DATABASE_URL = os.environ.get("DATABASE_URL") or open(".env").read().split("DATABASE_URL=")[1].strip()
TOKEN_FILE   = os.environ.get("GMAIL_TOKEN_FILE", "gmail_token.json")
LABEL_NAME   = "funding-reachout"
POLL_HOURS   = int(os.environ.get("POLL_HOURS", "25"))  # default 25h for daily cron, override for testing


def get_gmail_service():
    creds = Credentials.from_authorized_user_file(TOKEN_FILE)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def get_label_id(service, label_name):
    result = service.users().labels().list(userId="me").execute()
    for label in result.get("labels", []):
        if label["name"].lower() == label_name.lower():
            return label["id"]
    return None


def get_message_headers(service, message_id):
    msg = service.users().messages().get(
        userId="me", id=message_id, format="metadata",
        metadataHeaders=["In-Reply-To", "References", "From", "Date", "Subject"]
    ).execute()
    headers = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
    return msg, headers


def extract_sender_email(from_header):
    """Extract email address from 'Name <email>' format."""
    if "<" in from_header:
        return from_header.split("<")[1].strip(">").strip().lower()
    return from_header.strip().lower()


def find_enrollment_by_message_id(conn, in_reply_to):
    """Check if In-Reply-To matches a send we made."""
    if not in_reply_to:
        return None
    cur = conn.cursor()
    cur.execute("""
        SELECT s.enrollment_id, s.campaign_id, s.email
        FROM sends s
        WHERE s.mime_message_id = %s
        LIMIT 1
    """, (in_reply_to.strip(),))
    return cur.fetchone()


def record_reply(conn, gmail_msg, headers, enrollment, label_id, service):
    cur = conn.cursor()
    gmail_message_id = gmail_msg["id"]
    thread_id        = gmail_msg["threadId"]
    sender_email     = extract_sender_email(headers.get("From", ""))
    snippet          = gmail_msg.get("snippet", "")[:500]

    cur.execute("""
        INSERT INTO gmail_events
            (gmail_message_id, thread_id, email, event_type, received_at, snippet)
        VALUES (%s, %s, %s, 'reply', NOW(), %s)
        ON CONFLICT (gmail_message_id) DO NOTHING
    """, (gmail_message_id, thread_id, sender_email, snippet))

    if cur.rowcount > 0:
        print(f"  ✓ Reply recorded from {sender_email} (gmail_id: {gmail_message_id})")

        # Apply funding-reachout label
        if label_id:
            service.users().messages().modify(
                userId="me",
                id=gmail_message_id,
                body={"addLabelIds": [label_id]}
            ).execute()
            print(f"    ✓ Label '{LABEL_NAME}' applied")
    else:
        print(f"  Already processed: {gmail_message_id}")

    conn.commit()


def get_our_sending_addresses(conn):
    """Returns all from_email addresses used by our campaigns."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT from_email FROM campaigns WHERE from_email IS NOT NULL")
    return {r["from_email"].lower().strip() for r in cur.fetchall()}


def process_message_id(service, conn, label_id, gmail_message_id, our_addresses):
    gmail_msg, headers = get_message_headers(service, gmail_message_id)
    in_reply_to = headers.get("In-Reply-To", "").strip()
    references  = headers.get("References", "").strip()
    sender      = extract_sender_email(headers.get("From", ""))

    # Skip emails sent by us — these are our own follow-ups, not replies
    if sender in our_addresses:
        print(f"  Skipping our own email from {sender}")
        return

    # Check In-Reply-To first, then first References entry
    match = find_enrollment_by_message_id(conn, in_reply_to)
    if not match and references:
        first_ref = references.split()[0]
        match = find_enrollment_by_message_id(conn, first_ref)

    if match:
        print(f"  Reply matched to campaign_id={match['campaign_id']} email={match['email']}")
        record_reply(conn, gmail_msg, headers, match, label_id, service)
    else:
        print(f"  Not a campaign reply — skipping (In-Reply-To: {in_reply_to or 'none'})")


def poll_mode():
    """Poll Gmail inbox for recent messages and check for campaign replies."""
    print(f"Polling Gmail inbox (last {POLL_HOURS} hours)...")
    service = get_gmail_service()
    conn    = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    label_id = get_label_id(service, LABEL_NAME)
    if not label_id:
        print(f"WARNING: Label '{LABEL_NAME}' not found in Gmail — labeling will be skipped")

    # Search inbox for messages received in the last POLL_HOURS hours
    after = int((datetime.now(timezone.utc) - timedelta(hours=POLL_HOURS)).timestamp())
    results = service.users().messages().list(
        userId="me",
        q=f"in:inbox after:{after}",
        maxResults=100,
    ).execute()

    messages = results.get("messages", [])
    print(f"Found {len(messages)} inbox messages to check")

    our_addresses = get_our_sending_addresses(conn)
    for m in messages:
        process_message_id(service, conn, label_id, m["id"], our_addresses)

    conn.close()
    print("Done.")


if __name__ == "__main__":
    if "--poll" in sys.argv:
        poll_mode()
    else:
        print("Usage: python process_replies.py --poll")
        print("Lambda webhook mode is handled by lambda_handler() in this file.")
