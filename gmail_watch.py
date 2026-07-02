"""
Registers Gmail push notifications via Pub/Sub.
Run once to activate — Gmail will push notifications to our Pub/Sub topic.
Watch expires after 7 days; re-run weekly or add to cron.
"""

import os
import json
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

TOKEN_FILE  = os.environ.get("GMAIL_TOKEN_FILE", "gmail_token.json")
PUBSUB_TOPIC = "projects/fundraising-425606/topics/gmail-replies"

creds = Credentials.from_authorized_user_file(TOKEN_FILE)
if creds.expired and creds.refresh_token:
    creds.refresh(Request())

service = build("gmail", "v1", credentials=creds)

result = service.users().watch(
    userId="me",
    body={
        "topicName": PUBSUB_TOPIC,
        "labelIds": ["INBOX"],
        "labelFilterBehavior": "INCLUDE",
    }
).execute()

print("Watch registered:")
print(f"  historyId: {result['historyId']}")
print(f"  expiration: {result['expiration']} (ms since epoch, ~7 days)")
