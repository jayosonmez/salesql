"""
Lambda function: receives Gmail Pub/Sub push notifications via API Gateway,
finds new messages in Gmail history, and records campaign replies to Postgres.

Environment variables required:
  DATABASE_URL         — Neon Postgres connection string
  GMAIL_SECRET_NAME    — AWS Secrets Manager secret name storing gmail_token.json contents
  AWS_REGION           — e.g. us-east-2 (set automatically by Lambda)
"""

import os
import json
import base64
import ssl
import boto3
import pg8000.dbapi as pg8000
from urllib.parse import urlparse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

DATABASE_URL      = os.environ["DATABASE_URL"]
GMAIL_SECRET_NAME = os.environ.get("GMAIL_SECRET_NAME", "metsulin/gmail-token")
LABEL_NAME        = "funding-reachout"


# ── Gmail auth ────────────────────────────────────────────────────────────────

def get_gmail_service():
    sm = boto3.client("secretsmanager")
    secret = sm.get_secret_value(SecretId=GMAIL_SECRET_NAME)["SecretString"]
    creds  = Credentials.from_authorized_user_info(json.loads(secret))
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        sm.put_secret_value(
            SecretId=GMAIL_SECRET_NAME,
            SecretString=creds.to_json(),
        )
    return build("gmail", "v1", credentials=creds)


# ── Gmail helpers ─────────────────────────────────────────────────────────────

def get_label_id(service):
    result = service.users().labels().list(userId="me").execute()
    for label in result.get("labels", []):
        if label["name"].lower() == LABEL_NAME.lower():
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
    if "<" in from_header:
        return from_header.split("<")[1].strip(">").strip().lower()
    return from_header.strip().lower()


# ── Database helpers ──────────────────────────────────────────────────────────

def get_db():
    url = urlparse(DATABASE_URL)
    ssl_ctx = ssl.create_default_context()
    return pg8000.connect(
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip("/"),
        ssl_context=ssl_ctx,
    )


def fetchone_dict(cur):
    row = cur.fetchone()
    if row is None:
        return None
    return {desc[0]: val for desc, val in zip(cur.description, row)}


def fetchall_dict(cur):
    rows = cur.fetchall()
    return [{desc[0]: val for desc, val in zip(cur.description, row)} for row in rows]


def get_last_history_id(conn):
    cur = conn.cursor()
    cur.execute("SELECT value FROM global_config WHERE key = 'gmail_history_id'")
    row = fetchone_dict(cur)
    return row["value"] if row else None


def save_history_id(conn, history_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO global_config (key, value) VALUES ('gmail_history_id', %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """, (str(history_id),))
    conn.commit()


def get_our_sending_addresses(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT from_email FROM campaigns WHERE from_email IS NOT NULL")
    return {r["from_email"].lower().strip() for r in fetchall_dict(cur)}


def find_enrollment_by_message_id(conn, mime_id):
    if not mime_id:
        return None
    cur = conn.cursor()
    cur.execute("""
        SELECT enrollment_id, campaign_id, email
        FROM sends
        WHERE mime_message_id = %s
        LIMIT 1
    """, (mime_id.strip(),))
    return fetchone_dict(cur)


def record_reply(conn, service, label_id, gmail_msg, headers):
    sender_email     = extract_sender_email(headers.get("From", ""))
    gmail_message_id = gmail_msg["id"]
    thread_id        = gmail_msg["threadId"]
    snippet          = gmail_msg.get("snippet", "")[:500]

    in_reply_to = headers.get("In-Reply-To", "").strip()
    references  = headers.get("References", "").strip()

    match = find_enrollment_by_message_id(conn, in_reply_to)
    if not match and references:
        first_ref = references.split()[0]
        match = find_enrollment_by_message_id(conn, first_ref)

    if not match:
        print(f"  Not a campaign reply — skipping (In-Reply-To: {in_reply_to or 'none'})")
        return

    print(f"  Reply matched campaign_id={match['campaign_id']} email={match['email']}")

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gmail_events
            (gmail_message_id, thread_id, email, event_type, received_at, snippet)
        VALUES (%s, %s, %s, 'reply', NOW(), %s)
        ON CONFLICT (gmail_message_id) DO NOTHING
    """, (gmail_message_id, thread_id, sender_email, snippet))

    if cur.rowcount > 0:
        print(f"  ✓ Reply recorded from {sender_email}")
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


# ── Core processing ───────────────────────────────────────────────────────────

def process_history(service, conn, label_id, history_id):
    our_addresses = get_our_sending_addresses(conn)

    try:
        result = service.users().history().list(
            userId="me",
            startHistoryId=history_id,
            historyTypes=["messageAdded"],
            labelId="INBOX",
        ).execute()
    except Exception as e:
        print(f"  history.list failed (historyId may be expired): {e}")
        return

    history_records = result.get("history", [])
    print(f"  {len(history_records)} history records since historyId={history_id}")

    for record in history_records:
        for item in record.get("messagesAdded", []):
            msg_id = item["message"]["id"]
            try:
                gmail_msg, headers = get_message_headers(service, msg_id)
            except Exception as e:
                print(f"  Skipping message {msg_id}: {e}")
                continue
            sender = extract_sender_email(headers.get("From", ""))

            if sender in our_addresses:
                print(f"  Skipping our own email from {sender}")
                continue

            record_reply(conn, service, label_id, gmail_msg, headers)


# ── Lambda handler ────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    body = event.get("body", "")
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    try:
        payload = json.loads(body)
    except Exception:
        print("Bad request — could not parse body")
        return {"statusCode": 400, "body": "Bad Request"}

    message = payload.get("message", {})
    data    = message.get("data", "")
    try:
        notification = json.loads(base64.b64decode(data).decode("utf-8"))
    except Exception:
        print("Could not decode Pub/Sub message data")
        return {"statusCode": 400, "body": "Bad Request"}

    history_id    = str(notification.get("historyId", ""))
    email_address = notification.get("emailAddress", "")
    print(f"Pub/Sub notification: emailAddress={email_address} historyId={history_id}")

    if not history_id:
        return {"statusCode": 200, "body": "OK"}

    service  = get_gmail_service()
    conn     = get_db()
    label_id = get_label_id(service)

    last_history_id = get_last_history_id(conn)

    if last_history_id:
        process_history(service, conn, label_id, last_history_id)
    else:
        print(f"  No prior historyId stored — saving {history_id} as baseline")

    save_history_id(conn, history_id)
    conn.close()

    return {"statusCode": 200, "body": "OK"}
