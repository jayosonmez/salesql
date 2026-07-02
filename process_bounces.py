"""
Polls the metsulin-bounces SQS queue for SES bounce and complaint notifications
and writes them to ses_suppression so they are excluded from future sends.

Run any time, or add as a step in GitHub Actions before the daily send.
Safe to run repeatedly — uses INSERT ... ON CONFLICT DO NOTHING.
"""

import os
import json
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

DATABASE_URL   = os.environ.get("DATABASE_URL") or open(".env").read().split("DATABASE_URL=")[1].strip()
AWS_REGION     = os.environ.get("AWS_REGION", "us-east-2")
QUEUE_URL      = "https://sqs.us-east-2.amazonaws.com/093366266563/metsulin-bounces"
ALERT_EMAIL_TO = "jay@metsulin.com"
ALERT_EMAIL_FROM = "jay@metsulin.com"


def send_complaint_alert(ses_client, email):
    ses_client.send_email(
        Source=ALERT_EMAIL_FROM,
        Destination={"ToAddresses": [ALERT_EMAIL_TO]},
        Message={
            "Subject": {"Data": f"⚠️ Spam complaint received: {email}"},
            "Body": {"Text": {"Data": f"The recipient {email} marked your email as spam.\n\nThey have been added to the suppression list and will not receive future emails."}},
        },
    )
    print(f"  Alert sent to {ALERT_EMAIL_TO} for complaint from {email}")


def process():
    sqs  = boto3.client("sqs", region_name=AWS_REGION)
    ses  = boto3.client("ses", region_name=AWS_REGION)
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur  = conn.cursor()

    total     = 0
    processed = 0

    print("Polling SQS for bounce/complaint notifications...")

    while True:
        resp = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )

        messages = resp.get("Messages", [])
        if not messages:
            break

        for msg in messages:
            total += 1
            try:
                # SQS body is SNS envelope JSON
                sns_envelope = json.loads(msg["Body"])
                # SNS Message field contains the actual SES notification JSON
                ses_notification = json.loads(sns_envelope["Message"])

                notif_type = ses_notification.get("notificationType") or ses_notification.get("eventType")

                if notif_type == "Bounce":
                    bounce = ses_notification["bounce"]
                    reason = "BOUNCE"
                    for recipient in bounce.get("bouncedRecipients", []):
                        email = recipient["emailAddress"].lower().strip()
                        cur.execute("""
                            INSERT INTO ses_suppression (email, reason, suppressed_at, source)
                            VALUES (%s, %s, NOW(), 'sns-bounce')
                            ON CONFLICT (email) DO NOTHING
                        """, (email, reason))
                        print(f"  BOUNCE: {email}")
                        processed += 1

                elif notif_type == "Complaint":
                    complaint = ses_notification["complaint"]
                    reason = "COMPLAINT"
                    for recipient in complaint.get("complainedRecipients", []):
                        email = recipient["emailAddress"].lower().strip()
                        cur.execute("""
                            INSERT INTO ses_suppression (email, reason, suppressed_at, source)
                            VALUES (%s, %s, NOW(), 'sns-complaint')
                            ON CONFLICT (email) DO NOTHING
                        """, (email, reason))
                        print(f"  COMPLAINT: {email}")
                        processed += 1
                        send_complaint_alert(ses, email)

                # Delete message from queue after successful processing
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"],
                )

            except Exception as ex:
                print(f"  ERROR processing message: {ex}")
                # Don't delete — it will reappear after visibility timeout

        conn.commit()

    conn.close()
    print(f"Done. Processed {total} SQS messages — {processed} emails added to suppression list.")


if __name__ == "__main__":
    process()
