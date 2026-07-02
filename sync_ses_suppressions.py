"""
Syncs the SES account-level suppression list into the local ses_suppression table.

SES automatically adds emails here on:
  - Hard bounces  (reason = 'BOUNCE')
  - Complaints    (reason = 'COMPLAINT')

Run any time to pull the latest list:
  python sync_ses_suppressions.py

Safe to run repeatedly — uses INSERT ... ON CONFLICT DO NOTHING.
"""

import os
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import timezone

DATABASE_URL = os.environ.get("DATABASE_URL") or open(".env").read().split("DATABASE_URL=")[1].strip()
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-2")


def sync():
    ses  = boto3.client("sesv2", region_name=AWS_REGION)
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur  = conn.cursor()

    total   = 0
    new     = 0
    token   = None

    print("Fetching SES account suppression list...")

    while True:
        kwargs = {"PageSize": 1000}
        if token:
            kwargs["NextToken"] = token

        resp    = ses.list_suppressed_destinations(**kwargs)
        entries = resp.get("SuppressedDestinationSummaries", [])

        for entry in entries:
            email      = entry["EmailAddress"]
            reason     = entry["Reason"]          # 'BOUNCE' or 'COMPLAINT'
            suppressed = entry["LastUpdateTime"]  # datetime with tz

            cur.execute("""
                INSERT INTO ses_suppression (email, reason, suppressed_at, source)
                VALUES (%s, %s, %s, 'ses-account')
                ON CONFLICT (email) DO UPDATE
                    SET reason        = EXCLUDED.reason,
                        suppressed_at = EXCLUDED.suppressed_at,
                        source        = EXCLUDED.source
            """, (email, reason, suppressed))

            if cur.rowcount > 0:
                new += 1
            total += 1

        token = resp.get("NextToken")
        if not token:
            break

    conn.commit()
    conn.close()

    print(f"Done. {total} entries in SES list — {new} upserted into ses_suppression.")

    # Summary breakdown
    conn2 = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur2  = conn2.cursor()
    cur2.execute("""
        SELECT reason, COUNT(1) AS n
        FROM ses_suppression
        GROUP BY reason ORDER BY reason
    """)
    for row in cur2.fetchall():
        print(f"  {row['reason']:12s}: {row['n']}")
    conn2.close()


if __name__ == "__main__":
    sync()
