"""
Global email sender — one run per day, called by EventBridge cron at 7am PDT.
Priority per campaign: follow-ups (step > 1) before new initial sends.
Respects: global max_daily_total and per-campaign daily_limit.

Usage:
  python sender.py                    # dry run (no emails sent)
  python sender.py --send             # live send via SES
  python sender.py --send --limit 50  # live send, cap at 50 total this run
  python sender.py --limit 10         # dry run, cap at 10
"""

import sys
import os
import time
import uuid
import email as email_lib
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

DATABASE_URL = os.environ.get("DATABASE_URL") or open(".env").read().split("DATABASE_URL=")[1].strip()
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-2")
DRY_RUN      = "--send" not in sys.argv

# Optional per-run cap: --limit N overrides global/campaign limits for this run only.
_limit_arg = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--limit" and i+1 < len(sys.argv)), None)
RUN_LIMIT   = int(_limit_arg) if _limit_arg else None

# Throttle: 0.5s between sends = ~2 emails/sec = 1,000 emails in ~8 min.
# Well within SES default production rate of 14 emails/sec.
# Set to 0 in dry-run so tests run fast.
SEND_DELAY_SECS = 0.0 if DRY_RUN else 0.5

SEND_MARKER = "[DRY RUN]" if DRY_RUN else "[LIVE]"

# --------------------------------------------------------------------------- #
#  DB helpers                                                                  #
# --------------------------------------------------------------------------- #

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def get_global_limit(conn):
    cur = conn.cursor()
    cur.execute("SELECT value FROM global_config WHERE key = 'max_daily_total'")
    row = cur.fetchone()
    return int(row["value"]) if row else 500


def count_global_sent_today(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(1) AS n FROM sends
        WHERE status = 'sent'
        AND (sent_at AT TIME ZONE 'America/Los_Angeles')::DATE
            = (NOW() AT TIME ZONE 'America/Los_Angeles')::DATE
    """)
    return cur.fetchone()["n"]


def count_campaign_sent_today(conn, campaign_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(1) AS n FROM sends
        WHERE campaign_id = %s AND status = 'sent'
        AND (sent_at AT TIME ZONE 'America/Los_Angeles')::DATE
            = (NOW() AT TIME ZONE 'America/Los_Angeles')::DATE
    """, (campaign_id,))
    return cur.fetchone()["n"]


def get_active_campaigns(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, daily_limit, from_name, from_email, reply_to, gmail_label
        FROM campaigns WHERE status = 'active'
        ORDER BY id
    """)
    return cur.fetchall()


def get_step(conn, campaign_id, step_num):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, step_num, subject, body_template, wait_days
        FROM campaign_steps
        WHERE campaign_id = %s AND step_num = %s
    """, (campaign_id, step_num))
    return cur.fetchone()


def get_step_count(conn, campaign_id):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) AS n FROM campaign_steps WHERE campaign_id = %s", (campaign_id,))
    return cur.fetchone()["n"]


def get_previous_send(conn, enrollment_id, prev_step_num):
    """Fetch mime_message_id, raw_email and sent_at from the previous step send."""
    cur = conn.cursor()
    cur.execute("""
        SELECT mime_message_id, raw_email, sent_at
        FROM sends
        WHERE enrollment_id = %s AND sequence_num = %s
        ORDER BY sent_at DESC
        LIMIT 1
    """, (enrollment_id, prev_step_num))
    return cur.fetchone()


SAFETY_FILTER = """
    AND e.email NOT IN (SELECT email FROM unsubscribes)
    AND e.email NOT IN (SELECT email FROM suppressions)
    AND e.email NOT IN (SELECT email FROM ses_suppression)
    AND NOT EXISTS (
        SELECT 1 FROM gmail_events ge
        WHERE ge.email = e.email AND ge.event_type = 'reply'
    )
"""


def get_due_followups(conn, campaign_id, limit):
    """Enrollments past step 1 whose next_send_at has arrived."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            e.id AS enrollment_id,
            e.email,
            e.current_step,
            c.first_name,
            c.last_name,
            c.company
        FROM campaign_enrollments e
        JOIN contact_emails ce ON ce.email = e.email
        JOIN contacts c ON c.id = ce.contact_id
        WHERE e.campaign_id = %s
          AND e.status = 'active'
          AND e.current_step > 1
          AND e.next_send_at <= NOW()
          {SAFETY_FILTER}
        ORDER BY e.next_send_at ASC
        LIMIT %s
    """, (campaign_id, limit))
    return cur.fetchall()


def get_new_enrollments(conn, campaign_id, limit):
    """Enrollments at step 1 not yet sent (next_send_at is NULL)."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            e.id AS enrollment_id,
            e.email,
            e.current_step,
            c.first_name,
            c.last_name,
            c.company
        FROM campaign_enrollments e
        JOIN contact_emails ce ON ce.email = e.email
        JOIN contacts c ON c.id = ce.contact_id
        WHERE e.campaign_id = %s
          AND e.status = 'active'
          AND e.current_step = 1
          AND e.next_send_at IS NULL
          {SAFETY_FILTER}
        ORDER BY e.enrolled_at ASC
        LIMIT %s
    """, (campaign_id, limit))
    return cur.fetchall()


# --------------------------------------------------------------------------- #
#  Template rendering                                                           #
# --------------------------------------------------------------------------- #

def render(template, contact):
    first = (contact.get("first_name") or "").strip() or "there"
    last  = (contact.get("last_name")  or "").strip()
    full  = f"{first} {last}".strip()
    co    = (contact.get("company") or "").strip()
    out   = template
    for placeholder, value in [
        ("{{first_name}}", first),
        ("{{last_name}}",  last),
        ("{{full_name}}",  full),
        ("{{company}}",    co),
    ]:
        out = out.replace(placeholder, value)
    return out


# --------------------------------------------------------------------------- #
#  Email threading helpers                                                      #
# --------------------------------------------------------------------------- #

def extract_html_body(raw_email_str):
    """Parse a raw MIME string and return the HTML body."""
    if not raw_email_str:
        return ""
    msg = email_lib.message_from_string(raw_email_str)
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            payload = part.get_payload(decode=True)
            return payload.decode("utf-8", errors="replace") if payload else ""
    # Fall back to plain text wrapped in <pre>
    for part in msg.walk():
        if part.get_content_type() == "text/plain":
            payload = part.get_payload(decode=True)
            if payload:
                text = payload.decode("utf-8", errors="replace")
                return f"<pre style='font-family:inherit;'>{text}</pre>"
    return ""


def build_quoted_block(prev_raw_email, from_addr, sent_at):
    """Build an HTML blockquote of the previous email to append to follow-ups."""
    prev_body = extract_html_body(prev_raw_email)
    if not prev_body:
        return ""
    sent_str = sent_at.strftime("%a, %b %d %Y at %I:%M %p") if sent_at else ""
    return f"""
<br><br>
<div style="border-left:3px solid #ccc; padding-left:12px;
            color:#555; margin-top:8px; font-size:0.9em;">
  <div style="color:#888; margin-bottom:6px;">
    On {sent_str}, {from_addr} wrote:
  </div>
  {prev_body}
</div>
"""


# --------------------------------------------------------------------------- #
#  SES sending                                                                  #
# --------------------------------------------------------------------------- #

def build_mime(to_email, subject, body_html, from_addr,
               reply_to=None, in_reply_to=None, references=None):
    """Build a MIME message with a self-generated Message-ID for threading."""
    mime_id = f"<{uuid.uuid4()}@metsulin.com>"
    msg = MIMEMultipart("alternative")
    msg["Message-ID"] = mime_id
    msg["Subject"]    = subject
    msg["From"]       = from_addr
    msg["To"]         = to_email
    if reply_to:
        msg["Reply-To"]    = reply_to
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"]  = references or in_reply_to
    msg.attach(MIMEText(body_html, "html"))
    return msg, mime_id


def send_via_ses(ses_client, to_email, subject, body_html, from_addr,
                 reply_to=None, in_reply_to=None, references=None):
    msg, mime_id = build_mime(to_email, subject, body_html, from_addr,
                               reply_to, in_reply_to, references)
    resp = ses_client.send_raw_email(
        Source=from_addr,
        Destinations=[to_email],
        RawMessage={"Data": msg.as_string()},
        ConfigurationSetName="metsulin-sending",
    )
    return resp["MessageId"], msg.as_string(), mime_id


# --------------------------------------------------------------------------- #
#  Record a send and advance enrollment                                         #
# --------------------------------------------------------------------------- #

def record_send(conn, campaign_id, enrollment, step,
                ses_message_id, raw_email, mime_message_id):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO sends
            (campaign_id, email, sequence_num, enrollment_id, step_id,
             ses_message_id, mime_message_id, raw_email, sent_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'sent')
    """, (
        campaign_id,
        enrollment["email"],
        step["step_num"],
        enrollment["enrollment_id"],
        step["id"],
        ses_message_id,
        mime_message_id,
        raw_email,
    ))

    step_count = get_step_count(conn, campaign_id)
    if step["step_num"] >= step_count:
        cur.execute("""
            UPDATE campaign_enrollments
            SET status = 'completed', completed_at = NOW(), next_send_at = NULL
            WHERE id = %s
        """, (enrollment["enrollment_id"],))
    else:
        next_step = get_step(conn, campaign_id, step["step_num"] + 1)
        cur.execute("""
            UPDATE campaign_enrollments
            SET current_step = %s,
                next_send_at = NOW() + (%s * INTERVAL '1 day')
            WHERE id = %s
        """, (step["step_num"] + 1, next_step["wait_days"],
              enrollment["enrollment_id"]))

    conn.commit()


# --------------------------------------------------------------------------- #
#  Main send loop                                                               #
# --------------------------------------------------------------------------- #

def run():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    ses  = None if DRY_RUN else boto3.client("ses", region_name=AWS_REGION)

    if not DRY_RUN and RUN_LIMIT is None:
        print("WARNING: --limit N is required for live sends. Example: python sender.py --send --limit 50")
        conn.close()
        return

    global_limit     = get_global_limit(conn)
    global_sent      = count_global_sent_today(conn)
    global_remaining = global_limit - global_sent
    if RUN_LIMIT is not None:
        global_remaining = min(global_remaining, RUN_LIMIT)

    limit_note = f" (--limit {RUN_LIMIT})" if RUN_LIMIT is not None else ""
    print(f"{SEND_MARKER} Global limit: {global_limit} | Sent today: {global_sent} | Remaining: {global_remaining}{limit_note}")

    if global_remaining <= 0:
        print("Global daily limit reached. Nothing to send.")
        conn.close()
        return

    campaigns = get_active_campaigns(conn)
    if not campaigns:
        print("No active campaigns.")
        conn.close()
        return

    totals = {"followups": 0, "new": 0, "campaigns": []}

    for campaign in campaigns:
        cid    = campaign["id"]
        cname  = campaign["name"]
        climit = campaign["daily_limit"]

        cam_sent      = count_campaign_sent_today(conn, cid)
        cam_remaining = climit - cam_sent
        budget        = min(cam_remaining, global_remaining)

        from_name  = campaign.get("from_name") or "Jay Sonmez"
        from_email = campaign.get("from_email") or "jay@metsulin.com"
        from_addr  = f"{from_name} <{from_email}>" if from_name else from_email
        reply_to   = campaign.get("reply_to")

        print(f"\n--- Campaign: {cname} (id={cid}) ---")
        print(f"  Limit: {climit}/day | Sent today: {cam_sent} | Budget: {budget}")

        if budget <= 0:
            print("  Budget exhausted — skipping.")
            totals["campaigns"].append({"name": cname, "followups": 0, "new": 0})
            continue

        step_count = get_step_count(conn, cid)
        if step_count == 0:
            print("  No steps defined — skipping.")
            totals["campaigns"].append({"name": cname, "followups": 0, "new": 0})
            continue

        cam_followups = 0
        cam_new       = 0

        # ------------------------------------------------------------------- #
        #  Priority 1: follow-ups (threaded as replies)                        #
        # ------------------------------------------------------------------- #
        followups = get_due_followups(conn, cid, budget)
        print(f"  Follow-ups due: {len(followups)}")

        for enrollment in followups:
            if budget <= 0 or global_remaining <= 0:
                break

            step = get_step(conn, cid, enrollment["current_step"])
            if not step:
                print(f"  WARN: step {enrollment['current_step']} missing for enrollment {enrollment['enrollment_id']}")
                continue

            # Fetch the previous step's send to get threading headers + body
            prev = get_previous_send(conn, enrollment["enrollment_id"],
                                     enrollment["current_step"] - 1)

            in_reply_to = prev["mime_message_id"] if prev else None
            prev_subject = ""
            quoted_block = ""
            if prev and prev["raw_email"]:
                # Extract original subject from raw email for "Re:" prefix
                parsed = email_lib.message_from_string(prev["raw_email"])
                prev_subject = parsed.get("Subject", "")
                quoted_block = build_quoted_block(
                    prev["raw_email"], from_addr, prev["sent_at"]
                )

            subject  = render(step["subject"], enrollment)
            # If this is a follow-up subject, prefix with Re: to match thread
            if not subject.lower().startswith("re:"):
                thread_subject = f"Re: {prev_subject}" if prev_subject else subject
            else:
                thread_subject = subject

            body = render(step["body_template"], enrollment) + quoted_block

            print(f"  {SEND_MARKER} FU step{step['step_num']} -> {enrollment['email']}: {thread_subject}")

            if not DRY_RUN:
                try:
                    ses_id, raw, mime_id = send_via_ses(
                        ses, enrollment["email"], thread_subject, body,
                        from_addr, reply_to,
                        in_reply_to=in_reply_to,
                        references=in_reply_to,
                    )
                    record_send(conn, cid, enrollment, step, ses_id, raw, mime_id)
                except Exception as ex:
                    print(f"    ERROR: {ex}")
                    continue
            else:
                _, mime_id = build_mime(
                    enrollment["email"], thread_subject, body, from_addr,
                    reply_to, in_reply_to, in_reply_to,
                )
                record_send(conn, cid, enrollment, step,
                            "dry-run-ses-id", "", mime_id)

            cam_followups    += 1
            cam_sent         += 1
            budget           -= 1
            global_remaining -= 1
            time.sleep(SEND_DELAY_SECS)

        # ------------------------------------------------------------------- #
        #  Priority 2: new initial sends                                        #
        # ------------------------------------------------------------------- #
        if budget > 0 and global_remaining > 0:
            new_enrollments = get_new_enrollments(conn, cid, budget)
            print(f"  New enrollments queued: {len(new_enrollments)}")

            step = get_step(conn, cid, 1)
            if not step:
                print("  WARN: step 1 not defined — skipping new sends.")
            else:
                for enrollment in new_enrollments:
                    if budget <= 0 or global_remaining <= 0:
                        break

                    subject = render(step["subject"],       enrollment)
                    body    = render(step["body_template"], enrollment)

                    print(f"  {SEND_MARKER} NEW step1 -> {enrollment['email']}: {subject}")

                    if not DRY_RUN:
                        try:
                            ses_id, raw, mime_id = send_via_ses(
                                ses, enrollment["email"], subject, body,
                                from_addr, reply_to,
                            )
                            record_send(conn, cid, enrollment, step,
                                        ses_id, raw, mime_id)
                        except Exception as ex:
                            print(f"    ERROR: {ex}")
                            continue
                    else:
                        _, mime_id = build_mime(
                            enrollment["email"], subject, body, from_addr, reply_to,
                        )
                        record_send(conn, cid, enrollment, step,
                                    "dry-run-ses-id", "", mime_id)

                    cam_new          += 1
                    budget           -= 1
                    global_remaining -= 1
                    time.sleep(SEND_DELAY_SECS)

        print(f"  Sent: {cam_followups} follow-ups + {cam_new} new = {cam_followups + cam_new} total")
        totals["followups"] += cam_followups
        totals["new"]       += cam_new
        totals["campaigns"].append({"name": cname, "followups": cam_followups, "new": cam_new})

    # ----------------------------------------------------------------------- #
    #  Summary                                                                 #
    # ----------------------------------------------------------------------- #
    print("\n========== SUMMARY ==========")
    for c in totals["campaigns"]:
        total = c["followups"] + c["new"]
        print(f"  {c['name']:30s}  {c['followups']} FU + {c['new']} new = {total}")
    grand = totals["followups"] + totals["new"]
    print(f"  {'TOTAL':30s}  {totals['followups']} FU + {totals['new']} new = {grand} / {global_limit}")
    print("=============================")

    conn.close()


if __name__ == "__main__":
    run()
