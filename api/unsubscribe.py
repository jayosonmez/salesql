"""
Vercel serverless function: handles unsubscribe clicks.
URL: GET /api/unsubscribe?email=xxx&token=yyy

Validates HMAC token, suppresses the email + all sibling emails
for the same contact, then returns a confirmation page.
"""

import os
import hmac
import hashlib
import ssl
import pg8000.dbapi as pg8000
from urllib.parse import urlparse, parse_qs

DATABASE_URL       = os.environ["DATABASE_URL"]
UNSUBSCRIBE_SECRET = os.environ["UNSUBSCRIBE_SECRET"]


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


def suppress_email(cur, email, source="unsubscribe-link"):
    cur.execute("""
        INSERT INTO ses_suppression (email, reason, suppressed_at, source)
        VALUES (%s, 'UNSUBSCRIBE', NOW(), %s)
        ON CONFLICT (email) DO UPDATE
            SET reason='UNSUBSCRIBE', suppressed_at=NOW(), source=EXCLUDED.source
    """, (email, source))


def handler(request, response):
    # Parse query string
    qs = parse_qs(request.query)
    email = qs.get("email", [""])[0].strip().lower()
    token = qs.get("token", [""])[0].strip()

    # Validate HMAC token
    expected = hmac.new(
        UNSUBSCRIBE_SECRET.encode(), email.encode(), hashlib.sha256
    ).hexdigest()

    if not email or not hmac.compare_digest(token, expected):
        response.status_code = 400
        response.headers["Content-Type"] = "text/html"
        response.body = "<h2>Invalid unsubscribe link.</h2>"
        return

    conn = get_db()
    cur  = conn.cursor()

    # Suppress this email
    suppress_email(cur, email)

    # Find all other emails for the same contact and suppress those too
    cur.execute("""
        SELECT ce2.email
        FROM contact_emails ce1
        JOIN contact_emails ce2 ON ce2.contact_id = ce1.contact_id
        WHERE ce1.email = %s AND ce2.email != %s
    """, (email, email))
    sibling_emails = [row[0] for row in cur.fetchall()]
    for sibling in sibling_emails:
        suppress_email(cur, sibling, source="unsubscribe-sibling")

    conn.commit()
    conn.close()

    sibling_note = ""
    if sibling_emails:
        sibling_note = f"<p style='color:#888;font-size:13px;'>{len(sibling_emails)} associated email address(es) also unsubscribed.</p>"

    response.status_code = 200
    response.headers["Content-Type"] = "text/html"
    response.body = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Unsubscribed</title></head>
<body style="font-family:sans-serif;max-width:480px;margin:80px auto;text-align:center;color:#333;">
  <h2>You've been unsubscribed</h2>
  <p><strong>{email}</strong> has been removed from our mailing list.</p>
  <p>You will not receive any further emails from us.</p>
  {sibling_note}
</body>
</html>"""
