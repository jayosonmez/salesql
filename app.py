"""
Flask UI for Metsulin email cadence system.
Run: python app.py
"""

from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone

DATABASE_URL = open(".env").read().split("DATABASE_URL=")[1].strip()

app = Flask(__name__)
app.secret_key = "metsulin-secret-2026"

# --------------------------------------------------------------------------- #
#  DB helpers                                                                  #
# --------------------------------------------------------------------------- #

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def today_filter():
    """SQL fragment: filter sent_at to today in PDT."""
    return "(sent_at AT TIME ZONE 'America/Los_Angeles')::DATE = (NOW() AT TIME ZONE 'America/Los_Angeles')::DATE"


# --------------------------------------------------------------------------- #
#  Dashboard                                                                   #
# --------------------------------------------------------------------------- #

@app.route("/")
def dashboard():
    conn = get_conn()
    cur  = conn.cursor()

    # Global limit
    cur.execute("SELECT value FROM global_config WHERE key = 'max_daily_total'")
    row = cur.fetchone()
    global_limit = int(row["value"]) if row else 500

    # Global sent today
    cur.execute(f"SELECT COUNT(1) AS n FROM sends WHERE status='sent' AND {today_filter()}")
    global_sent = cur.fetchone()["n"]

    # Follow-ups vs new today (sequence_num=1 means initial)
    cur.execute(f"""
        SELECT
            COUNT(1) FILTER (WHERE sequence_num > 1) AS followups,
            COUNT(1) FILTER (WHERE sequence_num = 1) AS new
        FROM sends WHERE status='sent' AND {today_filter()}
    """)
    row = cur.fetchone()
    followups_today = row["followups"]
    new_today       = row["new"]

    # Per-campaign breakdown
    cur.execute(f"""
        SELECT
            c.id,
            c.name,
            c.status,
            c.daily_limit,
            COALESCE(SUM(CASE WHEN s.sequence_num > 1 AND {today_filter()} THEN 1 END), 0) AS followups_today,
            COALESCE(SUM(CASE WHEN s.sequence_num = 1 AND {today_filter()} THEN 1 END), 0) AS new_today,
            COALESCE(SUM(CASE WHEN {today_filter()} THEN 1 END), 0) AS total_today
        FROM campaigns c
        LEFT JOIN sends s ON s.campaign_id = c.id AND s.status = 'sent'
        GROUP BY c.id, c.name, c.status, c.daily_limit
        ORDER BY c.id
    """)
    campaigns_raw = cur.fetchall()

    campaign_rows = []
    active_count  = 0
    for r in campaigns_raw:
        if r["status"] == "active":
            active_count += 1
        pct = int(r["total_today"] / r["daily_limit"] * 100) if r["daily_limit"] else 0
        campaign_rows.append({**r, "pct": min(pct, 100)})

    # Upcoming follow-ups (next 7 days)
    cur.execute("""
        SELECT
            camp.name AS campaign_name,
            e.email,
            e.current_step,
            e.next_send_at
        FROM campaign_enrollments e
        JOIN campaigns camp ON camp.id = e.campaign_id
        WHERE e.status = 'active'
          AND e.current_step > 1
          AND e.next_send_at BETWEEN NOW() AND NOW() + INTERVAL '7 days'
        ORDER BY e.next_send_at
        LIMIT 50
    """)
    upcoming_followups = cur.fetchall()

    conn.close()
    return render_template("dashboard.html",
        global_limit=global_limit,
        global_sent=global_sent,
        followups_today=followups_today,
        new_today=new_today,
        active_campaigns=active_count,
        campaign_rows=campaign_rows,
        upcoming_followups=upcoming_followups,
    )


# --------------------------------------------------------------------------- #
#  Campaigns list                                                               #
# --------------------------------------------------------------------------- #

@app.route("/campaigns")
def campaigns():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
            c.id, c.name, c.status, c.daily_limit,
            COUNT(DISTINCT e.id)                                          AS total_enrolled,
            COUNT(DISTINCT e.id) FILTER (WHERE e.status = 'active')      AS active_count,
            COUNT(DISTINCT e.id) FILTER (WHERE e.status = 'completed')   AS completed_count,
            COUNT(DISTINCT cs.id)                                         AS step_count
        FROM campaigns c
        LEFT JOIN campaign_enrollments e  ON e.campaign_id = c.id
        LEFT JOIN campaign_steps       cs ON cs.campaign_id = c.id
        GROUP BY c.id, c.name, c.status, c.daily_limit
        ORDER BY c.id
    """)
    rows = cur.fetchall()
    conn.close()
    return render_template("campaigns.html", campaigns=rows)


# --------------------------------------------------------------------------- #
#  Create campaign                                                              #
# --------------------------------------------------------------------------- #

@app.route("/campaigns/new", methods=["GET", "POST"])
def campaign_new():
    if request.method == "POST":
        f = request.form
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO campaigns (name, from_name, from_email, reply_to, daily_limit, gmail_label, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            f["name"],
            f.get("from_name") or None,
            f.get("from_email") or None,
            f.get("reply_to")  or None,
            int(f.get("daily_limit", 100)),
            f.get("gmail_label") or None,
            f.get("status", "draft"),
        ))
        cid = cur.fetchone()["id"]
        conn.commit()
        conn.close()
        flash(f"Campaign '{f['name']}' created.", "success")
        return redirect(url_for("campaign_steps", campaign_id=cid))
    return render_template("campaign_form.html", campaign=None)


# --------------------------------------------------------------------------- #
#  Edit campaign                                                                #
# --------------------------------------------------------------------------- #

@app.route("/campaigns/<int:campaign_id>", methods=["GET", "POST"])
def campaign_edit(campaign_id):
    conn = get_conn()
    cur  = conn.cursor()
    if request.method == "POST":
        f = request.form
        cur.execute("""
            UPDATE campaigns
            SET name=%s, from_name=%s, from_email=%s, reply_to=%s,
                daily_limit=%s, gmail_label=%s, status=%s
            WHERE id=%s
        """, (
            f["name"],
            f.get("from_name") or None,
            f.get("from_email") or None,
            f.get("reply_to")  or None,
            int(f.get("daily_limit", 100)),
            f.get("gmail_label") or None,
            f.get("status", "draft"),
            campaign_id,
        ))
        conn.commit()
        conn.close()
        flash("Campaign updated.", "success")
        return redirect(url_for("campaigns"))

    cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
    campaign = cur.fetchone()
    conn.close()
    if not campaign:
        flash("Campaign not found.", "danger")
        return redirect(url_for("campaigns"))
    return render_template("campaign_form.html", campaign=campaign)


# --------------------------------------------------------------------------- #
#  Steps                                                                        #
# --------------------------------------------------------------------------- #

@app.route("/campaigns/<int:campaign_id>/steps")
def campaign_steps(campaign_id):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
    campaign = cur.fetchone()
    cur.execute("SELECT * FROM campaign_steps WHERE campaign_id=%s ORDER BY step_num", (campaign_id,))
    steps = cur.fetchall()
    conn.close()
    return render_template("steps.html", campaign=campaign, steps=steps, edit_step=None)


@app.route("/campaigns/<int:campaign_id>/steps/add", methods=["POST"])
def step_add(campaign_id):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(step_num), 0)+1 AS next FROM campaign_steps WHERE campaign_id=%s", (campaign_id,))
    next_num = cur.fetchone()["next"]
    f = request.form
    cur.execute("""
        INSERT INTO campaign_steps (campaign_id, step_num, subject, body_template, wait_days)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        campaign_id,
        next_num,
        f["subject"],
        f["body_template"],
        int(f.get("wait_days", 3)) if next_num > 1 else 0,
    ))
    conn.commit()
    conn.close()
    flash(f"Step {next_num} added.", "success")
    return redirect(url_for("campaign_steps", campaign_id=campaign_id))


@app.route("/campaigns/<int:campaign_id>/steps/<int:step_id>/edit", methods=["GET", "POST"])
def step_edit(campaign_id, step_id):
    conn = get_conn()
    cur  = conn.cursor()
    if request.method == "POST":
        f = request.form
        cur.execute("""
            UPDATE campaign_steps SET subject=%s, body_template=%s, wait_days=%s WHERE id=%s
        """, (f["subject"], f["body_template"], int(f.get("wait_days", 3)), step_id))
        conn.commit()
        conn.close()
        flash("Step updated.", "success")
        return redirect(url_for("campaign_steps", campaign_id=campaign_id))

    cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
    campaign = cur.fetchone()
    cur.execute("SELECT * FROM campaign_steps WHERE campaign_id=%s ORDER BY step_num", (campaign_id,))
    steps = cur.fetchall()
    cur.execute("SELECT * FROM campaign_steps WHERE id=%s", (step_id,))
    edit_step = cur.fetchone()
    conn.close()
    return render_template("steps.html", campaign=campaign, steps=steps, edit_step=edit_step)


@app.route("/campaigns/<int:campaign_id>/steps/<int:step_id>/delete", methods=["POST"])
def step_delete(campaign_id, step_id):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT step_num FROM campaign_steps WHERE id=%s", (step_id,))
    row = cur.fetchone()
    if row:
        deleted_num = row["step_num"]
        cur.execute("DELETE FROM campaign_steps WHERE id=%s", (step_id,))
        # Renumber steps above the deleted one
        cur.execute("""
            UPDATE campaign_steps SET step_num = step_num - 1
            WHERE campaign_id=%s AND step_num > %s
        """, (campaign_id, deleted_num))
        conn.commit()
    conn.close()
    flash("Step deleted.", "success")
    return redirect(url_for("campaign_steps", campaign_id=campaign_id))


# --------------------------------------------------------------------------- #
#  Enroll contacts                                                              #
# --------------------------------------------------------------------------- #

@app.route("/campaigns/<int:campaign_id>/enroll", methods=["GET", "POST"])
def enroll(campaign_id):
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
    campaign = cur.fetchone()

    if request.method == "POST":
        emails = request.form.getlist("emails")
        enrolled = 0
        for email in emails:
            try:
                cur.execute("""
                    INSERT INTO campaign_enrollments (campaign_id, email, enrolled_at, current_step, status)
                    VALUES (%s, %s, NOW(), 1, 'active')
                    ON CONFLICT (campaign_id, email) DO NOTHING
                """, (campaign_id, email))
                if cur.rowcount > 0:
                    enrolled += 1
            except Exception:
                conn.rollback()
                continue
        conn.commit()
        flash(f"Enrolled {enrolled} contact(s) into '{campaign['name']}'.", "success")
        return redirect(url_for("enroll", campaign_id=campaign_id))

    q      = request.args.get("q", "").strip()
    source = request.args.get("source", "").strip()

    # Already enrolled count
    cur.execute("SELECT COUNT(1) AS n FROM campaign_enrollments WHERE campaign_id=%s", (campaign_id,))
    already_enrolled = cur.fetchone()["n"]

    # Build eligible contacts query
    # Eligible = good email, not suppressed, not unsubscribed, not already enrolled in this campaign
    base_filters = """
        AND e.quality = 'good'
        AND ce.email NOT IN (SELECT email FROM unsubscribes)
        AND ce.email NOT IN (SELECT email FROM suppressions)
        AND ce.email NOT IN (SELECT email FROM ses_suppression)
        AND NOT EXISTS (
            SELECT 1 FROM campaign_enrollments en
            WHERE en.campaign_id = %s AND en.email = ce.email
        )
    """

    params = [campaign_id]

    search_filter = ""
    if q:
        search_filter = """
            AND (
                LOWER(c.first_name) LIKE %s
                OR LOWER(c.last_name) LIKE %s
                OR LOWER(ce.email) LIKE %s
                OR LOWER(c.company) LIKE %s
            )
        """
        like = f"%{q.lower()}%"
        params += [like, like, like, like]

    source_filter = ""
    if source:
        source_filter = "AND c.source = %s"
        params.append(source)

    count_sql = f"""
        SELECT COUNT(DISTINCT ce.email) AS n
        FROM contact_emails ce
        JOIN emails e ON e.email = ce.email
        JOIN contacts c ON c.id = ce.contact_id
        WHERE 1=1 {base_filters} {search_filter} {source_filter}
    """
    cur.execute(count_sql, params)
    total_eligible = cur.fetchone()["n"]

    contacts_sql = f"""
        SELECT DISTINCT ON (ce.email)
            ce.email,
            c.first_name,
            c.last_name,
            c.company,
            c.source
        FROM contact_emails ce
        JOIN emails e ON e.email = ce.email
        JOIN contacts c ON c.id = ce.contact_id
        WHERE 1=1 {base_filters} {search_filter} {source_filter}
        ORDER BY ce.email
        LIMIT 200
    """
    cur.execute(contacts_sql, params)
    contacts = cur.fetchall()

    conn.close()
    return render_template("enroll.html",
        campaign=campaign,
        contacts=contacts,
        already_enrolled=already_enrolled,
        total_eligible=total_eligible,
        q=q,
        source=source,
    )


# --------------------------------------------------------------------------- #
#  Global config                                                                #
# --------------------------------------------------------------------------- #

@app.route("/settings", methods=["GET", "POST"])
def settings():
    conn = get_conn()
    cur  = conn.cursor()
    if request.method == "POST":
        for key, val in [
            ("max_daily_total", request.form["max_daily_total"]),
            ("test_emails",     request.form.get("test_emails", "")),
        ]:
            cur.execute("""
                INSERT INTO global_config (key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, val))
        conn.commit()
        flash("Settings updated.", "success")
        conn.close()
        return redirect(url_for("dashboard"))

    cur.execute("SELECT key, value FROM global_config WHERE key IN ('max_daily_total', 'test_emails')")
    config = {r["key"]: r["value"] for r in cur.fetchall()}
    conn.close()
    return render_template("settings.html",
        max_daily_total=config.get("max_daily_total", 500),
        test_emails=config.get("test_emails", ""),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
