"""
Schema migration: add campaign cadence tables.
Run once. Idempotent (uses IF NOT EXISTS / DO NOTHING).
"""
import psycopg2

DATABASE_URL = open(".env").read().split("DATABASE_URL=")[1].strip()
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Step 1: Add columns to campaigns...")
for col, defn in [
    ("daily_limit", "INT NOT NULL DEFAULT 100"),
    ("from_name",   "TEXT"),
    ("reply_to",    "TEXT"),
    ("from_email",  "TEXT"),
]:
    try:
        cur.execute(f"ALTER TABLE campaigns ADD COLUMN {col} {defn}")
        print(f"  Added campaigns.{col}")
    except psycopg2.errors.DuplicateColumn:
        conn.rollback()
        print(f"  campaigns.{col} already exists — skipped")

print("\nStep 2: Create campaign_steps...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS campaign_steps (
        id          SERIAL PRIMARY KEY,
        campaign_id INT REFERENCES campaigns(id) ON DELETE CASCADE,
        step_num    INT NOT NULL CHECK (step_num >= 1),
        subject     TEXT NOT NULL,
        body_template TEXT NOT NULL,
        wait_days   INT NOT NULL DEFAULT 3,
        UNIQUE (campaign_id, step_num)
    )
""")
print("  campaign_steps ready")

print("\nStep 3: Create campaign_enrollments...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS campaign_enrollments (
        id           SERIAL PRIMARY KEY,
        campaign_id  INT REFERENCES campaigns(id) ON DELETE CASCADE,
        email        TEXT REFERENCES emails(email),
        enrolled_at  TIMESTAMPTZ DEFAULT NOW(),
        current_step INT NOT NULL DEFAULT 1,
        status       TEXT NOT NULL DEFAULT 'active',
        next_send_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        UNIQUE (campaign_id, email)
    )
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_enrollments_campaign_status ON campaign_enrollments(campaign_id, status)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_enrollments_next_send ON campaign_enrollments(next_send_at) WHERE status = 'active'")
print("  campaign_enrollments ready")

print("\nStep 4: Create global_config...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS global_config (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
""")
cur.execute("""
    INSERT INTO global_config (key, value)
    VALUES ('max_daily_total', '500')
    ON CONFLICT DO NOTHING
""")
print("  global_config ready (max_daily_total=500)")

print("\nStep 5: Add enrollment_id and step_id to sends...")
for col, defn in [
    ("enrollment_id", "INT REFERENCES campaign_enrollments(id)"),
    ("step_id",       "INT REFERENCES campaign_steps(id)"),
]:
    try:
        cur.execute(f"ALTER TABLE sends ADD COLUMN {col} {defn}")
        print(f"  Added sends.{col}")
    except psycopg2.errors.DuplicateColumn:
        conn.rollback()
        print(f"  sends.{col} already exists — skipped")

print("\nStep 6: Add first_name, last_name, company to contacts...")
for col in ["first_name", "last_name", "company"]:
    try:
        cur.execute(f"ALTER TABLE contacts ADD COLUMN {col} TEXT")
        print(f"  Added contacts.{col}")
    except psycopg2.errors.DuplicateColumn:
        conn.rollback()
        print(f"  contacts.{col} already exists — skipped")

print("\nStep 7: Index sends by date for daily-count queries...")
cur.execute("CREATE INDEX IF NOT EXISTS idx_sends_campaign_date ON sends(campaign_id, sent_at) WHERE status = 'sent'")
cur.execute("CREATE INDEX IF NOT EXISTS idx_sends_date ON sends(sent_at) WHERE status = 'sent'")

conn.commit()
conn.close()
print("\nMigration complete.")
