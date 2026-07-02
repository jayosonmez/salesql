import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

DATABASE_URL = open(".env").read().split("DATABASE_URL=")[1].strip()

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                linkedin_profile TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS emails (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                linkedin_profile TEXT REFERENCES profiles(linkedin_profile),
                quality TEXT,
                result TEXT,
                free BOOLEAN,
                role BOOLEAN
            );

            CREATE TABLE IF NOT EXISTS campaigns (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                subject TEXT,
                body_template TEXT,
                gmail_label TEXT,
                status TEXT DEFAULT 'draft',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS sends (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER REFERENCES campaigns(id),
                email TEXT REFERENCES emails(email),
                sequence_num INTEGER DEFAULT 1,
                ses_message_id TEXT,
                raw_email TEXT,
                gmail_insert_failed BOOLEAN DEFAULT FALSE,
                sent_at TIMESTAMPTZ,
                status TEXT DEFAULT 'pending'
            );

            CREATE TABLE IF NOT EXISTS ses_events (
                id SERIAL PRIMARY KEY,
                ses_message_id TEXT,
                event_type TEXT,
                event_subtype TEXT,
                occurred_at TIMESTAMPTZ,
                detail JSONB
            );

            CREATE TABLE IF NOT EXISTS ses_suppression (
                email TEXT PRIMARY KEY,
                reason TEXT,
                suppressed_at TIMESTAMPTZ,
                source TEXT DEFAULT 'ses'
            );

            CREATE TABLE IF NOT EXISTS gmail_events (
                id SERIAL PRIMARY KEY,
                gmail_message_id TEXT,
                thread_id TEXT,
                email TEXT,
                event_type TEXT,
                received_at TIMESTAMPTZ,
                snippet TEXT
            );

            CREATE TABLE IF NOT EXISTS suppressions (
                email TEXT PRIMARY KEY,
                reason TEXT,
                source TEXT,
                suppressed_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS unsubscribes (
                email TEXT PRIMARY KEY,
                unsubscribed_at TIMESTAMPTZ DEFAULT NOW(),
                source TEXT
            );
        """)
    conn.commit()
    print("Tables created.")

def import_profiles(conn):
    df = pd.read_csv("contacts.csv")
    profiles = df[['linkedin_profile']].dropna().drop_duplicates()
    rows = [(r['linkedin_profile'],) for _, r in profiles.iterrows()]
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO profiles (linkedin_profile) VALUES %s ON CONFLICT DO NOTHING",
            rows)
    conn.commit()
    print(f"Imported {len(rows)} profiles.")

def import_emails(conn):
    mv = pd.read_csv("MILLIONVERIFIER.COM.csv")
    mv['free'] = mv['free'].map({'yes': True, 'no': False}).fillna(False)
    mv['role'] = mv['role'].map({'yes': True, 'no': False}).fillna(False)
    mv = mv[['email', 'linkedin_profile', 'quality', 'result', 'free', 'role']].dropna(subset=['email'])
    rows = [(r['email'], r['linkedin_profile'], r['quality'], r['result'], bool(r['free']), bool(r['role'])) for _, r in mv.iterrows()]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO emails (email, linkedin_profile, quality, result, free, role)
            VALUES %s ON CONFLICT DO NOTHING
        """, rows)
    conn.commit()
    print(f"Imported {len(rows)} emails.")

def import_suppressions(conn):
    mv = pd.read_csv("MILLIONVERIFIER.COM.csv")
    bad = mv[mv['quality'] == 'bad'][['email']].dropna().drop_duplicates()
    rows = [(r['email'], 'bad_email', 'millionverifier') for _, r in bad.iterrows()]
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO suppressions (email, reason, source) VALUES %s ON CONFLICT DO NOTHING",
            rows)
    conn.commit()
    print(f"Pre-suppressed {len(rows)} bad emails.")

if __name__ == "__main__":
    conn = get_conn()
    create_tables(conn)
    import_profiles(conn)
    import_emails(conn)
    import_suppressions(conn)
    conn.close()
    print("\nMigration complete.")
