import sqlite3
import pandas as pd

DB_PATH = "metsulin.db"

def create_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS profiles (
            linkedin_profile TEXT PRIMARY KEY,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            linkedin_profile TEXT REFERENCES profiles(linkedin_profile),
            quality TEXT,
            result TEXT,
            free INTEGER,
            role INTEGER
        );

        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            subject TEXT,
            body_template TEXT,
            status TEXT DEFAULT 'draft',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS sends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER REFERENCES campaigns(id),
            email TEXT REFERENCES emails(email),
            sequence_num INTEGER DEFAULT 1,
            ses_message_id TEXT,
            raw_email TEXT,
            gmail_insert_failed INTEGER DEFAULT 0,
            sent_at DATETIME,
            status TEXT DEFAULT 'pending'
        );

        CREATE TABLE IF NOT EXISTS ses_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ses_message_id TEXT,
            event_type TEXT,
            event_subtype TEXT,
            occurred_at DATETIME,
            detail TEXT
        );

        CREATE TABLE IF NOT EXISTS ses_suppression (
            email TEXT PRIMARY KEY,
            reason TEXT,
            suppressed_at DATETIME,
            source TEXT DEFAULT 'ses'
        );

        CREATE TABLE IF NOT EXISTS gmail_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gmail_message_id TEXT,
            thread_id TEXT,
            email TEXT REFERENCES emails(email),
            event_type TEXT,
            received_at DATETIME,
            snippet TEXT
        );

        CREATE TABLE IF NOT EXISTS suppressions (
            email TEXT PRIMARY KEY,
            reason TEXT,
            source TEXT,
            suppressed_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS unsubscribes (
            email TEXT PRIMARY KEY,
            unsubscribed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            source TEXT
        );
    """)
    print("Tables created.")

def import_profiles(conn):
    df = pd.read_csv("contacts.csv")
    profiles = df[['linkedin_profile']].dropna().drop_duplicates()
    profiles.to_sql("profiles", conn, if_exists="append", index=False)
    print(f"Imported {len(profiles)} profiles.")

def import_emails(conn):
    mv = pd.read_csv("MILLIONVERIFIER.COM.csv")
    mv = mv.rename(columns={"free": "free", "role": "role"})
    mv['free'] = mv['free'].map({'yes': 1, 'no': 0}).fillna(0).astype(int) if 'free' in mv.columns else 0
    mv['role'] = mv['role'].map({'yes': 1, 'no': 0}).fillna(0).astype(int) if 'role' in mv.columns else 0
    mv = mv[['email', 'linkedin_profile', 'quality', 'result', 'free', 'role']].dropna(subset=['email'])
    mv.to_sql("emails", conn, if_exists="append", index=False)
    print(f"Imported {len(mv)} emails.")

def import_suppressions(conn):
    # Pre-suppress all bad emails from MillionVerifier
    mv = pd.read_csv("MILLIONVERIFIER.COM.csv")
    bad = mv[mv['quality'] == 'bad'][['email']].dropna().drop_duplicates()
    bad['reason'] = 'bad_email'
    bad['source'] = 'millionverifier'
    bad.to_sql("suppressions", conn, if_exists="append", index=False)
    print(f"Pre-suppressed {len(bad)} bad emails from MillionVerifier.")

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    create_tables(conn)
    import_profiles(conn)
    import_emails(conn)
    import_suppressions(conn)
    conn.commit()
    conn.close()
    print(f"\nDatabase ready: {DB_PATH}")
