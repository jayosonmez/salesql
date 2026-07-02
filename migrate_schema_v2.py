import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = open(".env").read().split("DATABASE_URL=")[1].strip()
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Step 1: Create contacts table...")
cur.execute("""
    CREATE TABLE contacts (
        id SERIAL PRIMARY KEY,
        linkedin_profile TEXT UNIQUE,
        source TEXT,
        salesql_fetched_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
""")

print("Step 2: Populate contacts from profiles...")
cur.execute("""
    INSERT INTO contacts (linkedin_profile, source, salesql_fetched_at, created_at)
    SELECT linkedin_profile, source, salesql_fetched_at, created_at
    FROM profiles
""")
print(f"  Inserted {cur.rowcount} contacts.")

print("Step 3: Create new clean emails table...")
cur.execute("""
    CREATE TABLE emails_new (
        email TEXT PRIMARY KEY,
        quality TEXT,
        result TEXT,
        free BOOLEAN,
        role BOOLEAN,
        verified_at TIMESTAMPTZ DEFAULT NOW()
    )
""")

print("Step 4: Populate emails_new (unique emails, MV data only)...")
cur.execute("""
    INSERT INTO emails_new (email, quality, result, free, role)
    SELECT DISTINCT ON (email) email, quality, result, free, role
    FROM emails
    ORDER BY email
""")
print(f"  Inserted {cur.rowcount} emails.")

print("Step 5: Create contact_emails junction table...")
cur.execute("""
    CREATE TABLE contact_emails (
        contact_id INTEGER REFERENCES contacts(id),
        email TEXT REFERENCES emails_new(email),
        email_type TEXT,
        is_primary BOOLEAN DEFAULT FALSE,
        PRIMARY KEY (contact_id, email)
    )
""")

print("Step 6: Populate contact_emails...")
cur.execute("""
    INSERT INTO contact_emails (contact_id, email, email_type)
    SELECT c.id, e.email, e.email_type
    FROM emails e
    JOIN contacts c ON c.linkedin_profile = e.linkedin_profile
    WHERE e.linkedin_profile IS NOT NULL
    ON CONFLICT DO NOTHING
""")
print(f"  Inserted {cur.rowcount} contact_email links.")

print("Step 7: Update sends table to reference emails_new...")
# sends.email is a text FK — check if it exists as FK constraint first
cur.execute("""
    SELECT constraint_name FROM information_schema.table_constraints
    WHERE table_name = 'sends' AND constraint_type = 'FOREIGN KEY'
""")
fk_constraints = [r[0] for r in cur.fetchall()]
for fk in fk_constraints:
    cur.execute(f"ALTER TABLE sends DROP CONSTRAINT IF EXISTS {fk}")

print("Step 8: Swap table names...")
cur.execute("ALTER TABLE emails RENAME TO emails_old")
cur.execute("ALTER TABLE emails_new RENAME TO emails")

print("Step 9: Restore sends FK to new emails table...")
cur.execute("""
    ALTER TABLE sends ADD CONSTRAINT sends_email_fkey
    FOREIGN KEY (email) REFERENCES emails(email)
""")

print("Step 10: Drop old tables...")
cur.execute("DROP TABLE emails_old")
cur.execute("DROP TABLE profiles")

conn.commit()
print("\nMigration complete.")

# Verify
cur.execute("SELECT COUNT(1) FROM contacts")
print(f"contacts: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(1) FROM emails")
print(f"emails: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(1) FROM contact_emails")
print(f"contact_emails: {cur.fetchone()[0]}")

conn.close()
