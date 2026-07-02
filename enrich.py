import requests
import json
import os
import time
import re
import sys
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = "efP55KczsExb0Mr8Q65FXwY0EE6sjVIA"
MV_API_KEY = "v8LMpDNKnk64I8YQInD1KqaU3"
SINGLE_URL = "https://api-public.salesql.com/v1/persons/enrich/"
BULK_URL   = "https://api-public.salesql.com/v1/persons/enrich/bulk"
ALLOWANCE_URL = "https://api-public.salesql.com/v1/allowance"
MV_URL = "https://api.millionverifier.com/api/v3/"
SL_DIR = "sl"
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Accept": "application/json"}
DATABASE_URL = open(".env").read().split("DATABASE_URL=")[1].strip()

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def slug(linkedin_url):
    match = re.search(r'linkedin\.com/in/([^/?]+)', linkedin_url)
    return match.group(1) if match else linkedin_url.replace("/", "_").replace(":", "")

def normalize(url):
    return url.replace('https://linkedin.com/', 'https://www.linkedin.com/')

def already_fetched(linkedin_url):
    return os.path.exists(os.path.join(SL_DIR, f"{slug(linkedin_url)}.json"))

def save(linkedin_url, data):
    path = os.path.join(SL_DIR, f"{slug(linkedin_url)}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def stamp_fetched(conn, linkedin_url):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO contacts (linkedin_profile, salesql_fetched_at, source)
        VALUES (%s, NOW(), 'salesql')
        ON CONFLICT (linkedin_profile) DO UPDATE SET salesql_fetched_at = NOW()
    """, (normalize(linkedin_url),))
    conn.commit()

def get_existing_emails(conn):
    cur = conn.cursor()
    cur.execute("SELECT email FROM emails")
    return set(r[0] for r in cur.fetchall())

def verify_email(email, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(MV_URL, params={"api": MV_API_KEY, "email": email}, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception:
            if attempt < retries - 1:
                time.sleep(3)
    return None

def verify_bulk(emails_list, workers=10):
    results = {}
    def _verify(email):
        return email, verify_email(email)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_verify, item["email"]): item for item in emails_list}
        for future in as_completed(futures):
            item = futures[future]
            email, result = future.result()
            results[email] = (result, item)
    return results

def verify_and_import(conn, new_emails, return_stats=False):
    if not new_emails:
        return {} if return_stats else None
    print(f"\n  Verifying {len(new_emails)} new email(s) via MillionVerifier (parallel)...")
    verified = verify_bulk(new_emails)

    email_rows = []   # for emails table
    ce_items = []     # for contact_emails table
    stats = {}        # linkedin_profile -> list of qualities

    for email, (result, item) in verified.items():
        profile = item["linkedin_profile"]
        if result:
            quality = result.get("quality")
            email_rows.append((
                email,
                quality,
                result.get("result"),
                result.get("free"),
                result.get("role")
            ))
            ce_items.append((profile, email, item.get("email_type", "")))
            print(f"    {email} -> {quality}")
            stats.setdefault(profile, []).append(quality)
        else:
            print(f"    {email} -> verification failed")

    if email_rows:
        cur = conn.cursor()

        # Upsert any missing contacts
        profiles = list(set(item[0] for item in ce_items))
        execute_values(cur,
            "INSERT INTO contacts (linkedin_profile, source) VALUES %s ON CONFLICT DO NOTHING",
            [(p, 'salesql') for p in profiles])

        # Insert into emails (MV data only)
        execute_values(cur, """
            INSERT INTO emails (email, quality, result, free, role)
            VALUES %s ON CONFLICT DO NOTHING
        """, email_rows)

        # Insert into contact_emails junction
        execute_values(cur, """
            INSERT INTO contact_emails (contact_id, email, email_type)
            SELECT c.id, %s, %s
            FROM contacts c WHERE c.linkedin_profile = %s
            ON CONFLICT DO NOTHING
        """, [(e, et, p) for p, e, et in ce_items])

        conn.commit()
        good = sum(1 for r in email_rows if r[1] == "good")
        print(f"  Imported {len(email_rows)} emails ({good} good) into database.")

    return stats if return_stats else None

def check_credits():
    r = requests.get(ALLOWANCE_URL, headers=HEADERS, timeout=10)
    if r.status_code == 200:
        data = r.json()
        print(f"Credits: {data.get('credits')}, Reset: {data.get('reset_date')}")
    else:
        print(f"Could not fetch credits: {r.status_code}")

def enrich_bulk(linkedin_urls, retries=3):
    payload = [{"linkedin_url": u} for u in linkedin_urls]
    for attempt in range(retries):
        r = requests.post(BULK_URL, json=payload, headers=HEADERS, timeout=60)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            wait = 2 ** attempt * 10
            print(f"  Rate limited. Waiting {wait}s...")
            time.sleep(wait)
        else:
            print(f"  Error {r.status_code}: {r.text[:200]}")
            return None
    return None

def run(linkedin_urls, batch_size=100, delay=2.0):
    pending = [u for u in linkedin_urls if not already_fetched(u)]
    print(f"Total: {len(linkedin_urls)} | Already fetched: {len(linkedin_urls)-len(pending)} | To fetch: {len(pending)}")

    conn = get_conn()
    existing_emails = get_existing_emails(conn)
    done = not_found = 0

    for i in range(0, len(pending), batch_size):
        batch = pending[i:i+batch_size]
        print(f"\nBatch {i//batch_size + 1}: {len(batch)} profiles...")
        results = enrich_bulk(batch)

        if results is None:
            print("  Batch failed, skipping.")
            not_found += len(batch)
            continue

        new_emails = []
        for url, result in zip(batch, results):
            save(url, result)
            stamp_fetched(conn, url)
            if isinstance(result, dict) and "error" not in result:
                done += 1
                for e in result.get("emails", []):
                    email = e["email"].lower().strip()
                    if "@openid." in email or email in existing_emails:
                        continue
                    new_emails.append({
                        "email": email,
                        "linkedin_profile": normalize(result.get("linkedin_url", url)),
                        "email_type": e.get("type", "")
                    })
                    existing_emails.add(email)
            else:
                not_found += 1

        print(f"  With data: {done} | Not found: {not_found}")
        verify_and_import(conn, new_emails)

        if i + batch_size < len(pending):
            time.sleep(delay)

    conn.close()
    print(f"\nDone. With data: {done}, Not found: {not_found}")

def get_unfetched_no_email(limit=100):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.linkedin_profile
        FROM contacts c
        LEFT JOIN contact_emails ce ON ce.contact_id = c.id
        WHERE ce.email IS NULL
        AND c.salesql_fetched_at IS NULL
        AND c.linkedin_profile IS NOT NULL
        LIMIT %s
    """, (limit,))
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows

def get_unfetched_risky_only(limit=100):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.linkedin_profile
        FROM contacts c
        JOIN contact_emails ce ON ce.contact_id = c.id
        JOIN emails e ON e.email = ce.email
        WHERE c.salesql_fetched_at IS NULL
        AND c.linkedin_profile IS NOT NULL
        GROUP BY c.linkedin_profile
        HAVING COUNT(1) FILTER (WHERE e.quality = 'good') = 0
           AND COUNT(1) FILTER (WHERE e.quality = 'risky') > 0
        LIMIT %s
    """, (limit,))
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows

def get_unfetched_bad_only(limit=2000):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.linkedin_profile
        FROM contacts c
        JOIN contact_emails ce ON ce.contact_id = c.id
        JOIN emails e ON e.email = ce.email
        WHERE c.salesql_fetched_at IS NULL
        AND c.linkedin_profile IS NOT NULL
        GROUP BY c.linkedin_profile
        HAVING COUNT(1) FILTER (WHERE e.quality = 'good') = 0
           AND COUNT(1) FILTER (WHERE e.quality = 'risky') = 0
           AND COUNT(1) FILTER (WHERE e.quality = 'bad') > 0
        LIMIT %s
    """, (limit,))
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows

def run_risky_generic(urls):
    if not urls:
        print("None found.")
        return

    pending = [u for u in urls if not already_fetched(u)]
    print(f"Already fetched in sl/: {len(urls)-len(pending)} | To fetch: {len(pending)}")

    conn = get_conn()
    existing_emails = get_existing_emails(conn)
    profiles_with_new = set()
    done = not_found = 0

    for i in range(0, len(pending), 100):
        batch = pending[i:i+100]
        print(f"\nBatch {i//100 + 1}: {len(batch)} profiles...")
        results = enrich_bulk(batch)

        if results is None:
            print("  Batch failed, skipping.")
            not_found += len(batch)
            continue

        new_emails = []
        for url, result in zip(batch, results):
            save(url, result)
            stamp_fetched(conn, url)
            if isinstance(result, dict) and "error" not in result:
                done += 1
                profile_url = normalize(result.get("linkedin_url", url))
                for e in result.get("emails", []):
                    email = e["email"].lower().strip()
                    if "@openid." in email or email in existing_emails:
                        continue
                    new_emails.append({
                        "email": email,
                        "linkedin_profile": profile_url,
                        "email_type": e.get("type", "")
                    })
                    existing_emails.add(email)
                    profiles_with_new.add(profile_url)
            else:
                not_found += 1

        print(f"  With data: {done} | Not found: {not_found}")
        if new_emails:
            verify_and_import(conn, new_emails)

        if i + 100 < len(pending):
            time.sleep(2)

    conn.close()

    print(f"\n--- Results ---")
    print(f"Profiles fetched: {len(pending)}")
    print(f"1. Profiles that returned new email addresses: {len(profiles_with_new)}")

    conn2 = get_conn()
    cur2 = conn2.cursor()
    good_count = 0
    for profile in profiles_with_new:
        cur2.execute("""
            SELECT COUNT(1) FROM contact_emails ce
            JOIN emails e ON e.email = ce.email
            JOIN contacts c ON c.id = ce.contact_id
            WHERE c.linkedin_profile = %s AND e.quality = 'good'
        """, (profile,))
        if cur2.fetchone()[0] > 0:
            good_count += 1
    conn2.close()
    print(f"2. Of those, contacts now with at least 1 good email: {good_count}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "credits":
        check_credits()
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        urls = list(dict.fromkeys(sys.argv[1:]))
        print(f"Enriching {len(urls)} specified profile(s)...")
        run(urls)
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "risky":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        urls = get_unfetched_risky_only(limit)
        print(f"Risky-only contacts to enrich: {len(urls)}")
        run_risky_generic(urls)
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "bad":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
        urls = get_unfetched_bad_only(limit)
        print(f"Bad-only contacts to enrich: {len(urls)}")
        run_risky_generic(urls)
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "noemail":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        urls = get_unfetched_no_email(limit)
        print(f"Enriching {len(urls)} no-email unfetched contacts...")
        run(urls)
        sys.exit(0)

    import pandas as pd
    df = pd.read_csv("contacts.csv")
    profiles = df['linkedin_profile'].dropna().unique().tolist()
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    print(f"Enriching up to {limit} profiles...")
    run(profiles[:limit])
