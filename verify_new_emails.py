import requests
import json
import glob
import psycopg2
import time

MV_API_KEY = "v8LMpDNKnk64I8YQInD1KqaU3"
MV_URL = "https://api.millionverifier.com/api/v3/"
DATABASE_URL = open(".env").read().split("DATABASE_URL=")[1].strip()

def get_existing_emails():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT email FROM emails")
    existing = set(r[0] for r in cur.fetchall())
    conn.close()
    return existing

def collect_new_emails(existing_emails):
    new_emails = []
    for f in glob.glob("sl/*.json"):
        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)
        if "error" in data:
            continue
        profile = data.get("linkedin_url", "")
        for e in data.get("emails", []):
            email = e["email"].lower().strip()
            if email not in existing_emails:
                new_emails.append({
                    "email": email,
                    "linkedin_profile": profile,
                    "email_type": e.get("type", "")
                })
    return new_emails

def verify_email(email):
    r = requests.get(MV_URL, params={"api": MV_API_KEY, "email": email}, timeout=15)
    if r.status_code == 200:
        return r.json()
    return None

if __name__ == "__main__":
    existing = get_existing_emails()
    new_emails = collect_new_emails(existing)
    print(f"New emails to verify: {len(new_emails)}")

    results = []
    for i, item in enumerate(new_emails, 1):
        email = item["email"]
        print(f"[{i}/{len(new_emails)}] Verifying {email}...")
        result = verify_email(email)
        if result:
            item.update({
                "quality": result.get("quality"),
                "result": result.get("result"),
                "free": result.get("free"),
                "role": result.get("role")
            })
            print(f"  {result.get('quality')} / {result.get('result')}")
        else:
            item.update({"quality": None, "result": None, "free": None, "role": None})
            print(f"  Failed to verify")
        results.append(item)
        time.sleep(0.5)

    # Save results to JSON for review before importing
    with open("new_emails_verified.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\nDone. Results saved to new_emails_verified.json")
    good = sum(1 for r in results if r.get("quality") == "good")
    risky = sum(1 for r in results if r.get("quality") == "risky")
    bad = sum(1 for r in results if r.get("quality") == "bad")
    print(f"Good: {good} | Risky: {risky} | Bad: {bad}")
