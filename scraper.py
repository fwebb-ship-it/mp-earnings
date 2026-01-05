#!/usr/bin/env python3
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import json
import hashlib

MYSOCIETY_BASE = "https://pages.mysociety.org/parl_register_interests/data/commons_rmfi/latest"
CATEGORIES = {
    "ad_hoc_payments": "category_1.1.csv",
    "ongoing_employment": "category_1.2.csv",
    "donations": "category_2.csv",
    "gifts_uk": "category_3.csv",
    "visits": "category_4.csv",
    "gifts_foreign": "category_5.csv",
    "property": "category_6.csv",
    "shareholdings": "category_7.csv",
    "miscellaneous": "category_8.csv",
    "overall": "overall.csv"
}

DB_PATH = Path("data/mp_earnings.db")
DATA_DIR = Path("data")
OUTPUT_DIR = Path("outputs")

def setup_directories():
    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

def fetch_csv(category_key):
    url = f"{MYSOCIETY_BASE}/{CATEGORIES[category_key]}"
    print(f"Fetching {category_key} from {url}")
    try:
        df = pd.read_csv(url)
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def fetch_all_data():
    data = {}
    for key in CATEGORIES:
        df = fetch_csv(key)
        if not df.empty:
            data[key] = df
            print(f"  -> {len(df)} records")
    return data

def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS interests (
        id INTEGER PRIMARY KEY, hash TEXT UNIQUE, category TEXT, member TEXT,
        party TEXT, mnis_id INTEGER, twfy_id TEXT, summary TEXT, value REAL,
        payer_name TEXT, received_date TEXT, registered TEXT, published TEXT,
        raw_json TEXT, first_seen TEXT, last_seen TEXT)""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, interest_hash TEXT,
        change_type TEXT, detected_at TEXT, old_value TEXT, new_value TEXT,
        member TEXT, category TEXT)""")
    conn.commit()
    return conn

def compute_hash(row):
    key_fields = ['member', 'category', 'summary', 'value', 'payer_name', 'received_date']
    hash_str = '|'.join(str(row.get(f, '')) for f in key_fields)
    return hashlib.md5(hash_str.encode()).hexdigest()

def process_earnings_data(df, category):
    records = []
    for _, row in df.iterrows():
        record = {
            'category': category, 'member': row.get('member'), 'party': row.get('party'),
            'mnis_id': row.get('mnis_id'), 'twfy_id': row.get('twfy_id'),
            'summary': row.get('summary'), 'value': row.get('value'),
            'payer_name': row.get('payer_name') or row.get('donor_name'),
            'received_date': row.get('received_date'), 'registered': row.get('registered'),
            'published': row.get('published'), 'raw_json': json.dumps(row.to_dict(), default=str)
        }
        record['hash'] = compute_hash(record)
        records.append(record)
    return records

def sync_to_database(data, conn):
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    stats = {'new': 0, 'total': 0}
    for category, df in data.items():
        for record in process_earnings_data(df, category):
            stats['total'] += 1
            cursor.execute("SELECT hash FROM interests WHERE hash = ?", (record['hash'],))
            if cursor.fetchone() is None:
                cursor.execute("""INSERT INTO interests (hash, category, member, party, mnis_id,
                    twfy_id, summary, value, payer_name, received_date, registered, published,
                    raw_json, first_seen, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (record['hash'], record['category'], record['member'], record['party'],
                     record['mnis_id'], record['twfy_id'], record['summary'], record['value'],
                     record['payer_name'], record['received_date'], record['registered'],
                     record['published'], record['raw_json'], now, now))
                cursor.execute("""INSERT INTO changes (interest_hash, change_type, detected_at,
                    new_value, member, category) VALUES (?, 'new', ?, ?, ?, ?)""",
                    (record['hash'], now, record['summary'], record['member'], record['category']))
                stats['new'] += 1
            else:
                cursor.execute("UPDATE interests SET last_seen = ? WHERE hash = ?", (now, record['hash']))
    conn.commit()
    return stats

def get_top_earners(conn, limit=50):
    return pd.read_sql_query("""SELECT member, party, SUM(value) as total_earnings,
        COUNT(*) as num_interests FROM interests WHERE value IS NOT NULL
        GROUP BY member ORDER BY total_earnings DESC LIMIT ?""", conn, params=(limit,))

def export_reports(conn):
    get_top_earners(conn).to_csv(OUTPUT_DIR / "top_earners.csv", index=False)
    pd.read_sql_query("SELECT * FROM changes ORDER BY detected_at DESC LIMIT 500", conn).to_csv(
        OUTPUT_DIR / "recent_changes.csv", index=False)
    for cat in ['ad_hoc_payments', 'ongoing_employment', 'donations', 'gifts_uk']:
        pd.read_sql_query(f"SELECT * FROM interests WHERE category = '{cat}'", conn).to_csv(
            OUTPUT_DIR / f"{cat}_full.csv", index=False)
    print(f"Reports exported to {OUTPUT_DIR}/")

def run_sync():
    print("=" * 60)
    print(f"MP Earnings Scraper - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    setup_directories()
    conn = init_database()
    print("\n📥 Fetching data from mySociety...")
    data = fetch_all_data()
    print("\n💾 Syncing to database...")
    stats = sync_to_database(data, conn)
    print(f"\n✅ Sync complete! Total: {stats['total']}, New: {stats['new']}")
    print("\n📊 Generating reports...")
    export_reports(conn)
    conn.close()

if __name__ == "__main__":
    run_sync()
