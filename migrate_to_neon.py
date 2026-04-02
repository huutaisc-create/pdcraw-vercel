"""
migrate_to_neon.py — Chạy 1 lần trên máy local để copy data từ MariaDB → Neon.

Cách dùng:
    pip install mysql-connector-python psycopg2-binary
    python migrate_to_neon.py
"""
import mysql.connector
import psycopg2
import psycopg2.extras
import json
import sys

# ── Config ─────────────────────────────────────────────────────────────────
# Đảm bảo password '123456' là đúng với MariaDB local của bạn
MARIADB = {
    'host': '127.0.0.1', 
    'user': 'root', 
    'password': '123456',
    'database': 'pdcraw', 
    'charset': 'utf8mb4', 
    'autocommit': True
}

# Đã cập nhật chuỗi kết nối Neon mới của bạn
NEON_URL = "postgresql://neondb_owner:npg_2x6udHZMytVl@ep-fragrant-hill-a1md82d2-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

# ── Migrate stories ─────────────────────────────────────────────────────────
def migrate():
    try:
        print("[*] Connecting to MariaDB...")
        src = mysql.connector.connect(**MARIADB)
        src_cur = src.cursor(dictionary=True)

        print("[*] Connecting to Neon...")
        dst = psycopg2.connect(NEON_URL)
        dst_cur = dst.cursor()
    except Exception as e:
        print(f"[!] Không thể kết nối database: {e}")
        sys.exit(1)

    # ── stories ────────────────────────────────────────────────────────────
    print("[*] Reading stories from MariaDB...")
    src_cur.execute("SELECT * FROM stories")
    rows = src_cur.fetchall()
    print(f"    {len(rows)} stories found.")

    inserted = 0
    skipped  = 0
    for row in rows:
        try:
            dst_cur.execute("""
                INSERT INTO stories (
                    id, title, slug, url, source, category, chapters,
                    downloaded_chapters, mapped_count, crawl_status, book_status,
                    last_account_idx, admin_control, last_updated,
                    actual_chapters, uploaded_chapters, author,
                    views, likes, cover_url, rating
                ) VALUES (
                    %(id)s, %(title)s, %(slug)s, %(url)s, %(source)s, %(category)s, %(chapters)s,
                    %(downloaded_chapters)s, %(mapped_count)s, %(crawl_status)s, %(book_status)s,
                    %(last_account_idx)s, %(admin_control)s, %(last_updated)s,
                    %(actual_chapters)s, %(uploaded_chapters)s, %(author)s,
                    %(views)s, %(likes)s, %(cover_url)s, %(rating)s
                )
                ON CONFLICT (slug) DO UPDATE SET
                    title               = EXCLUDED.title,
                    chapters            = EXCLUDED.chapters,
                    downloaded_chapters = EXCLUDED.downloaded_chapters,
                    crawl_status        = EXCLUDED.crawl_status,
                    last_updated        = EXCLUDED.last_updated,
                    uploaded_chapters   = EXCLUDED.uploaded_chapters
            """, row)
            inserted += 1
        except Exception as e:
            print(f"  [!] Skip story id={row.get('id')}: {e}")
            skipped += 1

    # Reset sequence sau khi insert với id cụ thể (để các bản ghi mới không bị trùng ID)
    try:
        dst_cur.execute("SELECT setval('stories_id_seq', (SELECT MAX(id) FROM stories))")
    except:
        pass

    # ── scraper_accounts_status ────────────────────────────────────────────
    print("[*] Reading accounts status...")
    src_cur.execute("SELECT * FROM scraper_accounts_status")
    acc_rows = src_cur.fetchall()
    for row in acc_rows:
        try:
            dst_cur.execute("""
                INSERT INTO scraper_accounts_status (account_email, account_index, locked_by, last_heartbeat)
                VALUES (%(account_email)s, %(account_index)s, %(locked_by)s, %(last_heartbeat)s)
                ON CONFLICT (account_email) DO UPDATE SET
                    locked_by      = EXCLUDED.locked_by,
                    last_heartbeat = EXCLUDED.last_heartbeat
            """, row)
        except Exception as e:
            print(f"  [!] Skip account {row.get('account_email')}: {e}")

    dst.commit()
    src.close()
    dst.close()

    print(f"\n[✓] Migration done!")
    print(f"    Stories: {inserted} inserted, {skipped} skipped")
    print(f"    Accounts: {len(acc_rows)} processed")

if __name__ == '__main__':
    migrate()