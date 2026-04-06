"""
rename_folders.py
- Kết nối DB lấy title + slug của tất cả truyện đã crawl
- Tính tên thư mục mới từ title (safe_folder_name)
- Rename folder cũ (slug-based) sang tên mới (title-based) trên disk
- Chạy 1 lần: python rename_folders.py [--dry-run]
"""

import os, sys, re, unicodedata, urllib.parse, argparse

sys.stdout.reconfigure(encoding='utf-8')

IMPORT_DIR = r'D:\Webtruyen\pdcraw\data_import'

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'api'))
from _db import get_conn


def safe_folder_name(title):
    """Giống hệt hàm trong wiki_scraper_agent.py."""
    name = unicodedata.normalize('NFD', title)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = re.sub(r'[\\/*?:"<>|]', '', name)
    name = re.sub(r'[^\w\s-]', '', name).strip()
    name = re.sub(r'[\s_]+', '-', name)
    name = re.sub(r'-+', '-', name).strip('-')
    name = name.lower()
    return name[:80] if name else 'unknown'


def find_old_folder(slug, title):
    """Tìm folder hiện tại trên disk theo các pattern có thể có."""
    candidates = [
        slug,                           # slug gốc từ DB
        urllib.parse.unquote(slug),     # decode %7E → ~
        safe_folder_name(title),        # title-based (nếu đã rename trước)
    ]
    for c in candidates:
        path = os.path.join(IMPORT_DIR, c)
        if os.path.isdir(path):
            return path, c
    return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='Chỉ xem, không đổi tên')
    args = parser.parse_args()
    dry = args.dry_run

    print(f"{'[DRY RUN] ' if dry else ''}Bắt đầu rename folders...")
    print(f"IMPORT_DIR: {IMPORT_DIR}\n")

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT id, slug, title, downloaded_chapters
        FROM stories
        WHERE downloaded_chapters > 0
        ORDER BY id
    """)
    stories = cur.fetchall()
    conn.close()

    print(f"Tổng truyện đã crawl: {len(stories)}\n")

    renamed  = 0
    skipped  = 0
    notfound = 0
    errors   = 0
    already  = 0

    for s in stories:
        slug  = s['slug']
        title = s['title'] or slug
        new_name = safe_folder_name(title)
        new_path = os.path.join(IMPORT_DIR, new_name)

        old_path, old_name = find_old_folder(slug, title)

        if old_path is None:
            print(f"  [?] NOT FOUND  | {slug[:50]}")
            notfound += 1
            continue

        if old_name == new_name:
            already += 1
            continue  # Đã đúng tên rồi

        if os.path.exists(new_path):
            print(f"  [!] CONFLICT   | '{old_name}' -> '{new_name}' (target đã tồn tại!)")
            skipped += 1
            continue

        print(f"  [>] RENAME     | '{old_name}'")
        print(f"               -> '{new_name}'")

        if not dry:
            try:
                os.rename(old_path, new_path)
                renamed += 1
            except Exception as e:
                print(f"  [!] LỖI: {e}")
                errors += 1
        else:
            renamed += 1

    print(f"\n{'='*60}")
    print(f"{'[DRY RUN] ' if dry else ''}Kết quả:")
    print(f"  Đã rename : {renamed}")
    print(f"  Đã đúng   : {already}")
    print(f"  Conflict  : {skipped}")
    print(f"  Không thấy: {notfound}")
    print(f"  Lỗi       : {errors}")


if __name__ == '__main__':
    main()
