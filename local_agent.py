"""
local_agent.py — Chạy trên máy local (Windows).
Poll Vercel mỗi 3s, nhận lệnh và thực thi locally.

Cách dùng:
    python local_agent.py

Config trong file agent_config.json (tự tạo lần đầu):
{
    "vercel_url": "https://your-app.vercel.app",
    "agent_secret": "changeme",
    "admin_name": "Admin Huy",
    "data_import_dir": "D:\\Webtruyen\\pdcraw\\data_import",
    "scraper_script": "D:\\Webtruyen\\pdcraw\\pd_scraper_fast-v1.py",
    "discovery_script": "D:\\Webtruyen\\pdcraw\\pd_discovery_auto.py",
    "check_update_script": "D:\\Webtruyen\\pdcraw\\check_update.py",
    "accounts_file": "D:\\Webtruyen\\pdcraw\\accounts.txt"
}
"""

import json
import os
import sys
import time
import re
import threading
import subprocess
import urllib.request
import urllib.parse
import http.client
import datetime

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'agent_config.json')
DEFAULT_CONFIG = {
    "vercel_url":           "https://your-app.vercel.app",
    "agent_secret":         "changeme",
    "admin_name":           "Admin",
    "machine_label":        "",
    "data_import_dir":      "D:\\Webtruyen\\pdcraw\\data_import",
    "scraper_script":       "D:\\Webtruyen\\pdcraw\\pd_scraper_fast-v1.py",
    "wiki_scraper_script":  "D:\\Webtruyen\\pdcraw\\wiki_scraper_agent.py",
    "discovery_script":     "D:\\Webtruyen\\pdcraw\\pd_discovery_auto.py",
    "check_update_script":  "D:\\Webtruyen\\pdcraw\\check_update.py",
    "accounts_file":        "D:\\Webtruyen\\pdcraw\\accounts.txt",
    "wiki_accounts_file":   "D:\\Webtruyen\\pdcraw\\userpass-wiki.txt",
}

def load_config():
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2, ensure_ascii=False)
        print(f"[!] Tạo file config mới: {CONFIG_PATH}")
        print("[!] Hãy chỉnh sửa file đó rồi chạy lại agent.")
        sys.exit(0)
    with open(CONFIG_PATH, encoding='utf-8') as f:
        return json.load(f)

CFG = load_config()
VERCEL_URL    = CFG['vercel_url'].rstrip('/')
AGENT_SECRET  = CFG['agent_secret']

def resolve_path(p):
    """Resolve đường dẫn tương đối theo thư mục chứa local_agent.py."""
    if not p: return p
    if not os.path.isabs(p):
        return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), p))
    return p

IMPORT_DIR         = resolve_path(CFG['data_import_dir'])
SCRAPER_PATH       = resolve_path(CFG['scraper_script'])
WIKI_SCRAPER_PATH  = resolve_path(CFG.get('wiki_scraper_script', ''))   # ← mới
DISCOVERY_PATH     = resolve_path(CFG['discovery_script'])
CHECK_UPDATE       = resolve_path(CFG['check_update_script'])
ACCOUNTS_FILE      = resolve_path(CFG['accounts_file'])
WIKI_ACCOUNTS_FILE = resolve_path(CFG.get('wiki_accounts_file', ''))    # ← mới
MACHINE_LABEL = CFG.get('machine_label', '')   # Nhãn máy này: 'A', 'B', 'C'...

HEADERS = {'X-Agent-Secret': AGENT_SECRET, 'Content-Type': 'application/json'}

# Tracking running scraper PIDs
SCRAPER_PIDS = []
PIDS_LOCK    = threading.Lock()

# Tracking cmd_ids đang xử lý để tránh chạy lại
PROCESSING_IDS = set()
PROCESSING_LOCK = threading.Lock()

# Kill cooldown: tránh chạy nhiều kill cùng lúc
KILL_LOCK    = threading.Lock()
KILL_RUNNING = False
LAST_KILL_TS = 0.0   # timestamp lần kill gần nhất
KILL_COOLDOWN = 60   # giây: bỏ qua kill nếu đã kill trong vòng N giây

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _request(method, path, body=None, timeout=30):
    """Gửi request tới Vercel API."""
    parsed = urllib.parse.urlparse(VERCEL_URL + path)
    host   = parsed.hostname
    port   = parsed.port
    use_https = parsed.scheme == 'https'

    if use_https:
        conn = http.client.HTTPSConnection(host, port or 443, timeout=timeout)
    else:
        conn = http.client.HTTPConnection(host, port or 80, timeout=timeout)

    data = json.dumps(body, ensure_ascii=False).encode() if body else None
    h = dict(HEADERS)
    if data:
        h['Content-Length'] = str(len(data))

    conn.request(method, parsed.path + (('?' + parsed.query) if parsed.query else ''), body=data, headers=h)
    resp = conn.getresponse()
    raw  = resp.read().decode('utf-8')
    conn.close()
    return json.loads(raw) if raw else {}

def poll_command():
    return _request('GET', '/api/agent?action=poll')

def report_done(cmd_id, result, status='done'):
    _request('POST', '/api/agent?action=done', {'command_id': cmd_id, 'result': result, 'status': status})

def heartbeat(running_count):
    _request('POST', '/api/agent?action=heartbeat', {
        'agent_id': CFG.get('admin_name', 'agent'),
        'running_scrapers': running_count
    })

def update_story_remote(story_id, **kwargs):
    """Cập nhật 1 story field trên Neon qua Vercel API."""
    _request('POST', '/api/agent?action=update_story', {'story_id': story_id, **kwargs})

# ── Helper: đọc file local ────────────────────────────────────────────────────

def scan_story_dir(slug):
    """Quét thư mục data_import/<slug>, trả về thông tin file."""
    story_dir = os.path.join(IMPORT_DIR, slug)
    if not os.path.exists(story_dir):
        return {'exists': False, 'total_files': 0, 'max_index': 0, 'actual_count': 0}
    files = [f for f in os.listdir(story_dir) if f.endswith('.txt')]
    file_map = {}
    for fname in files:
        m = re.search(r'_(\d+)\.txt$', fname)
        if m:
            idx = int(m.group(1))
            file_map[idx] = os.path.join(story_dir, fname)
    max_idx = max(file_map.keys()) if file_map else 0
    return {'exists': True, 'total_files': len(files), 'max_index': max_idx, 'actual_count': len(file_map), 'file_map': file_map}

# ── Command handlers ──────────────────────────────────────────────────────────

def _wait_bot_claimed_story(acc_idx, timeout=120):
    """
    Đợi bot acc_idx claim được 1 truyện VÀ save ít nhất 1 chương.
    Giống logic file cũ pd_manage_fast-v1.py dòng 1718-1734.
    Trả về True nếu bot đã vào việc, False nếu timeout/stop.
    """
    scraper_dir = os.path.dirname(os.path.abspath(SCRAPER_PATH))
    stop_file   = os.path.join(scraper_dir, 'stop.signal')
    start_wait  = time.time()
    target_story_id   = None
    initial_chapters  = -1

    print(f"  [~] Chờ Bot {acc_idx} claim truyện và lưu chương đầu...")
    while time.time() - start_wait < timeout:
        if os.path.exists(stop_file):
            return False
        try:
            resp = _request('GET', f'/api?action=get_crawling_story&acc_idx={acc_idx}')
            if not target_story_id:
                sid = resp.get('story_id')
                dc  = resp.get('downloaded_chapters', 0)
                if sid:
                    target_story_id  = sid
                    initial_chapters = dc or 0
                else:
                    time.sleep(2); continue
            else:
                sid = resp.get('story_id')
                dc  = resp.get('downloaded_chapters', 0)
                status = resp.get('crawl_status', '')
                if status != 'crawling' or (dc or 0) > initial_chapters:
                    print(f"  [+] Bot {acc_idx} đã lưu chương đầu. Kích hoạt bot tiếp theo.")
                    return True
                time.sleep(3)
        except Exception as e:
            time.sleep(2)
    print(f"  [!] Timeout chờ Bot {acc_idx} — tiếp tục mở bot kế tiếp.")
    return True  # timeout nhưng vẫn cho bot tiếp theo chạy


def handle_start_scraper(payload, cmd_id):
    global SCRAPER_PIDS
    admin         = payload.get('admin', CFG.get('admin_name'))
    account_idxs  = payload.get('accounts', [])
    threads       = int(payload.get('threads', len(account_idxs)))
    source        = payload.get('source', 'PD').upper()   # 'PD' hoặc 'WIKI'
    account_idxs  = account_idxs[:threads]

    # Chọn đúng script theo source
    if source == 'WIKI':
        script_path = WIKI_SCRAPER_PATH
        if not script_path or not os.path.exists(script_path):
            report_done(cmd_id, {'success': False,
                'message': f'wiki_scraper_script chưa cấu hình hoặc không tồn tại: {script_path}'}, 'error')
            return
    else:
        script_path = SCRAPER_PATH

    scraper_dir    = os.path.dirname(os.path.abspath(script_path))
    stop_file      = os.path.join(scraper_dir, 'stop.signal')
    lock_file      = os.path.join(scraper_dir, 'startup.lock')
    depleted_file  = os.path.join(scraper_dir, 'wiki_depleted.json')
    if os.path.exists(stop_file):
        os.remove(stop_file)
    # Giải phóng startup lock cũ (có thể còn kẹt từ lần chạy trước) trước khi launch bot
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
            print(f"[*] Đã xóa startup.lock cũ trước khi khởi động bot.")
        except Exception as e:
            print(f"[!] Không xóa được startup.lock: {e}")
    # Xóa depleted list cũ khi bắt đầu session WIKI mới (user chủ động nhấn start)
    if source == 'WIKI' and os.path.exists(depleted_file):
        try:
            os.remove(depleted_file)
            print(f"[*] Đã xóa wiki_depleted.json — session mới, reset quota tracking.")
        except Exception as e:
            print(f"[!] Không xóa được wiki_depleted.json: {e}")

    pids = []
    for i, acc_idx in enumerate(account_idxs):
        if os.path.exists(stop_file):
            break
        try:
            bot_env = os.environ.copy()
            bot_env['SERVER_URL']          = VERCEL_URL
            bot_env['AGENT_SECRET']        = AGENT_SECRET
            bot_env['DATA_IMPORT_DIR']     = str(IMPORT_DIR)
            bot_env['ACCOUNTS_FILE']       = str(ACCOUNTS_FILE)
            bot_env['WIKI_ACCOUNTS_FILE']  = str(WIKI_ACCOUNTS_FILE)  # ← truyền cho wiki
            bot_env['MACHINE_LABEL'] = MACHINE_LABEL   # truyền nhãn máy cho scraper
            if source != 'WIKI':
                # PD scraper: chia đều accounts cho từng bot — bot i lấy các index i, i+N, i+2N,...
                n_bots = len(account_idxs)
                bot_assigned = [account_idxs[j] for j in range(i, len(account_idxs), n_bots)]
                bot_env['BOT_ASSIGNED_ACCOUNTS'] = ','.join(str(x) for x in bot_assigned)
            # WIKI scraper: không set BOT_ASSIGNED_ACCOUNTS → bot load toàn bộ account pool
            # acc_idx truyền qua argument đã đủ để bot bắt đầu ở account khác nhau

            proc = subprocess.Popen(
                [sys.executable, script_path, str(acc_idx), '--admin', admin],
                creationflags=subprocess.CREATE_NEW_CONSOLE,
                env=bot_env
            )
            pids.append(proc.pid)
            print(f"[+] Started {source} scraper PID {proc.pid} (account {acc_idx})")
        except Exception as e:
            print(f"[!] Failed to start {source} scraper for account {acc_idx}: {e}")
            continue

        # Nếu không phải bot cuối: đợi bot này claim + lưu chương đầu
        if i < len(account_idxs) - 1:
            _wait_bot_claimed_story(acc_idx)

    with PIDS_LOCK:
        SCRAPER_PIDS.extend(pids)

    report_done(cmd_id, {'success': True, 'started': len(pids), 'pids': pids})

def handle_open_folder(payload, cmd_id):
    """Mở thư mục truyện trong Windows Explorer.
    Thử lần lượt: title (wiki dùng title), slug (PD dùng slug), slug đã unquote."""
    import urllib.parse, re

    slug  = payload.get('slug', '').strip()
    title = payload.get('title', '').strip()

    if not slug and not title:
        report_done(cmd_id, {'success': False, 'message': 'Thiếu slug/title'}, 'error')
        return

    # Tạo safe title (phải giống hệt safe_folder_name trong wiki_scraper_agent)
    def safe_name(t):
        import unicodedata as _ud
        n = _ud.normalize('NFD', t)
        n = ''.join(c for c in n if _ud.category(c) != 'Mn')
        n = re.sub(r'[\\/*?:"<>|]', '', n)
        n = re.sub(r'[^\w\s-]', '', n).strip()
        n = re.sub(r'[\s_]+', '-', n)
        n = re.sub(r'-+', '-', n).strip('-')
        return n[:80].lower() if n else 'unknown'

    # Thứ tự thử: title → slug → slug unquoted
    candidates = []
    if title:
        candidates.append(os.path.join(IMPORT_DIR, safe_name(title)))
    if slug:
        candidates.append(os.path.join(IMPORT_DIR, slug))
        candidates.append(os.path.join(IMPORT_DIR, urllib.parse.unquote(slug)))

    for folder in candidates:
        if os.path.exists(folder):
            try:
                subprocess.Popen(['explorer', folder])
                report_done(cmd_id, {'success': True, 'path': folder})
            except Exception as e:
                report_done(cmd_id, {'success': False, 'message': str(e)}, 'error')
            return

    report_done(cmd_id, {'success': False,
                         'message': f'Không tìm thấy thư mục. Đã thử: {candidates}'}, 'error')


def handle_generate_meta(payload, cmd_id):
    """Thu thập thông tin tên chương + mô tả cho danh sách truyện (để đổi tên về sau).

    Quy trình mỗi truyện:
    1. Tìm thư mục truyện (title-based → slug → unquote slug)
    2. Quét file .txt trong thư mục, phát hiện pattern tên chương:
       - Chuong-N / Chapter-N → KHÔNG có tên chương
       - Tên thực → Hướng 1: lấy từ tên file
    3. Nếu không có tên chương:
       - PD: đọc menu_map_v1.json đã có sẵn trên disk
       - WIKI: dùng Selenium scrape URL trong DB
       - Không có gì → meta_status='no_chapter_names'
    4. Lưu story_meta.json vào thư mục truyện
    5. Cập nhật meta_status trong DB
    """
    stories = payload.get('stories', [])  # list of {id, slug, title, source, url}

    def safe_name(t):
        import unicodedata as _ud
        n = _ud.normalize('NFD', t)
        n = ''.join(c for c in n if _ud.category(c) != 'Mn')
        n = re.sub(r'[\\/*?:"<>|]', '', n)
        n = re.sub(r'[^\w\s-]', '', n).strip()
        n = re.sub(r'[\s_]+', '-', n)
        n = re.sub(r'-+', '-', n).strip('-')
        return n[:80].lower() if n else 'unknown'

    NO_NAME_RE = re.compile(
        r'^(chuong|chapter|chap|c)[-_]?\d+_\d{4}\.txt$', re.IGNORECASE
    )

    def find_story_dir(slug, title):
        candidates = []
        if title:
            candidates.append(os.path.join(IMPORT_DIR, safe_name(title)))
        if slug:
            candidates.append(os.path.join(IMPORT_DIR, slug))
            candidates.append(os.path.join(IMPORT_DIR, urllib.parse.unquote(slug)))
        for p in candidates:
            if os.path.exists(p):
                return p
        return None

    def get_chapter_files(story_dir):
        """Trả về list tên file chương (đã sort theo index)."""
        files = []
        for f in os.listdir(story_dir):
            if f.endswith('.txt') and re.search(r'_\d{4}\.txt$', f):
                files.append(f)
        files.sort(key=lambda f: int(re.search(r'_(\d{4})\.txt$', f).group(1)))
        return files

    def extract_title_from_filename(fname):
        """Bỏ phần _NNNN.txt ở cuối."""
        return re.sub(r'_\d{4}\.txt$', '', fname).strip()

    # ── Bước 1: Phân loại các truyện ────────────────────────────────────────
    results       = []
    wiki_need_scrape = []   # truyện WIKI cần mở browser

    for s in stories:
        sid    = s.get('id')
        slug   = s.get('slug', '').strip()
        title  = s.get('title', '').strip()
        source = s.get('source', 'PD').upper()
        url    = s.get('url', '')

        story_dir = find_story_dir(slug, title)
        if not story_dir:
            update_story_remote(sid, meta_status='no_chapter_names')
            results.append({'id': sid, 'status': 'error', 'message': 'Không tìm thấy thư mục'})
            continue

        chapter_files = get_chapter_files(story_dir)
        if not chapter_files:
            update_story_remote(sid, meta_status='no_chapter_names')
            results.append({'id': sid, 'status': 'no_names', 'message': 'Thư mục không có file chương'})
            continue

        # Kiểm tra có tên chương thực sự không
        has_real_names = any(not NO_NAME_RE.match(f) for f in chapter_files)

        if has_real_names:
            # Hướng 1: lấy tên từ tên file
            chapter_titles = [extract_title_from_filename(f) for f in chapter_files]
            meta = {
                'story_id':       sid,
                'original_title': title,
                'source':         source,
                'url':            url,
                'method':         'from_files',
                'description':    '',
                'chapter_titles': chapter_titles,
                'total_chapters': len(chapter_titles),
                'generated_at':   datetime.datetime.now().isoformat(),
            }
            _save_meta_json(story_dir, meta)
            update_story_remote(sid, meta_status='ready')
            results.append({'id': sid, 'status': 'ready', 'method': 'from_files',
                            'chapters': len(chapter_titles)})
            print(f"  [META] #{sid} {title[:30]} → from_files ({len(chapter_titles)} chương)")

        else:
            # Không có tên chương thực — thử đọc menu_map (PD) hoặc scrape (WIKI)
            menu_map_path = os.path.join(story_dir, 'menu_map_v1.json')
            if os.path.exists(menu_map_path):
                # PD / bất kỳ source: menu_map sẵn trên disk
                try:
                    with open(menu_map_path, encoding='utf-8') as f:
                        menu_map = json.load(f)
                    chapter_titles = [menu_map[str(i)] for i in sorted(int(k) for k in menu_map.keys())]
                    meta = {
                        'story_id':       sid,
                        'original_title': title,
                        'source':         source,
                        'url':            url,
                        'method':         'from_menu_map',
                        'description':    '',
                        'chapter_titles': chapter_titles,
                        'total_chapters': len(chapter_titles),
                        'generated_at':   datetime.datetime.now().isoformat(),
                    }
                    _save_meta_json(story_dir, meta)
                    update_story_remote(sid, meta_status='ready')
                    results.append({'id': sid, 'status': 'ready', 'method': 'from_menu_map',
                                    'chapters': len(chapter_titles)})
                    print(f"  [META] #{sid} {title[:30]} → from_menu_map ({len(chapter_titles)} chương)")
                except Exception as e:
                    update_story_remote(sid, meta_status='no_chapter_names')
                    results.append({'id': sid, 'status': 'error', 'message': f'Đọc menu_map lỗi: {e}'})

            elif source == 'WIKI' and url:
                # Cần scrape bằng Selenium — đưa vào danh sách xử lý sau
                wiki_need_scrape.append({'s': s, 'story_dir': story_dir})
                # Kết quả sẽ được thêm vào results sau

            else:
                # PD không có menu_map và không phải WIKI → không có cách nào lấy tên
                update_story_remote(sid, meta_status='no_chapter_names')
                results.append({'id': sid, 'status': 'no_names',
                                'message': 'Không có tên chương, không có menu_map'})
                print(f"  [META] #{sid} {title[:30]} → no_chapter_names")

    # ── Bước 2: Xử lý WIKI cần scrape (mở browser 1 lần) ───────────────────
    if wiki_need_scrape:
        wiki_results = _scrape_wiki_meta_batch(wiki_need_scrape)
        results.extend(wiki_results)

    done_count    = sum(1 for r in results if r.get('status') == 'ready')
    no_name_count = sum(1 for r in results if r.get('status') == 'no_names')
    err_count     = sum(1 for r in results if r.get('status') == 'error')
    report_done(cmd_id, {
        'success': True,
        'total':   len(stories),
        'ready':   done_count,
        'no_names': no_name_count,
        'errors':  err_count,
        'results': results,
    })


def _save_meta_json(story_dir, meta):
    """Lưu story_meta.json vào thư mục truyện."""
    path = os.path.join(story_dir, 'story_meta.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return path


def _scrape_wiki_meta_batch(items):
    """Mở 1 Chrome, scrape description + chapter list từ wikicv.net cho nhiều truyện."""
    results = []
    driver = None
    temp_dir = None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
        import shutil, tempfile

        options = Options()
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-notifications')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument('--log-level=3')
        temp_dir = tempfile.mkdtemp()
        options.add_argument(f'--user-data-dir={temp_dir}')

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        print("  [META] Chrome khởi động cho WIKI scraping")

        # Đọc danh sách tài khoản Wiki
        wiki_accounts = []
        if WIKI_ACCOUNTS_FILE and os.path.exists(WIKI_ACCOUNTS_FILE):
            try:
                with open(WIKI_ACCOUNTS_FILE, encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '|' in line:
                            parts = line.split('|', 1)
                            wiki_accounts.append((parts[0].strip(), parts[1].strip()))
            except Exception as e:
                print(f"  [META] Không đọc được wiki accounts: {e}")
        acc_idx = 0
        logged_in = False

        for item in items:
            s         = item['s']
            story_dir = item['story_dir']
            sid       = s.get('id')
            title     = s.get('title', '').strip()
            url       = s.get('url', '')
            source    = s.get('source', 'WIKI').upper()

            try:
                detail = _wiki_get_story_detail(driver, url)
                results.append({
                    'id': sid, 'status': 'ready', 'method': 'scraped_wiki',
                    'chapters': len(detail.get('chapter_titles', []))
                })
            except _WikiAccessLimit:
                # Thử đăng nhập
                login_ok = False
                while acc_idx < len(wiki_accounts):
                    u, p = wiki_accounts[acc_idx]
                    acc_idx += 1
                    try:
                        _wiki_login(driver, u, p)
                        logged_in = True
                        print(f"  [META] Đăng nhập wiki: {u}")
                        login_ok = True
                        break
                    except Exception as le:
                        print(f"  [META] Login thất bại {u}: {le}")

                if login_ok:
                    try:
                        detail = _wiki_get_story_detail(driver, url)
                    except Exception as e2:
                        update_story_remote(sid, meta_status='no_chapter_names')
                        results.append({'id': sid, 'status': 'no_names',
                                        'message': f'Scrape thất bại sau login: {e2}'})
                        continue
                else:
                    update_story_remote(sid, meta_status='no_chapter_names')
                    results.append({'id': sid, 'status': 'no_names',
                                    'message': 'Hết lượt, không còn account để đăng nhập'})
                    continue

            except Exception as e:
                update_story_remote(sid, meta_status='no_chapter_names')
                results.append({'id': sid, 'status': 'error', 'message': str(e)})
                continue

            meta = {
                'story_id':       sid,
                'original_title': title,
                'source':         source,
                'url':            url,
                'method':         'scraped_wiki',
                'description':    detail.get('description', ''),
                'chapter_titles': detail.get('chapter_titles', []),
                'total_chapters': len(detail.get('chapter_titles', [])),
                'generated_at':   datetime.datetime.now().isoformat(),
            }
            _save_meta_json(story_dir, meta)
            update_story_remote(sid, meta_status='ready')
            print(f"  [META] #{sid} {title[:30]} → scraped_wiki ({len(detail.get('chapter_titles',[]))} chương)")

    except ImportError:
        print("  [META] Selenium không khả dụng — WIKI stories đánh dấu no_chapter_names")
        for item in items:
            sid = item['s'].get('id')
            update_story_remote(sid, meta_status='no_chapter_names')
            results.append({'id': sid, 'status': 'no_names', 'message': 'Selenium chưa cài đặt'})
    except Exception as e:
        print(f"  [META] Lỗi WIKI scraping: {e}")
        for item in items:
            if not any(r['id'] == item['s'].get('id') for r in results):
                sid = item['s'].get('id')
                update_story_remote(sid, meta_status='no_chapter_names')
                results.append({'id': sid, 'status': 'error', 'message': str(e)})
    finally:
        if driver:
            try: driver.quit()
            except: pass
        if temp_dir:
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except: pass

    return results


class _WikiAccessLimit(Exception): pass


def _wiki_login(driver, username, password):
    """Đăng nhập wikicv.net."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    driver.get('https://wikicv.net')
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'body')))
    import time as _time; _time.sleep(2)

    try:
        logout_probe = driver.find_elements(By.CSS_SELECTOR, 'a[data-action="logout"]')
        if logout_probe:
            return  # Đã đăng nhập
    except: pass

    login_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[data-action="login"]'))
    )
    login_btn.click()
    _time.sleep(3)

    windows = driver.window_handles
    is_new_window = len(windows) > 1
    main_window = driver.current_window_handle
    if is_new_window:
        driver.switch_to.window(windows[-1])
    else:
        iframes = driver.find_elements(By.TAG_NAME, 'iframe')
        if iframes:
            driver.switch_to.frame(iframes[0])

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, 'username')))
    driver.find_element(By.NAME, 'username').send_keys(username)
    driver.find_element(By.NAME, 'password').send_keys(password)
    try: driver.find_element(By.NAME, 'remember').click()
    except: pass
    driver.find_element(By.ID, 'login').click()

    if is_new_window:
        driver.switch_to.window(main_window)
    else:
        try: driver.switch_to.default_content()
        except: pass

    WebDriverWait(driver, 15).until(EC.url_contains('wikicv.net'))
    _time.sleep(2)


def _wiki_get_story_detail(driver, url):
    """Scrape tên chương + mô tả từ trang truyện wikicv.net.
    Raise _WikiAccessLimit nếu hết lượt."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time as _time

    ACCESS_MSG = "Đã hết lượt truy cập"
    driver.get(url)
    _time.sleep(4)

    if ACCESS_MSG in driver.page_source:
        raise _WikiAccessLimit(f"Hết lượt tại {url}")

    # Mô tả truyện — thử nhiều selector
    description = ''
    for sel in ['.book-intro', '.book-summary', '#bookSummary',
                '.story-detail .content', '.book-detail .content p']:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            description = el.text.strip()
            if description:
                break
        except: pass

    # Lấy danh sách chương — xử lý phân trang
    chapter_titles = []

    def _get_chapters_on_page():
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'ul li.chapter-name a'))
            )
            return [el.text.strip() for el in driver.find_elements(By.CSS_SELECTOR, 'ul li.chapter-name a')]
        except:
            return []

    # Kéo xuống để pagination hiện
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    _time.sleep(2)

    first_page = _get_chapters_on_page()
    chapter_titles.extend(first_page)

    # Kiểm tra phân trang
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    page_buttons = soup.select('ul.pagination li a')
    page_numbers = [int(b.text.strip()) for b in page_buttons if b.text.strip().isdigit()]
    max_page = max(page_numbers) if page_numbers else 1

    for p in range(2, max_page + 1):
        # Click số trang
        try:
            links = driver.find_elements(By.CSS_SELECTOR, 'ul.pagination li a')
            for a in links:
                if a.text.strip() == str(p):
                    driver.execute_script("arguments[0].click();", a)
                    _time.sleep(2)
                    break
            chapter_titles.extend(_get_chapters_on_page())
        except Exception as e:
            print(f"  [META] Không load được trang {p}: {e}")
            break

    return {'description': description, 'chapter_titles': chapter_titles}


def handle_kill_scrapers(payload, cmd_id):
    global SCRAPER_PIDS, KILL_RUNNING, LAST_KILL_TS
    killed = []

    # Guard: nếu đang kill hoặc vừa kill xong < COOLDOWN giây, bỏ qua
    with KILL_LOCK:
        now = time.time()
        if KILL_RUNNING:
            print(f"[!] Kill skipped (another kill in progress) cmd_id={cmd_id}")
            report_done(cmd_id, {'success': True, 'killed': 0, 'note': 'skipped: kill already running'}, 'done')
            return
        if now - LAST_KILL_TS < KILL_COOLDOWN:
            elapsed = int(now - LAST_KILL_TS)
            print(f"[!] Kill skipped (cooldown {elapsed}s/{KILL_COOLDOWN}s) cmd_id={cmd_id}")
            report_done(cmd_id, {'success': True, 'killed': 0, 'note': f'skipped: cooldown {elapsed}s'}, 'done')
            return
        KILL_RUNNING = True

    try:
        scraper_dir = os.path.dirname(os.path.abspath(SCRAPER_PATH))
        stop_file = os.path.join(scraper_dir, 'stop.signal')
        with open(stop_file, 'w') as f:
            f.write('STOP')
        print(f"[!] Wrote stop.signal to {stop_file}")
    except Exception as e:
        print(f"[!] Could not write stop.signal: {e}")

        # 2. Kill các PID đã lưu (start qua Web)
        with PIDS_LOCK:
            all_pids = list(set(SCRAPER_PIDS))
        for pid in all_pids:
            try:
                subprocess.run(f"taskkill /PID {pid} /F /T", shell=True, capture_output=True)
                killed.append(pid)
            except: pass

    # 3. Kill tất cả python process đang chạy các script cào
    #    Thu thập tên tất cả script cần kill
    scripts_to_kill = set()
    for spath in [SCRAPER_PATH, WIKI_SCRAPER_PATH, DISCOVERY_PATH, CHECK_UPDATE]:
        if spath:
            scripts_to_kill.add(os.path.basename(spath))
    print(f"[!] Scripts to kill: {scripts_to_kill}")

    # Thử WMIC trước (Win10), fallback sang tasklist /V (Win11)
    wmic_ok = False
    try:
        r_w = subprocess.run(
            'wmic process where "name=\'python.exe\'" get ProcessId,CommandLine /value',
            shell=True, capture_output=True, text=True, timeout=6
        )
        if 'ProcessId=' in r_w.stdout:
            wmic_ok = True
            block = {}
            for line in r_w.stdout.splitlines():
                line = line.strip()
                if line.startswith('CommandLine='):
                    block['cmd'] = line[len('CommandLine='):]
                elif line.startswith('ProcessId='):
                    v = line[len('ProcessId='):].strip()
                    if v.isdigit():
                        block['pid'] = int(v)
                elif line == '' and block.get('pid'):
                    pid_val = block['pid']
                    cmd_val = block.get('cmd', '')
                    if pid_val != os.getpid():
                        for sname in scripts_to_kill:
                            if sname in cmd_val:
                                subprocess.run(f"taskkill /PID {pid_val} /F /T", shell=True, capture_output=True)
                                killed.append(pid_val)
                                print(f"  [kill-wmic] PID {pid_val} ({sname})")
                                break
                    block = {}
    except Exception as e:
        print(f"[!] WMIC unavailable: {e}")

    # Fallback: dùng tasklist /V để lấy window title (chứa tên script)
    if not wmic_ok:
        try:
            r_t = subprocess.run(
                'tasklist /FO CSV /V /FI "IMAGENAME eq python.exe"',
                shell=True, capture_output=True, text=True
            )
            for line in r_t.stdout.splitlines():
                for sname in scripts_to_kill:
                    if sname in line:
                        parts = line.strip().strip('"').split('","')
                        if len(parts) >= 2:
                            try:
                                pid_val = int(parts[1])
                                if pid_val != os.getpid():
                                    subprocess.run(f"taskkill /PID {pid_val} /F /T", shell=True, capture_output=True)
                                    killed.append(pid_val)
                                    print(f"  [kill-tasklist] PID {pid_val} ({sname})")
                            except: pass
        except Exception as e:
            print(f"[!] tasklist kill error: {e}")

    # 4. Chrome v\u00e0 ChromeDriver s\u1ebd t\u1ef1 \u0111\u1ed9ng ch\u1ebft do l\u1ec7nh Taskkill /PID ... /T \u1edf tr\u00ean \u0111\u00e3 ti\u00eau di\u1ec7t "c\u1ea3 c\u00e2y gia ph\u1ea3" c\u1ee7a ti\u1ebfn tr\u00ecnh \u0111\u00f3.
    # Tuy\u1ec7t \u0111\u1ed1i kh\u00f4ng qu\u00e9t global chrome.exe t\u1edbi t\u1ea5t c\u1ea3 user kh\u00e1c tr\u00ean h\u1ec7 \u0111i\u1ec1u h\u00e0nh.

    with PIDS_LOCK:
        SCRAPER_PIDS = []

    print(f"[!] Kill done. PIDs killed: {killed}")

    # Reset tr\u1ea1ng th\u00e1i c\u1ee7a kill
    with KILL_LOCK:
        LAST_KILL_TS = time.time()
        KILL_RUNNING = False

    report_done(cmd_id, {'success': True, 'killed': len(killed), 'pids': killed})


def handle_submit_discovery(payload, cmd_id):
    url    = payload.get('url', '')
    source = payload.get('source', 'PD')
    print(f"  [{_ts()}] DBG [submit_discovery] id={cmd_id}  url={url!r}  source={source}")
    print(f"  [{_ts()}] DBG [submit_discovery] DISCOVERY_PATH={DISCOVERY_PATH}")
    print(f"  [{_ts()}] DBG [submit_discovery] File tồn tại: {os.path.exists(DISCOVERY_PATH)}")
    try:
        bot_env = os.environ.copy()
        bot_env['SERVER_URL']      = VERCEL_URL
        bot_env['AGENT_SECRET']    = AGENT_SECRET
        bot_env['DATA_IMPORT_DIR'] = str(IMPORT_DIR)
        bot_env['ACCOUNTS_FILE']   = str(ACCOUNTS_FILE)

        # Dùng PIPE thay vì CREATE_NEW_CONSOLE để capture stdout/stderr
        # → thấy được lỗi crash của discovery script ngay trong log này
        proc = subprocess.Popen(
            [sys.executable, DISCOVERY_PATH, '--url', url, '--source', source],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=bot_env,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        print(f"  [{_ts()}] DBG [submit_discovery] Spawn OK → PID={proc.pid}")

        # BUG 2 FIX: poll đã set status=running atomic, không gọi thêm
        threading.Thread(target=_wait_discovery, args=(cmd_id, proc), daemon=True).start()
        print(f"  [{_ts()}] DBG [submit_discovery] Thread _wait_discovery đã spawn")
    except Exception as e:
        print(f"  [{_ts()}] DBG [submit_discovery] LỖI spawn: {e}")
        report_done(cmd_id, {'success': False, 'message': str(e)}, 'error')

def _wait_discovery(cmd_id, proc):
    """Chờ discovery_conflicts.json rồi báo kết quả."""
    result_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'discovery_conflicts.json')
    print(f"  [{_ts()}] DBG [wait_discovery] id={cmd_id}  chờ file: {result_file}")

    # Đọc stdout/stderr của process trong thread riêng để không block
    output_lines = []
    def _read_output():
        try:
            for line in proc.stdout:
                line = line.rstrip()
                output_lines.append(line)
                print(f"  [DISC] {line}")
        except Exception:
            pass
    threading.Thread(target=_read_output, daemon=True).start()

    # Xóa file cũ nếu còn sót
    if os.path.exists(result_file):
        os.remove(result_file)
        print(f"  [{_ts()}] DBG [wait_discovery] Đã xóa file cũ")

    for i in range(300):  # tối đa 10 phút
        time.sleep(2)
        rc = proc.poll()
        if rc is not None and rc != 0 and i < 3:
            # Crash sớm — in toàn bộ output để debug
            time.sleep(0.5)  # chờ _read_output drain nốt
            print(f"  [{_ts()}] DBG [wait_discovery] ❌ Process crash rc={rc}")
            print(f"  [{_ts()}] DBG [wait_discovery] OUTPUT ({len(output_lines)} dòng):")
            for ln in output_lines[-30:]:
                print(f"    | {ln}")
            report_done(cmd_id, {'success': False, 'message': f'Discovery script crash rc={rc}', 'output': output_lines[-20:]}, 'error')
            return
        if os.path.exists(result_file):
            print(f"  [{_ts()}] DBG [wait_discovery] File xuất hiện sau {i*2}s → đọc...")
            try:
                with open(result_file, encoding='utf-8') as f:
                    txt = f.read().strip()
                if txt:
                    result = json.loads(txt)
                    print(f"  [+] Discovery xong: new={result.get('new',0)} conflicts={len(result.get('conflicts',[]))}")
                    report_done(cmd_id, result)
                    return
                else:
                    print(f"  [{_ts()}] DBG [wait_discovery] File rỗng → chờ tiếp")
            except Exception as e:
                print(f"  [{_ts()}] DBG [wait_discovery] Lỗi đọc file: {e}")
        else:
            if i % 15 == 0:
                print(f"  [{_ts()}] DBG [wait_discovery] Vẫn chờ... {i*2}s  proc_alive={proc.poll() is None}")

    print(f"  [{_ts()}] DBG [wait_discovery] TIMEOUT → báo rỗng")
    report_done(cmd_id, {'new': 0, 'conflicts': []})

def handle_scan_updates(payload, cmd_id):
    try:
        bot_env = os.environ.copy()
        bot_env['SERVER_URL']      = VERCEL_URL
        bot_env['AGENT_SECRET']    = AGENT_SECRET
        bot_env['DATA_IMPORT_DIR'] = str(IMPORT_DIR)
        bot_env['ACCOUNTS_FILE']   = str(ACCOUNTS_FILE)  # ← FIX

        proc = subprocess.Popen(
            [sys.executable, CHECK_UPDATE],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
            env=bot_env
        )
        _request('POST', '/api/agent?action=done', {
            'command_id': cmd_id,
            'result': {'success': True, 'pid': proc.pid},
            'status': 'running'
        })
        threading.Thread(target=_wait_updates, args=(cmd_id, proc), daemon=True).start()
    except Exception as e:
        report_done(cmd_id, {'success': False, 'message': str(e)}, 'error')

def _wait_updates(cmd_id, proc):
    result_file = 'update_results.json'
    for _ in range(600):  # tối đa 10 phút
        time.sleep(2)
        if os.path.exists(result_file):
            try:
                with open(result_file, encoding='utf-8') as f:
                    content = f.read().strip()
                if content.startswith('[') and content.endswith(']'):
                    report_done(cmd_id, json.loads(content))
                    return
            except: pass
    report_done(cmd_id, [], 'done')

def handle_sync_selected(payload, cmd_id):
    ids = payload.get('ids', [])
    results = []
    for sid in ids:
        # Lấy slug từ Vercel
        try:
            story_data = _request('GET', f'/api/agent?action=poll')  # placeholder
            # Thực tế: agent cần query trực tiếp hoặc payload có slug
            slug = payload.get('slugs', {}).get(str(sid), '')
            if not slug:
                results.append({'id': sid, 'ok': False, 'msg': 'No slug'})
                continue
            info = scan_story_dir(slug)
            update_story_remote(sid,
                downloaded_chapters=info['max_index'],
                actual_chapters=info['actual_count'])
            results.append({'id': sid, 'ok': True, 'max': info['max_index']})
        except Exception as e:
            results.append({'id': sid, 'ok': False, 'msg': str(e)})
    report_done(cmd_id, {'success': True, 'results': results})

def handle_check_upload_content(payload, cmd_id):
    """Scan file local, trả kết quả về Vercel."""
    ids       = payload.get('ids', [])
    min_chars = int(payload.get('min_chars', 500))
    # Payload cần có map id→slug vì agent không có DB trực tiếp
    slug_map  = payload.get('slug_map', {})
    results   = []

    for sid in ids:
        slug = slug_map.get(str(sid), '')
        if not slug:
            results.append({'id': sid, 'error': 'slug not found'}); continue

        info = scan_story_dir(slug)
        if not info['exists']:
            results.append({'id': sid, 'title': slug, 'slug': slug,
                            'total_files': 0, 'delta': 0,
                            'missing_indexes': [], 'error_chapters': [],
                            'error': 'Thư mục không tồn tại'}); continue

        file_map       = info.get('file_map', {})
        all_indexes    = sorted(file_map.keys())
        uploaded_idx   = int(payload.get('uploaded_map', {}).get(str(sid), 0))
        delta_indexes  = [i for i in all_indexes if i > uploaded_idx]

        missing = []
        if delta_indexes:
            expected = set(range(min(delta_indexes), max(delta_indexes)+1))
            missing  = sorted(expected - set(delta_indexes))

        error_chapters = []
        for idx in delta_indexes:
            fpath = file_map[idx]
            fname = os.path.basename(fpath)
            title = re.sub(r'_\d+\.txt$', '', fname).strip()
            try:
                fsize = os.path.getsize(fpath)
                if fsize < min_chars * 2:
                    error_chapters.append({'index': idx, 'title': title, 'chars': fsize // 2})
            except Exception as e:
                error_chapters.append({'index': idx, 'title': title, 'chars': -1, 'error': str(e)})

        results.append({
            'id': sid, 'slug': slug,
            'total_files': len(all_indexes), 'delta': len(delta_indexes),
            'uploaded_so_far': uploaded_idx, 'max_index': info['max_index'],
            'missing_indexes': missing, 'error_chapters': error_chapters
        })

    report_done(cmd_id, {'success': True, 'results': results})

def handle_do_upload(payload, cmd_id):
    """Đọc file .txt local → upload lên web truyện."""
    import random

    sid        = payload.get('story_id')
    slug       = payload.get('slug', '')
    web_url    = payload.get('web_url', '').rstrip('/')
    secret     = payload.get('secret', '')
    skip_errors= payload.get('skip_errors', False)
    min_chars  = int(payload.get('min_chars', 500))
    batch_size = max(10, min(200, int(payload.get('batch_size', 50))))
    uploaded_idx = int(payload.get('uploaded_chapters', 0))
    story_row  = payload.get('story_row', {})

    if not slug:
        report_done(cmd_id, {'success': False, 'message': 'Missing slug'}, 'error'); return

    info = scan_story_dir(slug)
    if not info['exists']:
        report_done(cmd_id, {'success': False, 'message': f'Thư mục {slug} không tồn tại'}, 'error'); return

    file_map   = info.get('file_map', {})
    delta_keys = sorted([k for k in file_map if k > uploaded_idx])
    if not delta_keys:
        report_done(cmd_id, {'success': True, 'inserted': 0, 'last_index': uploaded_idx,
                             'message': 'Không có chương mới'}); return

    # Build chapters payload
    chapters_payload = []
    skipped = 0

    # Placeholder cho chương bị thiếu
    full_range = set(range(delta_keys[0], delta_keys[-1]+1))
    for missing_idx in sorted(full_range - set(delta_keys)):
        chapters_payload.append({'index': missing_idx, 'title': f'Chương {missing_idx}',
                                 'content': 'Bị mất chương, cập nhật sau, mong các đạo hữu thông cảm.'})

    for idx in delta_keys:
        fpath = file_map[idx]
        fname = os.path.basename(fpath)
        title = re.sub(r'_\d+\.txt$', '', fname).strip()
        try:
            with open(fpath, encoding='utf-8') as f:
                txt = f.read().strip()
            txt = _clean_content(txt)
        except:
            skipped += 1; continue
        if len(txt) < min_chars and not skip_errors:
            skipped += 1; continue
        chapters_payload.append({'index': idx, 'title': title, 'content': txt})

    chapters_payload.sort(key=lambda x: x['index'])

    story_payload = {
        'title':        story_row.get('title', slug),
        'slug':         slug,
        'author':       story_row.get('author', ''),
        'category':     story_row.get('category', ''),
        'description':  story_row.get('description', ''),
        'cover_url':    story_row.get('cover_url', ''),
        'book_status':  story_row.get('book_status', 'Ongoing'),
        'view_count':   random.randint(4000, 6000),
        'like_count':   random.randint(70, 200),
        'follow_count': random.randint(200, 500),
    }

    total_inserted = 0
    last_ok_index  = uploaded_idx
    batch_logs     = []
    total_batches  = (len(chapters_payload) + batch_size - 1) // batch_size

    parsed_url = urllib.parse.urlparse(web_url)
    host = parsed_url.hostname; port = parsed_url.port
    use_https = parsed_url.scheme == 'https'
    path = parsed_url.path.rstrip('/') + '/api/admin/stories'

    for i in range(0, len(chapters_payload), batch_size):
        batch    = chapters_payload[i:i+batch_size]
        batch_no = i // batch_size + 1
        body_bytes = json.dumps({'story': story_payload, 'chapters': batch}, ensure_ascii=False).encode()
        try:
            if use_https:
                conn_h = http.client.HTTPSConnection(host, port or 443, timeout=60)
            else:
                conn_h = http.client.HTTPConnection(host, port or 80, timeout=60)
            conn_h.request('POST', path, body=body_bytes, headers={
                'Content-Type': 'application/json; charset=utf-8',
                'X-Upload-Secret': urllib.parse.quote(secret, safe=''),
                'Content-Length': str(len(body_bytes))
            })
            resp = conn_h.getresponse()
            resp_body = resp.read().decode()
            conn_h.close()
            if resp.status not in (200, 201):
                raise Exception(f"HTTP {resp.status}: {resp_body[:200]}")
            result = json.loads(resp_body)
            inserted = result.get('inserted', len(batch))
            total_inserted += inserted
            last_ok_index = max(c['index'] for c in batch)
            # Cập nhật DB ngay sau mỗi batch
            update_story_remote(sid, uploaded_chapters=last_ok_index)
            batch_logs.append({'ok': True, 'msg': f'✅ Batch {batch_no}/{total_batches} — +{inserted} chương'})
        except Exception as e:
            batch_logs.append({'ok': False, 'msg': f'❌ Batch {batch_no} lỗi: {e}'})
            break

    report_done(cmd_id, {
        'success': True, 'inserted': total_inserted,
        'skipped': skipped, 'last_index': last_ok_index,
        'batch_logs': batch_logs
    })

def _clean_content(text):
    skip = [
        r'^Chương\s+\d+[:\s]', r'^Index:\s*\d+\s*$',
        r'^[–—-]+\s*$', r'^\[.*\]\s*$',
        r'^_{3,}\s*$', r'^-{3,}\s*$', r'^\*{3,}\s*$',
        r'^(Editor|Translator|TL|Dich|Bien tap)\s*[::.]',
        r'^Nguon\s*[::]', r'^https?://',
        r'tangthuvien\.vn|truyenfull|metruyencv|wattpad|truyenphuongdong',
    ]
    import re as _re
    compiled = [_re.compile(p, _re.IGNORECASE) for p in skip]
    out = [l.strip() for l in text.splitlines() if l.strip() and not any(p.search(l.strip()) for p in compiled)]
    return '\n'.join(out).strip()

def handle_import_local_data(payload, cmd_id):
    """Ghi nhận data truyện đã có sẵn trên local vào DB.
    Quét thư mục data_import/<folder_name>, đếm số chương lớn nhất từ tên file,
    update downloaded_chapters và crawl_status='paused' lên DB."""
    story_id    = payload.get('story_id')
    folder_name = payload.get('folder_name', '').strip()

    if not story_id or not folder_name:
        report_done(cmd_id, {'success': False, 'message': 'Thiếu story_id hoặc folder_name'}, 'error')
        return

    story_dir = os.path.join(IMPORT_DIR, folder_name)
    if not os.path.exists(story_dir):
        report_done(cmd_id, {'success': False, 'message': f'Không tìm thấy thư mục: {story_dir}'}, 'error')
        return

    files = [f for f in os.listdir(story_dir) if f.endswith('.txt')]
    max_idx = 0
    for fname in files:
        m = re.search(r'(\d+)', fname)
        if m:
            max_idx = max(max_idx, int(m.group(1)))

    if max_idx == 0:
        report_done(cmd_id, {'success': False, 'message': f'Không tìm thấy file chương nào trong {folder_name}'}, 'error')
        return

    update_story_remote(story_id, downloaded_chapters=max_idx, crawl_status='paused')
    print(f"  [✓] Import local: story_id={story_id}, folder={folder_name}, max_idx={max_idx}, files={len(files)}")
    report_done(cmd_id, {'success': True, 'folder': folder_name, 'downloaded_chapters': max_idx, 'total_files': len(files)})

# ── Main loop ─────────────────────────────────────────────────────────────────

HANDLERS = {
    'start_scraper':       handle_start_scraper,
    'kill_scrapers':       handle_kill_scrapers,
    'submit_discovery':    handle_submit_discovery,
    'scan_updates':        handle_scan_updates,
    'sync_selected':       handle_sync_selected,
    'check_upload_content':handle_check_upload_content,
    'do_upload':           handle_do_upload,
    'open_folder':         handle_open_folder,
    'generate_meta':       handle_generate_meta,
    'import_local_data':   handle_import_local_data,
}

def _ts():
    return datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]

def main():
    print(f"[*] PDCraw Local Agent khởi động")
    print(f"[*] Kết nối tới: {VERCEL_URL}")
    print(f"[*] Nhấn Ctrl+C để thoát\n")

    hb_counter = 5 # Force fire immediately on first loop
    while True:
        try:
            # Heartbeat m\u1ed7i 15s (5 x 3s)
            hb_counter += 1
            if hb_counter >= 5:
                with PIDS_LOCK:
                    running = len(SCRAPER_PIDS)
                heartbeat(running)
                hb_counter = 0

            # Poll lệnh
            resp = poll_command()
            if resp.get('has_command'):
                cmd_id = resp['id']
                action = resp['action']
                payload= resp.get('payload', {})

                # Bỏ qua nếu lệnh này đang được xử lý
                with PROCESSING_LOCK:
                    if cmd_id in PROCESSING_IDS:
                        pass  # skip
                    else:
                        PROCESSING_IDS.add(cmd_id)
                        print(f"[→] Nhận lệnh: {action} (id={cmd_id})")
                        handler_fn = HANDLERS.get(action)
                        if handler_fn:
                            def _run(fn, p, cid):
                                try:
                                    fn(p, cid)
                                finally:
                                    with PROCESSING_LOCK:
                                        PROCESSING_IDS.discard(cid)
                            threading.Thread(
                                target=_run,
                                args=(handler_fn, payload, cmd_id),
                                daemon=True
                            ).start()
                        else:
                            PROCESSING_IDS.discard(cmd_id)
                            report_done(cmd_id, {'success': False, 'message': f'Unknown action: {action}'}, 'error')

        except KeyboardInterrupt:
            print("\n[*] Agent dừng."); break
        except Exception as e:
            print(f"[!] Lỗi poll: {e}")

        time.sleep(3)

if __name__ == '__main__':
    main()
