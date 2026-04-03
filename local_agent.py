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
    "vercel_url":         "https://your-app.vercel.app",
    "agent_secret":       "changeme",
    "admin_name":         "Admin",
    "data_import_dir":    "D:\\Webtruyen\\pdcraw\\data_import",
    "scraper_script":     "D:\\Webtruyen\\pdcraw\\pd_scraper_fast-v1.py",
    "discovery_script":   "D:\\Webtruyen\\pdcraw\\pd_discovery_auto.py",
    "check_update_script":"D:\\Webtruyen\\pdcraw\\check_update.py",
    "accounts_file":      "D:\\Webtruyen\\pdcraw\\accounts.txt",
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
IMPORT_DIR    = CFG['data_import_dir']
def resolve_path(p):
    if not p: return p
    if not os.path.isabs(p):
        return os.path.abspath(os.path.join(os.path.dirname(__file__), p))
    return p

SCRAPER_PATH  = resolve_path(CFG['scraper_script'])
DISCOVERY_PATH= resolve_path(CFG['discovery_script'])
CHECK_UPDATE  = resolve_path(CFG['check_update_script'])
ACCOUNTS_FILE = resolve_path(CFG['accounts_file'])

HEADERS = {'X-Agent-Secret': AGENT_SECRET, 'Content-Type': 'application/json'}

# Tracking running scraper PIDs
SCRAPER_PIDS = []
PIDS_LOCK    = threading.Lock()

# Tracking cmd_ids đang xử lý để tránh chạy lại
PROCESSING_IDS = set()
PROCESSING_LOCK = threading.Lock()

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
    source        = payload.get('source', 'PD')
    account_idxs  = account_idxs[:threads]

    scraper_dir = os.path.dirname(os.path.abspath(SCRAPER_PATH))
    stop_file = os.path.join(scraper_dir, 'stop.signal')
    if os.path.exists(stop_file):
        os.remove(stop_file)

    pids = []
    for i, acc_idx in enumerate(account_idxs):
        if os.path.exists(stop_file):
            break
        try:
            proc = subprocess.Popen(
                [sys.executable, SCRAPER_PATH, str(acc_idx), '--admin', admin],
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            pids.append(proc.pid)
            print(f"[+] Started scraper PID {proc.pid} (account {acc_idx})")
        except Exception as e:
            print(f"[!] Failed to start scraper for account {acc_idx}: {e}")
            continue

        # Nếu không phải bot cuối: đợi bot này claim + lưu chương đầu
        if i < len(account_idxs) - 1:
            _wait_bot_claimed_story(acc_idx)

    with PIDS_LOCK:
        SCRAPER_PIDS.extend(pids)

    report_done(cmd_id, {'success': True, 'started': len(pids), 'pids': pids})

def handle_kill_scrapers(payload, cmd_id):
    global SCRAPER_PIDS
    scraper_dir = os.path.dirname(os.path.abspath(SCRAPER_PATH))
    stop_file = os.path.join(scraper_dir, 'stop.signal')
    with open(stop_file, 'w') as f:
        f.write('STOP')

    with PIDS_LOCK:
        all_pids = list(set(SCRAPER_PIDS))

    for pid in all_pids:
        try:
            subprocess.run(f"taskkill /PID {pid} /F /T", shell=True, capture_output=True)
        except: pass

    try:
        subprocess.run("taskkill /F /IM chromedriver.exe /T", shell=True, capture_output=True)
    except: pass

    with PIDS_LOCK:
        SCRAPER_PIDS = []

    report_done(cmd_id, {'success': True, 'killed': len(all_pids)})

def handle_submit_discovery(payload, cmd_id):
    url    = payload.get('url', '')
    source = payload.get('source', 'PD')
    try:
        proc = subprocess.Popen(
            [sys.executable, DISCOVERY_PATH, '--url', url, '--source', source],
            creationflags=subprocess.CREATE_NEW_CONSOLE
        )
        # Đánh dấu command là running, không done ngay
        _request('POST', '/api/agent?action=done', {
            'command_id': cmd_id,
            'result': {'success': True, 'pid': proc.pid},
            'status': 'running'
        })
        # Poll kết quả discovery
        threading.Thread(target=_wait_discovery, args=(cmd_id, proc), daemon=True).start()
    except Exception as e:
        report_done(cmd_id, {'success': False, 'message': str(e)}, 'error')

def _wait_discovery(cmd_id, proc):
    """Chờ discovery_conflicts.json rồi báo kết quả."""
    result_file = 'discovery_conflicts.json'
    for _ in range(300):  # tối đa 5 phút
        time.sleep(2)
        if os.path.exists(result_file):
            try:
                with open(result_file, encoding='utf-8') as f:
                    result = json.load(f)
                report_done(cmd_id, result)
                return
            except: pass
    report_done(cmd_id, {'new': 0, 'conflicts': []})

def handle_scan_updates(payload, cmd_id):
    try:
        proc = subprocess.Popen(
            [sys.executable, CHECK_UPDATE],
            creationflags=subprocess.CREATE_NEW_CONSOLE
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

# ── Main loop ─────────────────────────────────────────────────────────────────

HANDLERS = {
    'start_scraper':       handle_start_scraper,
    'kill_scrapers':       handle_kill_scrapers,
    'submit_discovery':    handle_submit_discovery,
    'scan_updates':        handle_scan_updates,
    'sync_selected':       handle_sync_selected,
    'check_upload_content':handle_check_upload_content,
    'do_upload':           handle_do_upload,
}

def main():
    print(f"[*] PDCraw Local Agent khởi động")
    print(f"[*] Kết nối tới: {VERCEL_URL}")
    print(f"[*] Nhấn Ctrl+C để thoát\n")

    hb_counter = 0
    while True:
        try:
            # Heartbeat mỗi 30s
            hb_counter += 1
            if hb_counter >= 10:
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
