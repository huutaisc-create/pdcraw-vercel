"""
api/index.py — Entry point chính cho Vercel serverless.
Phục vụ HTML admin UI tại / và điều phối các /api/* routes.
"""
from http.server import BaseHTTPRequestHandler
import json, os, urllib.parse
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from _db import get_conn, json_serial

# Lazy-load HTML để tránh crash khi cold-start nếu file chưa có
_HTML_PATH = os.path.join(os.path.dirname(__file__), '..', 'static', 'index.html')
_HTML_CACHE = None

def _get_html():
    global _HTML_CACHE
    if _HTML_CACHE is None:
        try:
            with open(_HTML_PATH, encoding='utf-8') as _f:
                _HTML_CACHE = _f.read()
        except FileNotFoundError:
            _HTML_CACHE = '<h1>Admin UI not found</h1>'
    return _HTML_CACHE


class handler(BaseHTTPRequestHandler):

    # ── helpers ──────────────────────────────────────────────────────────────

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Agent-Secret')

    def _json(self, data, status=200):
        body = json.dumps(data, default=json_serial, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length)) if length else {}

    # ── OPTIONS (preflight) ──────────────────────────────────────────────────

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    # ── GET ──────────────────────────────────────────────────────────────────

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        # Serve admin UI
        if path in ('/', ''):
            body = _get_html().encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self._cors()
            self.end_headers()
            self.wfile.write(body)
            return

        if not path.startswith('/api'):
            self._json({'error': 'not found'}, 404); return

        action = params.get('action', [None])[0]

        try:
            conn = get_conn()
            cur  = conn.cursor()

            # ── list stories ────────────────────────────────────────────────
            if action == 'list':
                search      = params.get('search',    [''])[0]
                status      = params.get('status',    [''])[0]
                category    = params.get('category',  [''])[0]
                book_status = params.get('book_status',[''])[0]
                source      = params.get('source',    [''])[0]
                admin       = params.get('admin',     [None])[0]
                page        = int(params.get('page',  [1])[0])
                limit, offset = 50, (page - 1) * 50

                where, args = [], []
                if source:      where.append("source = %s");                    args.append(source)
                if search:      where.append("(title ILIKE %s OR slug ILIKE %s)"); args += [f'%{search}%', f'%{search}%']
                if status:      where.append("crawl_status = %s");              args.append(status)
                if category:    where.append("category = %s");                  args.append(category)
                if book_status == 'Full':    where.append("book_status = 'Full'")
                elif book_status == 'Ongoing': where.append("book_status != 'Full'")

                w = ('WHERE ' + ' AND '.join(where)) if where else ''

                cur.execute(f"SELECT * FROM stories {w} ORDER BY CASE WHEN COALESCE(uploaded_chapters,0)>0 AND COALESCE(uploaded_chapters,0)>=COALESCE(downloaded_chapters,0) THEN 1 ELSE 0 END ASC, CASE crawl_status WHEN 'crawling' THEN 0 WHEN 'repairing' THEN 1 WHEN 'selected' THEN 2 WHEN 'paused' THEN 3 WHEN 'error' THEN 4 WHEN 'pending' THEN 5 WHEN 'completed' THEN 6 ELSE 5 END ASC, last_updated DESC LIMIT %s OFFSET %s", args + [limit, offset])
                stories = [dict(r) for r in cur.fetchall()]

                cur.execute(f"SELECT COUNT(*) AS total FROM stories {w}", args)
                total = cur.fetchone()['total']

                cur.execute("SELECT COUNT(*) AS q FROM stories WHERE crawl_status IN ('selected','repairing')")
                q_count = cur.fetchone()['q']
                cur.execute("SELECT COUNT(*) AS r FROM stories WHERE crawl_status = 'crawling'")
                r_count = cur.fetchone()['r']

                q_mine = 0
                if admin:
                    cur.execute("SELECT COUNT(*) AS qm FROM stories WHERE crawl_status IN ('selected','repairing') AND admin_control = %s", (admin,))
                    q_mine = cur.fetchone()['qm']

                self._json({
                    'stories': stories, 'total': total, 'page': page,
                    'total_pages': max(1, (total + limit - 1) // limit),
                    'stats': {'queue': q_count, 'running': r_count, 'my_queue': q_mine}
                })

            # ── categories ──────────────────────────────────────────────────
            elif action == 'get_categories':
                source = params.get('source', [''])[0]
                if source:
                    cur.execute("SELECT DISTINCT category FROM stories WHERE category IS NOT NULL AND source = %s ORDER BY category", (source,))
                else:
                    cur.execute("SELECT DISTINCT category FROM stories WHERE category IS NOT NULL ORDER BY category")
                self._json({'categories': [r['category'] for r in cur.fetchall()]})

            # ── accounts ────────────────────────────────────────────────────
            elif action == 'get_accounts':
                accounts_path = os.path.join(os.path.dirname(__file__), '..', 'accounts.txt')
                try:
                    with open(accounts_path, encoding='utf-8') as f:
                        raw = [l.strip() for l in f if '|' in l and not l.strip().startswith('#')]
                except:
                    raw = []
                cur.execute("SELECT account_email, locked_by FROM scraper_accounts_status")
                lock_map = {r['account_email']: r['locked_by'] for r in cur.fetchall()}
                accounts = [
                    {'index': i+1, 'email': line.split('|')[0], 'locked_by': lock_map.get(line.split('|')[0])}
                    for i, line in enumerate(raw)
                ]
                self._json({'accounts': accounts})

            else:
                self._json({'error': f'unknown GET action: {action}'}, 400)

            conn.close()

        except Exception as e:
            self._json({'error': str(e), 'stories': [], 'total': 0,
                        'page': 1, 'total_pages': 1,
                        'stats': {'queue': 0, 'running': 0, 'my_queue': 0}}, 500)

    # ── POST ─────────────────────────────────────────────────────────────────

    def do_POST(self):
        data   = self._body()
        action = data.get('action')

        # Delegate heavy/local actions to agent command queue
        LOCAL_ACTIONS = {
            'start_scraper', 'kill_scrapers',
            'submit_discovery', 'check_discovery',
            'scan_updates', 'check_update_status',
            'do_upload', 'check_upload_content',
            'sync_progress', 'sync_selected',
            'delete_menu_map',
        }

        if action in LOCAL_ACTIONS:
            try:
                conn = get_conn(); cur = conn.cursor()
                import datetime
                cur.execute("""
                    INSERT INTO agent_commands (action, payload, status, created_at)
                    VALUES (%s, %s, 'pending', %s)
                    RETURNING id
                """, (action, json.dumps(data, ensure_ascii=False), datetime.datetime.utcnow()))
                cmd_id = cur.fetchone()['id']
                conn.commit(); conn.close()
                self._json({'success': True, 'queued': True, 'command_id': cmd_id,
                            'message': f'Lệnh {action} đã gửi tới local agent.'})
            except Exception as e:
                self._json({'success': False, 'message': str(e)}, 500)
            return

        # ── DB-only actions ──────────────────────────────────────────────────
        try:
            conn = get_conn(); cur = conn.cursor()

            if action == 'toggle_select':
                sid = data['id']; is_sel = data['selected']; admin = data.get('admin')
                cur.execute("SELECT crawl_status, downloaded_chapters FROM stories WHERE id = %s", (sid,))
                row = cur.fetchone()
                if row:
                    if is_sel:
                        cur.execute("UPDATE stories SET crawl_status='selected', admin_control=%s WHERE id=%s", (admin, sid))
                    else:
                        ns = 'paused' if (row['downloaded_chapters'] or 0) > 0 else 'pending'
                        cur.execute("UPDATE stories SET crawl_status=%s, admin_control=NULL WHERE id=%s", (ns, sid))
                conn.commit()
                self._json({'success': True})

            elif action == 'batch_toggle_select':
                ids = data.get('ids', []); is_sel = data['selected']; admin = data.get('admin')
                for sid in ids:
                    if is_sel:
                        cur.execute("UPDATE stories SET crawl_status='selected', admin_control=%s WHERE id=%s", (admin, sid))
                    else:
                        cur.execute("UPDATE stories SET crawl_status=CASE WHEN downloaded_chapters>0 THEN 'paused' ELSE 'pending' END, admin_control=NULL WHERE id=%s", (sid,))
                conn.commit()
                self._json({'success': True, 'message': f'Updated {len(ids)} stories.'})

            elif action == 'get_slugs_by_ids':
                ids = data.get('ids', [])
                if ids:
                    cur.execute("SELECT id, slug FROM stories WHERE id = ANY(%s)", (ids,))
                    self._json({'success': True, 'stories': [dict(r) for r in cur.fetchall()]})
                else:
                    self._json({'success': True, 'stories': []})

            elif action == 'reset_bot':
                ids = data.get('ids', [])
                if ids:
                    cur.execute("UPDATE stories SET last_account_idx=NULL, last_updated=NOW() WHERE id = ANY(%s)", (ids,))
                    conn.commit()
                self._json({'success': True, 'updated': len(ids)})

            elif action == 'batch_change_status':
                ids    = data.get('ids', [])
                status = data.get('status', '')
                VALID  = {'pending','selected','crawling','paused','completed','error','repairing'}
                if status not in VALID:
                    self._json({'success': False, 'message': f'Invalid status: {status}'}); conn.close(); return
                if ids:
                    cur.execute(
                        f"UPDATE stories SET crawl_status=%s, last_updated=NOW() WHERE id = ANY(%s)",
                        (status, ids)
                    )
                    conn.commit()
                self._json({'success': True, 'updated': len(ids)})

            elif action == 'crawl_missing':
                ids = data.get('ids', [])
                for sid in ids:
                    cur.execute("UPDATE stories SET crawl_status='repairing' WHERE id=%s", (sid,))
                conn.commit()
                self._json({'success': True})

            elif action == 'apply_updates':
                items = data.get('items', [])
                for item in items:
                    cur.execute("UPDATE stories SET chapters=%s, crawl_status='selected' WHERE id=%s",
                                (item['new_chapters'], item['id']))
                conn.commit()
                self._json({'success': True, 'updated': len(items)})

            elif action == 'get_ongoing':
                src = data.get('source', '')
                if src:
                    cur.execute("SELECT id,title,source,chapters,downloaded_chapters,book_status FROM stories WHERE book_status!='Full' AND source=%s ORDER BY last_updated DESC", (src,))
                else:
                    cur.execute("SELECT id,title,source,chapters,downloaded_chapters,book_status FROM stories WHERE book_status!='Full' ORDER BY last_updated DESC")
                self._json({'success': True, 'stories': [dict(r) for r in cur.fetchall()]})

            elif action == 'lock_account_pool':
                admin = data['admin']; indexes = data.get('indexes', [])
                cur.execute("UPDATE scraper_accounts_status SET locked_by=NULL WHERE locked_by=%s", (admin,))
                accounts_path = os.path.join(os.path.dirname(__file__), '..', 'accounts.txt')
                try:
                    with open(accounts_path, encoding='utf-8') as f:
                        raw = [l.strip() for l in f if '|' in l and not l.strip().startswith('#')]
                except: raw = []
                for idx in indexes:
                    if 1 <= idx <= len(raw):
                        email = raw[idx-1].split('|')[0]
                        cur.execute("""
                            INSERT INTO scraper_accounts_status (account_email, account_index, locked_by)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (account_email) DO UPDATE SET locked_by=EXCLUDED.locked_by
                        """, (email, idx, admin))
                conn.commit()
                self._json({'success': True, 'message': f'Locked {len(indexes)} accounts for {admin}.'})

            elif action == 'resolve_conflicts':
                updates = data.get('updates', []); count = 0
                for item in updates:
                    f = item.get('full_data')
                    if f:
                        cur.execute("UPDATE stories SET views=%s,likes=%s,chapters=%s,book_status=%s,cover_url=%s,rating=%s,source=%s WHERE slug=%s",
                                    (f.get('views'), f.get('likes'), f.get('chapters'), f.get('book_status'),
                                     f.get('cover_url'), f.get('rating'), f.get('source','PD'), f.get('slug')))
                        count += 1
                conn.commit()
                self._json({'success': True, 'updated': count})

            elif action == 'reset_upload':
                import urllib.request as _ureq
                sid = data['story_id']; web_url = data.get('web_url','').rstrip('/'); secret = data.get('secret','')
                cur.execute("SELECT slug, title FROM stories WHERE id=%s", (sid,))
                row = cur.fetchone()
                if not row: self._json({'success': False, 'message': 'Không tìm thấy truyện'}); return
                errors = []
                try:
                    req = _ureq.Request(f"{web_url}/api/admin/stories/{row['slug']}",
                                        headers={'X-Upload-Secret': secret}, method='DELETE')
                    with _ureq.urlopen(req, timeout=15) as r:
                        result = json.loads(r.read())
                        if not result.get('success'): errors.append(result.get('message',''))
                except Exception as e: errors.append(str(e))
                cur.execute("UPDATE stories SET uploaded_chapters=0 WHERE id=%s", (sid,))
                conn.commit()
                msg = f"Reset OK{(' — Web lỗi: '+'; '.join(errors)) if errors else ''}"
                self._json({'success': True, 'message': msg})

            elif action in ('load_check_cache', 'save_check_cache', 'save_one_check_cache'):
                # Cache lưu trong DB (bảng agent_kv) thay vì file
                if action == 'load_check_cache':
                    cur.execute("SELECT value FROM agent_kv WHERE key='check_cache'")
                    row = cur.fetchone()
                    cache = json.loads(row['value']) if row else {}
                    self._json({'success': True, 'cache': cache})
                elif action == 'save_check_cache':
                    cache = data.get('cache', {})
                    cur.execute("INSERT INTO agent_kv(key,value) VALUES('check_cache',%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
                                (json.dumps(cache, ensure_ascii=False),))
                    conn.commit()
                    self._json({'success': True})
                elif action == 'save_one_check_cache':
                    cur.execute("SELECT value FROM agent_kv WHERE key='check_cache'")
                    row = cur.fetchone()
                    cache = json.loads(row['value']) if row else {}
                    cache[str(data.get('story_id'))] = data.get('entry', {})
                    cur.execute("INSERT INTO agent_kv(key,value) VALUES('check_cache',%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
                                (json.dumps(cache, ensure_ascii=False),))
                    conn.commit()
                    self._json({'success': True})

            elif action in ('check_discovery', 'check_update_status'):
                # Lấy kết quả từ agent_commands gần nhất
                act_map = {'check_discovery': 'submit_discovery', 'check_update_status': 'scan_updates'}
                source_action = act_map[action]
                cur.execute("""
                    SELECT status, result FROM agent_commands
                    WHERE action=%s ORDER BY created_at DESC LIMIT 1
                """, (source_action,))
                row = cur.fetchone()
                if row and row['status'] == 'done':
                    result = json.loads(row['result']) if row['result'] else {}
                    self._json({'status': 'finished', 'results': result})
                else:
                    self._json({'status': 'running'})

            else:
                self._json({'success': False, 'message': f'Unknown action: {action}'}, 400)

            conn.close()

        except Exception as e:
            self._json({'success': False, 'message': str(e)}, 500)
