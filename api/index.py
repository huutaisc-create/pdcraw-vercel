"""
api/index.py — Entry point chính cho Vercel serverless.
Phục vụ HTML admin UI tại / và điều phối các /api/* routes.
"""
from http.server import BaseHTTPRequestHandler
import json, os, urllib.parse, sys

# Ensure the api/ directory is on the path so _db can be imported
_API_DIR = os.path.dirname(os.path.abspath(__file__))
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)
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
                admin         = params.get('admin',         [None])[0]
                machine_label = params.get('machine_label', [''])[0]
                page        = int(params.get('page',  [1])[0])
                limit, offset = 50, (page - 1) * 50

                where, args = [], []
                if source:      where.append("source = %s");                    args.append(source)
                if search:      where.append("(title ILIKE %s OR slug ILIKE %s)"); args += [f'%{search}%', f'%{search}%']
                if category:    where.append("category = %s");                  args.append(category)
                if book_status == 'Full':    where.append("book_status = 'Full'")
                elif book_status == 'Ongoing': where.append("book_status != 'Full'")

                # Filter theo admin + machine: chỉ thấy truyện của mình hoặc chưa gán ai
                if admin and machine_label:
                    where.append("""(
                        (admin_control = %s OR admin_control IS NULL OR admin_control = '')
                        AND (storage_label = %s OR storage_label IS NULL OR storage_label = '')
                    )""")
                    args += [admin, machine_label]
                elif admin:
                    where.append("(admin_control = %s OR admin_control IS NULL OR admin_control = '')")
                    args.append(admin)

                # Filter status: 'crawl_done' = truyện đã cào xong 100%
                if status == 'crawl_done':
                    where.append("COALESCE(chapters,0) > 0 AND CAST(COALESCE(downloaded_chapters,0) AS numeric) / COALESCE(chapters,0) >= 0.99")
                elif status:
                    where.append("crawl_status = %s"); args.append(status)
                else:
                    # Mặc định: ẩn truyện đã cào >= 99% (bỏ qua vài chương lẻ cuối)
                    where.append("NOT (COALESCE(chapters,0) > 0 AND CAST(COALESCE(downloaded_chapters,0) AS numeric) / COALESCE(chapters,0) >= 0.99)")

                w = ('WHERE ' + ' AND '.join(where)) if where else ''

                # Sort toàn DB: % cào (downloaded/chapters) giảm dần — gần xong lên đầu, chưa có chapters xuống cuối
                order = (
                    "CASE WHEN COALESCE(chapters,0) > 0 "
                    "     THEN CAST(COALESCE(downloaded_chapters,0) AS numeric) / COALESCE(chapters,0) "
                    "     ELSE -1 END DESC, "
                    "id ASC"
                )

                cur.execute(f"SELECT * FROM stories {w} ORDER BY {order} LIMIT %s OFFSET %s", args + [limit, offset])
                stories = [dict(r) for r in cur.fetchall()]

                cur.execute(f"SELECT COUNT(*) AS total FROM stories {w}", args)
                total = cur.fetchone()['total']

                if admin:
                    cur.execute("SELECT COUNT(*) AS q FROM stories WHERE crawl_status IN ('selected','repairing') AND admin_control = %s", (admin,))
                else:
                    cur.execute("SELECT COUNT(*) AS q FROM stories WHERE crawl_status IN ('selected','repairing')")
                q_count = cur.fetchone()['q']

                if admin:
                    cur.execute("SELECT COUNT(*) AS r FROM stories WHERE crawl_status = 'crawling' AND admin_control = %s", (admin,))
                else:
                    cur.execute("SELECT COUNT(*) AS r FROM stories WHERE crawl_status = 'crawling'")
                r_count = cur.fetchone()['r']

                q_mine = q_count  # đã filter theo admin rồi nên my_queue = queue

                self._json({
                    'stories': stories, 'total': total, 'page': page,
                    'total_pages': max(1, (total + limit - 1) // limit),
                    'stats': {'queue': q_count, 'running': r_count, 'my_queue': q_mine}
                })

            # ── list stories for meta generation ────────────────────────────
            elif action == 'list_stories_for_meta':
                cur.execute("""
                    SELECT id, title, slug, url, source
                    FROM stories
                    WHERE downloaded_chapters >= 1
                    ORDER BY id
                """)
                self._json({'success': True, 'stories': [dict(r) for r in cur.fetchall()]})

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
                # Chọn file account theo source (PD hoặc WIKI)
                source_param = params.get('source', ['PD'])[0].upper()
                if source_param == 'WIKI':
                    accounts_path = os.path.normpath(os.path.join(_API_DIR, '..', 'userpass-wiki.txt'))
                    lock_key = 'wiki'
                else:
                    accounts_path = os.path.normpath(os.path.join(_API_DIR, '..', 'accounts.txt'))
                    lock_key = 'pd'
                try:
                    with open(accounts_path, encoding='utf-8') as f:
                        raw = [l.strip() for l in f if l.strip() and not l.strip().startswith('#')]
                        # Chấp nhận cả format "user|pass" và "user" (pass mặc định)
                        raw = [l for l in raw if l]
                except:
                    raw = []
                lock_table_key = f'accounts_{lock_key}'
                cur.execute("SELECT account_email, locked_by FROM scraper_accounts_status WHERE source = %s", (source_param,))
                lock_map = {r['account_email']: r['locked_by'] for r in cur.fetchall()}
                accounts = [
                    {'index': i, 'email': line.split('|')[0], 'locked_by': lock_map.get(line.split('|')[0])}
                    for i, line in enumerate(raw)
                ]
                self._json({'accounts': accounts})

            # ── bot config ──────────────────────────────────────────────────
            elif action == 'get_bot_config':
                cur.execute("SELECT value FROM agent_kv WHERE key='bot_config'")
                row = cur.fetchone()
                if row:
                    config = json.loads(row['value'])
                else:
                    config = {'total_bots': 1, 'startup_delay': 60}
                self._json({'success': True, 'config': config})

            # ── machine labels ───────────────────────────────────────────────
            elif action == 'get_machine_labels':
                cur.execute("SELECT value FROM agent_kv WHERE key='machine_labels'")
                row = cur.fetchone()
                labels = json.loads(row['value']) if row else ['A', 'B', 'C', 'D']
                self._json({'labels': labels})

            # ── get crawling story by acc_idx (dùng để chờ bot claim) ────
            elif action == 'get_story':
                sid = params.get('id', [None])[0]
                if sid:
                    cur.execute("SELECT id, title, slug, url FROM stories WHERE id=%s", (int(sid),))
                    row = cur.fetchone()
                    self._json({'story': dict(row) if row else None})
                else:
                    self._json({'story': None})

            elif action == 'get_crawling_story':
                acc_idx = params.get('acc_idx', [None])[0]
                if acc_idx is not None:
                    cur.execute(
                        "SELECT id, downloaded_chapters, crawl_status FROM stories WHERE last_account_idx=%s AND crawl_status='crawling' LIMIT 1",
                        (int(acc_idx),)
                    )
                    row = cur.fetchone()
                    if row:
                        self._json({'story_id': row['id'], 'downloaded_chapters': row['downloaded_chapters'], 'crawl_status': row['crawl_status']})
                    else:
                        self._json({'story_id': None})
                else:
                    self._json({'story_id': None})

            # ── agent status (heartbeat check) ──────────────────────────────
            elif action == 'agent_status':
                import datetime
                cur.execute("SELECT value FROM agent_kv WHERE key='heartbeat'")
                row = cur.fetchone()
                if row:
                    hb = json.loads(row['value'])
                    ts_str = hb.get('ts', '2000-01-01T00:00:00')
                    try:
                        ts = datetime.datetime.fromisoformat(ts_str)
                    except Exception:
                        ts = datetime.datetime(2000, 1, 1)
                    age = (datetime.datetime.utcnow() - ts).total_seconds()
                    online = age < 60
                    self._json({'online': online, 'running': hb.get('running', 0), 'age_seconds': int(age)})
                else:
                    self._json({'online': False, 'running': 0})

            # ── get command result (frontend poll) ────────────────────────────────
            elif action == 'get_command_result':
                cmd_id = params.get('command_id', [None])[0]
                if not cmd_id or not cmd_id.isdigit():
                    self._json({'error': 'Missing or invalid command_id'}, 400)
                else:
                    cur.execute(
                        "SELECT id, action, status, result FROM agent_commands WHERE id = %s",
                        (int(cmd_id),)
                    )
                    row = cur.fetchone()
                    if row:
                        self._json({
                            'id': row['id'],
                            'action': row['action'],
                            'status': row['status'],
                            'result': json.loads(row['result']) if row['result'] else None,
                        })
                    else:
                        self._json({'error': 'Not found'}, 404)

            # ── list queue stories (per-admin) ──────────────────────────────
            elif action == 'list_queue':
                admin = params.get('admin', [''])[0]
                if not admin:
                    self._json({'success': False, 'message': 'Missing admin'}, 400)
                else:
                    cur.execute("""
                        SELECT id, title, slug, crawl_status, downloaded_chapters, chapters
                        FROM stories
                        WHERE crawl_status IN ('selected','repairing')
                          AND admin_control = %s
                        ORDER BY id
                    """, (admin,))
                    self._json({'success': True, 'stories': [dict(r) for r in cur.fetchall()]})

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

        # Auth check cho các action từ internal scripts (discovery, agent...)
        # Các action từ browser UI không cần secret (không có header này)
        AGENT_ONLY_ACTIONS = {'insert_story'}
        if action in AGENT_ONLY_ACTIONS:
            _secret = os.environ.get('AGENT_SECRET', 'changeme')
            if self.headers.get('X-Agent-Secret', '') != _secret:
                self._json({'error': 'Unauthorized'}, 401); return

        # Delegate heavy/local actions to agent command queue
        LOCAL_ACTIONS = {
            'start_scraper', 'kill_scrapers',
            'submit_discovery',
            'scan_updates',
            'do_upload', 'check_upload_content',
            'sync_progress', 'sync_selected',
            'delete_menu_map',
            'open_folder',
            'generate_meta_all',  # ← tạo meta.json cho tất cả truyện đã craw
            'import_local_data',
            'manual_crawl',       # ← cào tay từng chương
        }

        if action in LOCAL_ACTIONS:
            try:
                conn = get_conn(); cur = conn.cursor()
                import datetime

                # target_machine: lệnh chỉ gửi đến đúng máy của admin này
                target_machine = data.get('admin') or data.get('target_machine') or ''

                # Nếu gửi STOP khẩn cấp, huỷ bỏ lệnh pending của ĐÚNG admin đó
                if action == 'kill_scrapers':
                    if target_machine:
                        cur.execute("""
                            UPDATE agent_commands
                            SET status = 'cancelled',
                                result = '{"note":"Máy chủ đã huỷ bỏ do lệnh STOP KHẨN CẤP"}'
                            WHERE status = 'pending' AND target_machine = %s
                        """, (target_machine,))
                    else:
                        cur.execute("""
                            UPDATE agent_commands
                            SET status = 'cancelled',
                                result = '{"note":"Máy chủ đã huỷ bỏ do lệnh STOP KHẨN CẤP"}'
                            WHERE status = 'pending'
                        """)

                cur.execute("""
                    INSERT INTO agent_commands (action, payload, status, created_at, target_machine)
                    VALUES (%s, %s, 'pending', %s, %s)
                    RETURNING id
                """, (action, json.dumps(data, ensure_ascii=False), datetime.datetime.utcnow(), target_machine or None))
                cmd_id = cur.fetchone()['id']
                conn.commit(); conn.close()
                self._json({'success': True, 'queued': True, 'command_id': cmd_id,
                            'message': f'Lệnh {action} đã gửi tới local agent.'})
            except Exception as e:
                self._json({'success': False, 'message': str(e)}, 500)
            return

        # ── Status-check actions (POST nhưng chỉ đọc DB, không queue) ──────────
        if action == 'check_command':
            try:
                cmd_id = data.get('command_id')
                conn = get_conn(); cur = conn.cursor()
                cur.execute("SELECT status, result FROM agent_commands WHERE id=%s", (cmd_id,))
                row = cur.fetchone()
                conn.close()
                if not row:
                    self._json({'status': 'not_found'})
                else:
                    result = json.loads(row['result']) if row['result'] else {}
                    self._json({'status': row['status'], 'result': result})
            except Exception as e:
                self._json({'success': False, 'message': str(e)}, 500)
            return

        if action in ('check_discovery', 'check_update_status'):
            try:
                conn = get_conn(); cur = conn.cursor()
                act_map = {'check_discovery': 'submit_discovery', 'check_update_status': 'scan_updates'}
                source_action = act_map[action]
                cur.execute("""
                    SELECT status, result FROM agent_commands
                    WHERE action=%s ORDER BY created_at DESC LIMIT 1
                """, (source_action,))
                row = cur.fetchone()
                conn.close()
                if row and row['status'] == 'done':
                    result = json.loads(row['result']) if row['result'] else {}
                    self._json({'status': 'finished', 'results': result})
                else:
                    self._json({'status': 'running'})
            except Exception as e:
                self._json({'success': False, 'message': str(e)}, 500)
            return

        # ── DB-only actions ──────────────────────────────────────────────────
        try:
            conn = get_conn(); cur = conn.cursor()

            if action == 'cancel_all_pending':
                # Huỷ lệnh pending/running — chỉ của admin này
                target_machine = data.get('admin') or data.get('target_machine') or ''
                action_filter  = data.get('action_filter')
                if target_machine and action_filter:
                    cur.execute("""
                        UPDATE agent_commands SET status='cancelled'
                        WHERE status IN ('pending','running')
                          AND target_machine = %s AND action = %s
                    """, (target_machine, action_filter))
                elif target_machine:
                    cur.execute("""
                        UPDATE agent_commands SET status='cancelled'
                        WHERE status IN ('pending','running') AND target_machine = %s
                    """, (target_machine,))
                elif action_filter:
                    cur.execute("""
                        UPDATE agent_commands SET status='cancelled'
                        WHERE status IN ('pending','running') AND action = %s
                    """, (action_filter,))
                else:
                    cur.execute("""
                        UPDATE agent_commands SET status='cancelled'
                        WHERE status IN ('pending','running')
                    """)
                count = cur.rowcount
                conn.commit()
                self._json({'success': True, 'cancelled': count})

            elif action == 'set_bot_config':
                total_bots    = data.get('total_bots', 1)
                startup_delay = data.get('startup_delay', 60)
                config = {'total_bots': int(total_bots), 'startup_delay': int(startup_delay)}
                cur.execute("""
                    INSERT INTO agent_kv(key, value)
                    VALUES ('bot_config', %s)
                    ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
                """, (json.dumps(config, ensure_ascii=False),))
                conn.commit()
                self._json({'success': True, 'config': config})

            elif action == 'set_machine_labels':
                labels = data.get('labels', ['A', 'B', 'C', 'D'])
                cur.execute("""
                    INSERT INTO agent_kv(key, value) VALUES('machine_labels', %s)
                    ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
                """, (json.dumps(labels, ensure_ascii=False),))
                conn.commit()
                self._json({'success': True, 'labels': labels})

            elif action == 'toggle_select':
                sid = data['id']; is_sel = data['selected']; admin = data.get('admin')
                cur.execute("SELECT crawl_status, downloaded_chapters FROM stories WHERE id = %s", (sid,))
                row = cur.fetchone()
                if row and row['crawl_status'] != 'completed':
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
                        cur.execute("UPDATE stories SET crawl_status='selected', admin_control=%s WHERE id=%s AND crawl_status != 'completed'", (admin, sid))
                    else:
                        cur.execute("UPDATE stories SET crawl_status=CASE WHEN downloaded_chapters>0 THEN 'paused' ELSE 'pending' END, admin_control=NULL WHERE id=%s AND crawl_status != 'completed'", (sid,))
                conn.commit()
                self._json({'success': True, 'message': f'Updated {len(ids)} stories.'})

            elif action == 'reset_crawling_all':
                cur.execute("UPDATE stories SET crawl_status='selected', last_account_idx=NULL WHERE crawl_status IN ('crawling', 'repairing')")
                conn.commit()
                self._json({'success': True, 'affected': cur.rowcount})

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

            elif action == 'delete_stories':
                ids = data.get('ids', [])
                if not ids:
                    self._json({'success': False, 'message': 'Không có ID nào được chọn'}); conn.close(); return
                cur.execute("DELETE FROM stories WHERE id = ANY(%s)", (ids,))
                conn.commit()
                self._json({'success': True, 'deleted': cur.rowcount})

            elif action == 'crawl_missing':
                ids = data.get('ids', [])
                for sid in ids:
                    cur.execute("UPDATE stories SET crawl_status='repairing' WHERE id=%s", (sid,))
                conn.commit()
                self._json({'success': True, 'message': f'✅ Đã chuyển {len(ids)} truyện sang repairing'})

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
                admin  = data['admin']
                indexes = data.get('indexes', [])
                source_param = data.get('source', 'PD').upper()

                # Chỉ unlock account cùng source của admin này
                cur.execute("UPDATE scraper_accounts_status SET locked_by=NULL WHERE locked_by=%s AND source=%s", (admin, source_param))

                # Đọc đúng file theo source
                if source_param == 'WIKI':
                    accounts_path = os.path.join(os.path.dirname(__file__), '..', 'userpass-wiki.txt')
                else:
                    accounts_path = os.path.join(os.path.dirname(__file__), '..', 'accounts.txt')
                try:
                    with open(accounts_path, encoding='utf-8') as f:
                        raw = [l.strip() for l in f if l.strip() and not l.strip().startswith('#')]
                except: raw = []

                for idx in indexes:
                    if 0 <= idx < len(raw):
                        email = raw[idx].split('|')[0]
                        cur.execute("""
                            INSERT INTO scraper_accounts_status (account_email, account_index, locked_by, source)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (account_email) DO UPDATE
                                SET locked_by=EXCLUDED.locked_by, source=EXCLUDED.source
                        """, (email, idx, admin, source_param))
                conn.commit()
                self._json({'success': True, 'message': f'Locked {len(indexes)} accounts ({source_param}) for {admin}.'})

            elif action == 'change_storage_label':
                ids   = data.get('ids', [])
                label = data.get('label')  # None = xóa gán (set NULL)
                if ids:
                    cur.execute(
                        "UPDATE stories SET storage_label=%s, last_updated=NOW() WHERE id = ANY(%s)",
                        (label, ids)
                    )
                    conn.commit()
                self._json({'success': True, 'updated': len(ids)})

            elif action == 'update_meta_status':
                ids    = data.get('ids', [])
                status = data.get('status')  # 'ready' | 'no_chapter_names' | None (reset)
                if ids:
                    cur.execute(
                        "UPDATE stories SET meta_status=%s, last_updated=NOW() WHERE id = ANY(%s)",
                        (status, ids)
                    )
                    conn.commit()
                self._json({'success': True, 'updated': len(ids)})

            elif action == 'batch_check_slugs':
                slugs = data.get('slugs', [])
                if not slugs:
                    self._json({'existing': []}); conn.close(); return
                cur.execute("SELECT slug FROM stories WHERE slug = ANY(%s::text[])", (slugs,))
                existing = [r['slug'] for r in cur.fetchall()]
                self._json({'existing': existing})

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

            elif action == 'insert_story':
                # Insert truyện mới từ discovery (nếu slug đã tồn tại → báo conflict)
                slug = data.get('slug', '').strip()
                if not slug:
                    self._json({'success': False, 'message': 'Missing slug'}); conn.close(); return
                # Kiểm tra slug đã tồn tại chưa
                cur.execute("SELECT id, source, chapters FROM stories WHERE slug = %s", (slug,))
                existing = cur.fetchone()
                if existing:
                    self._json({
                        'success': False,
                        'conflict': True,
                        'existing': {
                            'id': existing['id'],
                            'source': existing['source'],
                            'chapters': existing['chapters'],
                        }
                    })
                else:
                    cur.execute("""
                        INSERT INTO stories
                            (slug, title, author, category, views, likes, chapters,
                             book_status, cover_url, rating, url, source, crawl_status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                    """, (
                        slug,
                        data.get('title', slug),
                        data.get('author', 'N/A'),
                        data.get('category', 'N/A'),
                        str(data.get('views', '0')),
                        str(data.get('likes', '0')),
                        int(data.get('chapters', 0) or 0),
                        data.get('book_status', 'Ongoing'),
                        data.get('cover_url', ''),
                        data.get('rating', 'N/A'),
                        data.get('url', ''),
                        data.get('source', 'PD'),
                    ))
                    conn.commit()
                    self._json({'success': True, 'inserted': 1, 'slug': slug})

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
