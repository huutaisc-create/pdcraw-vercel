"""
api/agent.py — Endpoint dành riêng cho local_agent.py
- GET  /api/agent?action=poll          → lấy lệnh pending
- POST /api/agent?action=done          → báo kết quả xong
- POST /api/agent?action=heartbeat     → heartbeat
- POST /api/agent?action=update_story  → agent cập nhật DB sau khi scrape
"""
from http.server import BaseHTTPRequestHandler
import json, os, datetime, urllib.parse, sys

def _import_db():
    _api_dir = os.path.dirname(os.path.abspath(__file__))
    if _api_dir not in sys.path:
        sys.path.insert(0, _api_dir)
    from _db import get_conn, json_serial
    return get_conn, json_serial

AGENT_SECRET = os.environ.get("AGENT_SECRET", "changeme")


class handler(BaseHTTPRequestHandler):

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Agent-Secret')

    def _json(self, data, status=200):
        _, json_serial = _import_db()
        body = json.dumps(data, default=json_serial, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _auth(self):
        secret = self.headers.get('X-Agent-Secret', '')
        if secret != AGENT_SECRET:
            self._json({'error': 'Unauthorized'}, 401)
            return False
        return True

    def _body(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    # ── GET /api/agent?action=poll ───────────────────────────────────────────
    def do_GET(self):
        if not self._auth(): return
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        action = params.get('action', [''])[0]

        if action == 'poll':
            try:
                get_conn, _ = _import_db()
                conn = get_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT id, action, payload FROM agent_commands
                    WHERE status = 'pending'
                    ORDER BY created_at ASC
                    LIMIT 1
                """)
                row = cur.fetchone()
                conn.close()
                if row:
                    self._json({
                        'has_command': True,
                        'id': row['id'],
                        'action': row['action'],
                        'payload': json.loads(row['payload']) if row['payload'] else {}
                    })
                else:
                    self._json({'has_command': False})
            except Exception as e:
                self._json({'error': str(e)}, 500)

        elif action == 'poll_result':
            # Ki\u1ec3m tra k\u1ebft qu\u1ea3 1 l\u1ec7nh theo command_id
            cmd_id = params.get('command_id', [None])[0]
            if not cmd_id:
                self._json({'error': 'Missing command_id'}, 400); return
            try:
                get_conn, _ = _import_db()
                conn = get_conn(); cur = conn.cursor()
                cur.execute("""
                    SELECT id, action, status, result, created_at, finished_at
                    FROM agent_commands WHERE id = %s
                """, (int(cmd_id),))
                row = cur.fetchone()
                conn.close()
                if row:
                    self._json({
                        'id': row['id'],
                        'action': row['action'],
                        'status': row['status'],
                        'result': json.loads(row['result']) if row['result'] else None,
                    })
                else:
                    self._json({'error': 'Command not found'}, 404)
            except Exception as e:
                self._json({'error': str(e)}, 500)

        else:
            self._json({'error': 'unknown action'}, 400)

    # ── POST /api/agent ───────────────────────────────────────────────────────
    def do_POST(self):
        if not self._auth(): return
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        action = params.get('action', [''])[0]
        data   = self._body()

        try:
            get_conn, _ = _import_db()
            conn = get_conn(); cur = conn.cursor()

            # Báo lệnh đã xong + lưu kết quả
            if action == 'done':
                cmd_id = data['command_id']
                result = data.get('result')
                status = data.get('status', 'done')  # 'done' hoặc 'error'
                cur.execute("""
                    UPDATE agent_commands
                    SET status=%s, result=%s, finished_at=%s
                    WHERE id=%s
                """, (status, json.dumps(result, ensure_ascii=False) if result else None,
                      datetime.datetime.utcnow(), cmd_id))
                conn.commit()
                self._json({'success': True})

            # Heartbeat — agent báo còn sống
            elif action == 'heartbeat':
                agent_id = data.get('agent_id', 'default')
                running  = data.get('running_scrapers', 0)
                cur.execute("""
                    INSERT INTO agent_kv(key, value)
                    VALUES ('heartbeat', %s)
                    ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
                """, (json.dumps({
                    'agent_id': agent_id,
                    'running': running,
                    'ts': datetime.datetime.utcnow().isoformat()
                }),))
                conn.commit()
                self._json({'success': True})

            # Agent cập nhật DB story sau khi scrape xong 1 batch
            elif action == 'update_story':
                sid              = data['story_id']
                downloaded       = data.get('downloaded_chapters')
                status_val       = data.get('crawl_status')
                uploaded         = data.get('uploaded_chapters')
                actual           = data.get('actual_chapters')
                sets, args = [], []
                if downloaded is not None: sets.append("downloaded_chapters=%s"); args.append(downloaded)
                if status_val:             sets.append("crawl_status=%s");         args.append(status_val)
                if uploaded is not None:   sets.append("uploaded_chapters=%s");    args.append(uploaded)
                if actual is not None:     sets.append("actual_chapters=%s");      args.append(actual)
                if sets:
                    args.append(sid)
                    cur.execute(f"UPDATE stories SET {', '.join(sets)}, last_updated=NOW() WHERE id=%s", args)
                    conn.commit()
                self._json({'success': True})

            # Agent bulk-update nhiều stories (sau sync)
            elif action == 'bulk_update_stories':
                rows = data.get('rows', [])
                for row in rows:
                    sid = row.get('id')
                    if not sid: continue
                    sets, args = [], []
                    for col in ('downloaded_chapters', 'actual_chapters', 'crawl_status',
                                'uploaded_chapters', 'mapped_count'):
                        if col in row:
                            sets.append(f"{col}=%s"); args.append(row[col])
                    if sets:
                        args.append(sid)
                        cur.execute(f"UPDATE stories SET {', '.join(sets)}, last_updated=NOW() WHERE id=%s", args)
                conn.commit()
                self._json({'success': True, 'updated': len(rows)})

            # Lấy danh sách stories cần scrape (agent dùng khi claim task)
            elif action == 'claim_story':
                acc_idx    = data.get('account_index', -1)
                admin_name = data.get('admin_name')
                admin_filter = "AND (admin_control IS NULL OR admin_control = '')"
                f_args = []
                if admin_name:
                    admin_filter = "AND (admin_control = %s OR admin_control IS NULL OR admin_control = '')"
                    f_args = [admin_name]

                cur.execute(f"""
                    SELECT id, slug, title, url, downloaded_chapters, chapters, crawl_status
                    FROM stories
                    WHERE (
                        crawl_status = 'repairing'
                        OR (crawl_status = 'selected' AND (last_account_idx = %s OR last_account_idx IS NULL))
                        OR (crawl_status = 'crawling' AND (last_account_idx = %s OR last_updated < NOW() - INTERVAL '5 minutes'))
                    )
                    {admin_filter}
                    ORDER BY
                        CASE WHEN crawl_status='repairing' THEN 0
                             WHEN crawl_status='crawling' AND last_account_idx=%s THEN 1
                             WHEN crawl_status='selected' AND last_account_idx=%s THEN 2
                             WHEN crawl_status='selected' THEN 3
                             ELSE 4 END,
                        last_updated ASC
                    LIMIT 1
                """, [acc_idx, acc_idx] + f_args + [acc_idx, acc_idx])

                story = cur.fetchone()
                if story:
                    cur.execute("UPDATE stories SET crawl_status='crawling', last_account_idx=%s, last_updated=NOW() WHERE id=%s",
                                (acc_idx, story['id']))
                    conn.commit()
                    self._json({'success': True, 'story': dict(story)})
                else:
                    self._json({'success': True, 'story': None})

            else:
                self._json({'error': f'unknown action: {action}'}, 400)

            conn.close()

        except Exception as e:
            self._json({'error': str(e)}, 500)
