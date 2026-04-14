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
            # admin_name dùng để lọc lệnh — chỉ nhận lệnh của đúng admin/máy này
            admin_name = params.get('admin_name', [''])[0]
            try:
                get_conn, _ = _import_db()
                conn = get_conn(); cur = conn.cursor()
                # Atomic claim: chỉ lấy lệnh của admin này (target_machine = admin_name)
                # hoặc lệnh không có target (NULL) nếu không truyền admin_name
                if admin_name:
                    cur.execute("""
                        UPDATE agent_commands SET status = 'running'
                        WHERE id = (
                            SELECT id FROM agent_commands
                            WHERE status = 'pending'
                              AND target_machine = %s
                            ORDER BY created_at ASC
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING id, action, payload
                    """, (admin_name,))
                else:
                    cur.execute("""
                        UPDATE agent_commands SET status = 'running'
                        WHERE id = (
                            SELECT id FROM agent_commands
                            WHERE status = 'pending'
                              AND (target_machine IS NULL OR target_machine = '')
                            ORDER BY created_at ASC
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING id, action, payload
                    """)
                row = cur.fetchone()
                conn.commit()
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
                chapters         = data.get('chapters')
                meta_status      = data.get('meta_status', '__unset__')
                storage_label    = data.get('storage_label', '__unset__')
                sets, args = [], []
                if downloaded is not None:       sets.append("downloaded_chapters=%s"); args.append(downloaded)
                if status_val:                   sets.append("crawl_status=%s");         args.append(status_val)
                if uploaded is not None:         sets.append("uploaded_chapters=%s");    args.append(uploaded)
                if actual is not None:           sets.append("actual_chapters=%s");      args.append(actual)
                if chapters is not None:         sets.append("chapters=%s");             args.append(chapters)
                if meta_status != '__unset__':   sets.append("meta_status=%s");          args.append(meta_status)
                if storage_label != '__unset__': sets.append("storage_label=%s");        args.append(storage_label or None)
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

            # Lấy story để scrape — strict isolation theo admin + machine_label
            elif action == 'claim_story':
                acc_idx       = data.get('account_index', -1)
                admin_name    = data.get('admin_name', '')
                source_filter = data.get('source_filter')  # 'PD' | 'WIKI' | None
                machine_label = data.get('machine_label', '')

                # Bắt buộc có admin_name để tránh claim nhầm story của người khác
                if not admin_name:
                    self._json({'success': False, 'message': 'Missing admin_name'}, 400)
                    conn.close(); return

                # Lọc source
                source_clause = ""
                if source_filter == 'WIKI':
                    source_clause = "AND source = 'WIKI'"
                elif source_filter == 'PD':
                    source_clause = "AND (source = 'PD' OR source IS NULL OR source = '')"

                # storage_label: nhận story của máy mình HOẶC chưa được gán máy nào
                # (khi claim xong sẽ tự gán storage_label = machine_label)
                if machine_label:
                    storage_clause = "AND (storage_label = %s OR storage_label IS NULL OR storage_label = '')"
                    storage_args   = [machine_label]
                else:
                    storage_clause = "AND (storage_label IS NULL OR storage_label = '')"
                    storage_args   = []

                cur.execute(f"""
                    SELECT id, slug, title, url, downloaded_chapters, chapters, crawl_status
                    FROM stories
                    WHERE crawl_status IN ('selected', 'repairing')
                      AND admin_control = %s
                      {source_clause}
                      {storage_clause}
                    ORDER BY
                        CASE WHEN crawl_status = 'repairing' THEN 0 ELSE 1 END,
                        CASE WHEN last_account_idx = %s THEN 0 ELSE 1 END,
                        CASE WHEN COALESCE(chapters,0) > 0
                             THEN CAST(COALESCE(downloaded_chapters,0) AS numeric) / COALESCE(chapters,0)
                             ELSE -1 END DESC,
                        id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """, [admin_name] + storage_args + [acc_idx])

                story = cur.fetchone()
                if story:
                    cur.execute("""
                        UPDATE stories
                        SET crawl_status = 'crawling',
                            last_account_idx = %s,
                            storage_label = %s,
                            last_updated = NOW()
                        WHERE id = %s
                    """, (acc_idx, machine_label or None, story['id']))
                    conn.commit()
                    self._json({'success': True, 'story': dict(story)})
                else:
                    self._json({'success': True, 'story': None})

            # Recover story bị stuck — chỉ lấy lại story của đúng máy này
            elif action == 'recover_stuck_stories':
                admin_name    = data.get('admin_name', '')
                machine_label = data.get('machine_label', '')
                if not admin_name:
                    self._json({'success': False, 'message': 'Missing admin_name'}, 400)
                    conn.close(); return

                if machine_label:
                    storage_clause = "AND storage_label = %s"
                    storage_args   = [machine_label]
                else:
                    storage_clause = "AND (storage_label IS NULL OR storage_label = '')"
                    storage_args   = []

                cur.execute(f"""
                    UPDATE stories
                    SET crawl_status = 'selected', last_updated = NOW()
                    WHERE crawl_status = 'crawling'
                      AND admin_control = %s
                      {storage_clause}
                      AND last_updated < NOW() - INTERVAL '5 minutes'
                    RETURNING id, slug, title
                """, [admin_name] + storage_args)
                recovered = [dict(r) for r in cur.fetchall()]
                conn.commit()
                self._json({'success': True, 'recovered': recovered, 'count': len(recovered)})

            else:
                self._json({'error': f'unknown action: {action}'}, 400)

            conn.close()

        except Exception as e:
            self._json({'error': str(e)}, 500)
