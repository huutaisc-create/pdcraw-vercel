"""
delete_menu_map.py
------------------
Chạy 1 lần trên máy Windows:
  python delete_menu_map.py

Luồng:
  1. Đọc agent_config.json để lấy vercel_url, agent_secret, data_import_dir
  2. Poll agent_commands lấy lệnh 'delete_menu_map' đang pending
  3. Với mỗi story_id → query Vercel API lấy slug → xóa file menu_map_v1.json
  4. Báo done về DB
"""

import json, os, urllib.request, urllib.parse, sys

# ── Load config ──────────────────────────────────────────────────────────────
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'agent_config.json')
with open(CONFIG_PATH, encoding='utf-8') as f:
    cfg = json.load(f)

VERCEL_URL   = cfg['vercel_url'].rstrip('/')
SECRET       = cfg['agent_secret']
DATA_DIR     = cfg['data_import_dir']
HEADERS      = {'X-Agent-Secret': SECRET, 'Content-Type': 'application/json'}

def api_get(path):
    req = urllib.request.Request(f"{VERCEL_URL}{path}", headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

def api_post(path, body):
    data = json.dumps(body, ensure_ascii=False).encode()
    req  = urllib.request.Request(f"{VERCEL_URL}{path}", data=data, headers=HEADERS, method='POST')
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

# ── Poll lệnh delete_menu_map pending ────────────────────────────────────────
print("🔍 Đang lấy lệnh từ DB...")
resp = api_get('/api/agent?action=poll')

if not resp.get('has_command'):
    print("⚠  Không có lệnh nào đang chờ. Hãy nhấn nút 'XÓA MENU MAP' trên UI trước.")
    sys.exit(0)

if resp.get('action') != 'delete_menu_map':
    print(f"⚠  Lệnh hiện tại là '{resp.get('action')}', không phải delete_menu_map. Bỏ qua.")
    sys.exit(0)

cmd_id   = resp['id']
payload  = resp.get('payload', {})
ids      = payload.get('story_ids') or payload.get('ids', [])

if not ids:
    print("⚠  Không có story_id nào trong lệnh.")
    api_post('/api/agent?action=done', {'command_id': cmd_id, 'status': 'done', 'result': {'deleted': 0}})
    sys.exit(0)

print(f"📋 Tìm thấy lệnh xóa menu_map cho {len(ids)} truyện (cmd_id={cmd_id})")

# ── Lấy slug của từng story_id ────────────────────────────────────────────────
print("🔎 Đang lấy slug từ DB...")
slugs_resp = api_post('/api', {'action': 'get_slugs_by_ids', 'ids': ids})
stories    = slugs_resp.get('stories', [])  # [{id, slug}, ...]

if not stories:
    # fallback: thử dùng id làm tên thư mục
    stories = [{'id': i, 'slug': str(i)} for i in ids]
    print("⚠  Không lấy được slug, dùng ID làm tên thư mục.")

# ── Xóa file ──────────────────────────────────────────────────────────────────
deleted, skipped, errors = [], [], []

for s in stories:
    slug     = s.get('slug') or str(s.get('id'))
    filepath = os.path.join(DATA_DIR, slug, 'menu_map_v1.json')

    if os.path.exists(filepath):
        try:
            os.remove(filepath)
            deleted.append(slug)
            print(f"  ✅ Đã xóa: {filepath}")
        except Exception as e:
            errors.append(slug)
            print(f"  ❌ Lỗi xóa {filepath}: {e}")
    else:
        skipped.append(slug)
        print(f"  ⏭  Không tìm thấy: {filepath}")

# ── Báo done ─────────────────────────────────────────────────────────────────
result = {'deleted': len(deleted), 'skipped': len(skipped), 'errors': len(errors),
          'deleted_slugs': deleted, 'error_slugs': errors}
api_post('/api/agent?action=done', {'command_id': cmd_id, 'status': 'done', 'result': result})

print(f"\n🎉 Hoàn tất! Đã xóa: {len(deleted)} | Không có file: {len(skipped)} | Lỗi: {len(errors)}")
