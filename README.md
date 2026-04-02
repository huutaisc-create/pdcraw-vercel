# PDCraw Admin — Vercel + Neon Deployment Guide

## Cấu trúc project

```
pdcraw-vercel/
├── api/
│   ├── _db.py          # DB helper (Neon PostgreSQL)
│   ├── index.py        # API chính + serve UI
│   └── agent.py        # Endpoint cho local agent
├── static/
│   └── index.html      # Admin UI (giữ nguyên từ file cũ)
├── local_agent.py      # Chạy trên máy Windows local
├── migrate_to_neon.py  # Script migrate data 1 lần
├── schema_neon.sql     # PostgreSQL schema
├── accounts.txt        # Danh sách tài khoản scraper
├── requirements.txt
└── vercel.json
```

---

## Bước 1 — Tạo Neon Database

1. Vào https://neon.tech → tạo project mới
2. Copy **Connection String** dạng:
   `postgresql://user:pass@ep-xxx.aws.neon.tech/neondb?sslmode=require`
3. Vào **SQL Editor** → paste toàn bộ nội dung `schema_neon.sql` → Run

---

## Bước 2 — Migrate data từ MariaDB

Trên máy local:
```bash
pip install mysql-connector-python psycopg2-binary
```

Mở `migrate_to_neon.py`, thay `NEON_URL` bằng connection string thật, rồi:
```bash
python migrate_to_neon.py
```

---

## Bước 3 — Deploy lên Vercel

```bash
npm install -g vercel       # cài Vercel CLI (nếu chưa có)
cd pdcraw-vercel
vercel login
vercel --prod
```

Trong quá trình deploy, Vercel sẽ hỏi cài đặt. Chọn defaults.

### Thêm Environment Variables trên Vercel Dashboard:

| Key | Value |
|-----|-------|
| `NEON_DATABASE_URL` | Connection string từ Neon |
| `AGENT_SECRET` | Chuỗi bí mật tự đặt, ví dụ: `pdcraw_secret_2024` |

Vào: Vercel Dashboard → Project → Settings → Environment Variables

---

## Bước 4 — Cấu hình Local Agent

Copy `local_agent.py` vào thư mục pdcraw trên máy local.

Chạy lần đầu để tạo file config:
```bash
python local_agent.py
```

Mở `agent_config.json` vừa tạo, điền đúng các đường dẫn:
```json
{
    "vercel_url":          "https://your-app.vercel.app",
    "agent_secret":        "pdcraw_secret_2024",
    "admin_name":          "Admin Huy",
    "data_import_dir":     "D:\\Webtruyen\\pdcraw\\data_import",
    "scraper_script":      "D:\\Webtruyen\\pdcraw\\pd_scraper_fast-v1.py",
    "discovery_script":    "D:\\Webtruyen\\pdcraw\\pd_discovery_auto.py",
    "check_update_script": "D:\\Webtruyen\\pdcraw\\check_update.py",
    "accounts_file":       "D:\\Webtruyen\\pdcraw\\accounts.txt"
}
```

Chạy agent (để cửa sổ CMD mở suốt khi muốn dùng):
```bash
python local_agent.py
```

---

## Luồng hoạt động

```
Bạn bấm "START SCRAPER" trên web
    → Vercel ghi lệnh vào bảng agent_commands (Neon)
    → local_agent.py poll mỗi 3s, thấy lệnh
    → local_agent chạy pd_scraper_fast-v1.py trên máy local
    → Scraper cào xong → agent báo kết quả về Vercel
    → UI cập nhật
```

---

## Tính năng nào chạy ở đâu

| Tính năng | Vercel | Local Agent |
|-----------|--------|-------------|
| Xem danh sách truyện | ✅ | |
| Chọn/bỏ chọn truyện | ✅ | |
| Check update | → gửi lệnh | ✅ chạy Chrome |
| Start/Stop scraper | → gửi lệnh | ✅ chạy process |
| Check & Upload | → gửi lệnh | ✅ đọc file .txt |
| Discovery | → gửi lệnh | ✅ chạy Chrome |
| DB CRUD | ✅ | ✅ (update sau scrape) |

---

## Troubleshooting

**Agent không nhận lệnh:**
- Kiểm tra `vercel_url` và `agent_secret` trong `agent_config.json`
- Kiểm tra AGENT_SECRET trên Vercel Dashboard khớp với config

**DB connection failed:**
- Kiểm tra NEON_DATABASE_URL đúng và có `?sslmode=require` ở cuối

**Scraper không chạy:**
- Đảm bảo đường dẫn `scraper_script` trong config chính xác
- Đảm bảo `accounts.txt` đúng vị trí
