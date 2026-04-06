-- ============================================================
-- Neon PostgreSQL Schema cho PDCraw Admin
-- Chạy file này 1 lần trong Neon SQL Editor
-- ============================================================

-- Bảng truyện chính
CREATE TABLE IF NOT EXISTS stories (
    id                  SERIAL PRIMARY KEY,
    title               TEXT,
    slug                VARCHAR(255) UNIQUE,
    url                 TEXT,
    source              VARCHAR(50)  DEFAULT 'PD',
    category            TEXT,
    chapters            INTEGER      DEFAULT 0,
    downloaded_chapters INTEGER      DEFAULT 0,
    mapped_count        INTEGER      DEFAULT 0,
    crawl_status        VARCHAR(50)  DEFAULT 'pending',
    book_status         VARCHAR(50),
    last_account_idx    INTEGER,
    admin_control       VARCHAR(100),
    last_updated        TIMESTAMPTZ  DEFAULT NOW(),
    actual_chapters     INTEGER      DEFAULT 0,
    uploaded_chapters   INTEGER      DEFAULT 0,
    author              VARCHAR(255),
    views               VARCHAR(50),
    likes               VARCHAR(50),
    cover_url           VARCHAR(500),
    rating              VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_stories_crawl_status  ON stories(crawl_status);
CREATE INDEX IF NOT EXISTS idx_stories_admin_control ON stories(admin_control);
CREATE INDEX IF NOT EXISTS idx_stories_source        ON stories(source);

-- Bảng trạng thái tài khoản scraper
CREATE TABLE IF NOT EXISTS scraper_accounts_status (
    account_email  VARCHAR(255) PRIMARY KEY,
    account_index  INTEGER,
    locked_by      VARCHAR(100),
    source         VARCHAR(50)  DEFAULT 'PD',
    last_heartbeat TIMESTAMPTZ  DEFAULT NOW()
);

-- Migration: thêm column source nếu bảng đã tồn tại
ALTER TABLE scraper_accounts_status ADD COLUMN IF NOT EXISTS source VARCHAR(50) DEFAULT 'PD';

-- Bảng hàng đợi lệnh gửi tới local agent
CREATE TABLE IF NOT EXISTS agent_commands (
    id          SERIAL PRIMARY KEY,
    action      VARCHAR(100) NOT NULL,
    payload     TEXT,                        -- JSON string
    status      VARCHAR(50)  DEFAULT 'pending', -- pending | running | done | error
    result      TEXT,                        -- JSON string kết quả trả về
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agent_commands_status ON agent_commands(status);

-- Bảng key-value nhỏ (heartbeat, check_cache, v.v.)
CREATE TABLE IF NOT EXISTS agent_kv (
    key   VARCHAR(100) PRIMARY KEY,
    value TEXT
);

-- Migration: thêm storage_label cho stories (máy nào lưu truyện này)
ALTER TABLE stories ADD COLUMN IF NOT EXISTS storage_label VARCHAR(20) DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_stories_storage_label ON stories(storage_label);

-- Migration: thêm meta_status (trạng thái thu thập thông tin để đổi tên)
-- NULL = chưa xử lý, 'no_chapter_names' = không có tên chương, 'ready' = đã có meta JSON
ALTER TABLE stories ADD COLUMN IF NOT EXISTS meta_status VARCHAR(30) DEFAULT NULL;

-- ============================================================
-- Migrate data từ MariaDB (chạy sau khi dump ra CSV)
-- COPY stories FROM '/path/to/stories.csv' CSV HEADER;
-- Hoặc dùng pgloader / import thủ công qua Neon console
-- ============================================================
