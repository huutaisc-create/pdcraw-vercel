import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

# Lấy link kết nối từ Environment Variables trên Vercel Dashboard
# Hãy đảm bảo bạn đã thêm biến NEON_DATABASE_URL trên Vercel
DATABASE_URL = os.environ.get("NEON_DATABASE_URL", "")

@contextmanager
def get_db_connection():
    """
    Tạo kết nối tới PostgreSQL (Neon).
    Sử dụng: with get_db_connection() as conn:
    """
    conn = None
    try:
        conn = psycopg2.connect(
            DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        yield conn
    except Exception as e:
        print(f"Lỗi kết nối Database: {e}")
        raise
    finally:
        if conn:
            conn.close()

def json_serial(obj):
    """Xử lý các kiểu dữ liệu ngày tháng khi chuyển sang JSON."""
    from datetime import datetime, date
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Kiểu dữ liệu {type(obj)} không hỗ trợ JSON")