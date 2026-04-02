import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

# Lấy link Neon từ cấu hình Environment Variables trên Vercel
# Đảm bảo trên Vercel bạn đặt tên biến là NEON_DATABASE_URL
DATABASE_URL = os.environ.get("NEON_DATABASE_URL", "")

@contextmanager
def get_db_connection():
    """
    Tạo kết nối an toàn tới Neon PostgreSQL.
    Sử dụng 'with get_db_connection() as conn:' để tự động đóng kết nối.
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
    """Xử lý định dạng ngày tháng khi trả về dữ liệu JSON."""
    from datetime import datetime, date
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} không thể nén thành JSON")