import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_2x6udHZMytVl@ep-fragrant-hill-a1md82d2-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

def get_conn():
    """Trả về một psycopg2 connection với RealDictCursor."""
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=10
    )

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = get_conn()
        yield conn
    except Exception as e:
        print(f"Lỗi kết nối Neon: {e}")
        raise
    finally:
        if conn:
            conn.close()

def json_serial(obj):
    from datetime import datetime, date
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")
