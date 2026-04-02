import psycopg2
import os
from psycopg2.extras import RealDictCursor

# Lấy chuỗi kết nối từ Environment Variable trên Vercel
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    # Kết nối tới Postgres (Neon) dùng RealDictCursor để trả về dạng dictionary
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn