from __future__ import annotations
import psycopg2
from contextlib import contextmanager
from .config import settings

@contextmanager
def get_conn():
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_db,
        user=settings.pg_user,
        password=settings.pg_password,
    )
    try:
        yield conn
    finally:
        conn.close()
