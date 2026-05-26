import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Prefer Supabase vars (production); fall back to DB_* vars (local docker-compose).
_supabase_host = os.getenv("SUPABASE_HOST")

DB_CONFIG = {
    "host": _supabase_host or os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("SUPABASE_PORT") or os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("SUPABASE_DB") or os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("SUPABASE_USER") or os.getenv("DB_USER", "postgres"),
    "password": os.getenv("SUPABASE_PASSWORD") or os.getenv("DB_PASSWORD", ""),
    **({"sslmode": "require"} if _supabase_host else {}),
}


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(**DB_CONFIG)
