import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("SUPABASE_HOST"),
    "port": int(os.getenv("SUPABASE_PORT", 5432)),
    "dbname": os.getenv("SUPABASE_DB", "postgres"),
    "user": os.getenv("SUPABASE_USER", "postgres"),
    "password": os.getenv("SUPABASE_PASSWORD"),
    "sslmode": "require",
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)
