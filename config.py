import os

from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "football_db"),
    "user": os.getenv("DB_USER", "football"),
    "password": os.getenv("DB_PASSWORD", "football"),
}
