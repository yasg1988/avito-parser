import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

SCAN_DELAY_SEARCH = float(os.getenv("SCAN_DELAY_SEARCH", "4"))
SCAN_DELAY_HOUSE = float(os.getenv("SCAN_DELAY_HOUSE", "6"))
MAX_CONSECUTIVE_ERRORS = int(os.getenv("MAX_CONSECUTIVE_ERRORS", "5"))

BASE_URL = "https://www.avito.ru"
CITY = "yoshkar-ola"

SEARCH_CATEGORIES = {
    "sale": "prodam-ASgBAgICAUSSA8YQ",
    "rent": "sdam-ASgBAgICAUSSA8gQ",
}

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
)
