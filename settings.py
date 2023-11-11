import os

from dotenv import load_dotenv

load_dotenv()


def read_bool(value: str) -> bool:
    return value.lower() in ("1", "true")


DB_NAME = os.environ["DB_NAME"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_PASS = os.environ["DB_PASS"]
DB_USER = os.environ["DB_USER"]

REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])
REDIS_USER = os.environ["REDIS_USER"]
REDIS_PASS = os.environ["REDIS_PASS"]
REDIS_DB = int(os.environ["REDIS_DB"])
REDIS_USE_SSL = read_bool(os.environ["REDIS_USE_SSL"])

DISCORD_AC_WEBHOOK = os.environ["DISCORD_AC_WEBHOOK"]
