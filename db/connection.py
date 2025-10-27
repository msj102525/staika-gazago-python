import pymysql
from queue import Queue
from dotenv import load_dotenv
import os

load_dotenv()  # .env 파일 로드

class MySQLPool:
    def __init__(self, host, port, user, password, database, pool_size=10):
        self.pool = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            conn = pymysql.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                database=database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
            self.pool.put(conn)

    def get_conn(self):
        return self.pool.get()

    def release_conn(self, conn):
        self.pool.put(conn)

    def close_all(self):
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()


# -----------------------------
# UAA Pool
# -----------------------------
uaa_pool = MySQLPool(
    host=os.getenv("UAA_DB_HOST"),
    port=os.getenv("UAA_DB_PORT"),
    user=os.getenv("UAA_DB_USER"),
    password=os.getenv("UAA_DB_PASSWORD"),
    database=os.getenv("UAA_DB_NAME"),
    pool_size=int(os.getenv("UAA_DB_POOL_SIZE", 10)),
)

# -----------------------------
# GAZAGO Pool
# -----------------------------
gazago_pool = MySQLPool(
    host=os.getenv("GAZAGO_DB_HOST"),
    port=os.getenv("GAZAGO_DB_PORT"),
    user=os.getenv("GAZAGO_DB_USER"),
    password=os.getenv("GAZAGO_DB_PASSWORD"),
    database=os.getenv("GAZAGO_DB_NAME"),
    pool_size=int(os.getenv("GAZAGO_DB_POOL_SIZE", 10)),
)
