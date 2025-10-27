from db.connection import uaa_pool, gazago_pool
from pymysql.cursors import DictCursor

# -----------------------------
#  중복 체크
# -----------------------------
def user_code_exists(conn, code: str) -> bool:
    with conn.cursor(DictCursor) as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt FROM user WHERE user_code = %s", (code,))
        return cursor.fetchone()["cnt"] > 0


def referral_code_exists(conn, code: str) -> bool:
    with conn.cursor(DictCursor) as cursor:
        cursor.execute("SELECT COUNT(*) AS cnt FROM user_info WHERE referral_code = %s", (code,))
        return cursor.fetchone()["cnt"] > 0


# -----------------------------
#  업데이트 함수 (commit 포함)
# -----------------------------
def update_user_code(conn, user_id: int, code: str):
    """user_code 업데이트 + commit"""
    with conn.cursor(DictCursor) as cursor:
        cursor.execute("UPDATE user SET user_code = %s WHERE id = %s", (code, user_id))
    conn.commit()


def update_referral_code(conn, bridge_id: int, code: str):
    """referral_code 업데이트 + commit
    - bridge_id 기준으로만 업데이트
    """
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(
            "UPDATE user_info SET referral_code = %s WHERE user_id = %s",
            (code, bridge_id),
        )
    conn.commit()


# -----------------------------
#  조회 함수
# -----------------------------
def get_all_users_with_bridge():
    """
    uaa.user + user_bridge join 결과 모든 user_id 조회
    """
    conn = uaa_pool.get_conn()
    try:
        with conn.cursor(DictCursor) as cursor:
            sql = """
                SELECT u.id AS user_id
                FROM user u
                JOIN user_bridge ub ON ub.user_id = u.id
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [row["user_id"] for row in rows]
    finally:
        uaa_pool.release_conn(conn)


def get_users_missing_referral():
    """
    referral_code가 NULL 또는 ''인 유저 조회
    - user_info.user_id = user_bridge.id 기준
    """
    conn = uaa_pool.get_conn()
    try:
        with conn.cursor(DictCursor) as cursor:
            sql = """
                SELECT u.id AS user_id, ub.id AS bridge_id
                FROM db_uaa.user u
                JOIN db_uaa.user_bridge ub ON ub.user_id = u.id
                JOIN db_gazago.user_info ui ON ui.user_id = ub.id
                WHERE ui.referral_code IS NULL OR ui.referral_code = ''
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [{"user_id": row["user_id"], "bridge_id": row["bridge_id"]} for row in rows]
    finally:
        uaa_pool.release_conn(conn)
