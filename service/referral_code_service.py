from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import string
import time
from pymysql.err import OperationalError, InterfaceError
from db.connection import uaa_pool, gazago_pool
from crud.referral_code_crud import (
    user_code_exists,
    referral_code_exists,
    update_user_code,
    update_referral_code,
    get_all_users_with_bridge,
    get_users_missing_referral,
)


# -----------------------------
#  유틸 함수
# -----------------------------
def generate_user_code() -> str:
    """8자리 대문자 랜덤 코드"""
    return "".join(random.choices(string.ascii_uppercase, k=8))


# -----------------------------
#  단일 유저 처리 로직
# -----------------------------
def update_user_task(user_id: int, bridge_id: int = None, max_retry: int = 3, retry_delay: int = 3):
    """
    단일 유저 처리:
    - 유저 코드 생성 및 중복 검사
    - update 실행 + commit
    - 연결 실패 시 자동 재시도
    """
    for attempt in range(1, max_retry + 1):
        uaa_conn, gazago_conn = None, None
        try:
            uaa_conn = uaa_pool.get_conn()
            gazago_conn = gazago_pool.get_conn()

            while True:
                code = generate_user_code()

                if not user_code_exists(uaa_conn, code) and not referral_code_exists(gazago_conn, code):
                    update_user_code(uaa_conn, user_id, code)
                    uaa_conn.commit()

                    update_referral_code(gazago_conn, bridge_id or user_id, code)
                    gazago_conn.commit()
                    return user_id, code

        except (OperationalError, InterfaceError) as db_err:
            print(f"⚠️ [user_id={user_id}] DB 연결 오류 발생, {attempt}/{max_retry}회 재시도 중... ({db_err})")
            time.sleep(retry_delay)
        except Exception as e:
            print(f"❌ [user_id={user_id}] 처리 실패: {e}")
            return None
        finally:
            try:
                if uaa_conn:
                    uaa_pool.release_conn(uaa_conn)
            except Exception:
                pass
            try:
                if gazago_conn:
                    gazago_pool.release_conn(gazago_conn)
            except Exception:
                pass

    print(f"🚫 [user_id={user_id}] {max_retry}회 재시도 후 실패")
    return None


# -----------------------------
#  병렬 실행 + tqdm + referral_code 재시도
# -----------------------------
def process_all_users_parallel(max_workers: int = 20, max_referral_retry: int = 3):
    """모든 유저 병렬 처리 + tqdm ETA + referral_code 미입력 재시도"""
    user_ids = get_all_users_with_bridge()
    results_set = set()

    print(f"\n🚀 총 {len(user_ids)}명의 유저 코드 업데이트 시작")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_user_task, uid, uid): uid for uid in user_ids}
        with tqdm(total=len(futures), desc="1차 전체 업데이트", unit="user", dynamic_ncols=True,
                  smoothing=0.2, bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} | ETA: {remaining} | {rate_fmt}  ") as pbar:
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results_set.add(result[0])
                except Exception as e:
                    uid = futures[future]
                    print(f"❌ [user_id={uid}] 처리 중 예외 발생: {e}")
                finally:
                    pbar.update(1)

    for retry_count in range(1, max_referral_retry + 1):
        missing_users = get_users_missing_referral()
        if not missing_users:
            print("\n🎉 모든 유저가 referral_code를 가지고 있습니다.")
            break

        print(f"\n🔄 [{retry_count}/{max_referral_retry}] referral_code 없는 유저 {len(missing_users)}명 재시도 중...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(update_user_task, u["user_id"], u["bridge_id"]): u["user_id"] for u in missing_users}
            with tqdm(total=len(futures), desc=f"Retry Round {retry_count}", unit="user", dynamic_ncols=True,
                      smoothing=0.2, bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} | ETA: {remaining} | {rate_fmt}  ") as pbar:
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            results_set.add(result[0])
                    except Exception as e:
                        uid = futures[future]
                        print(f"❌ [user_id={uid}] 재시도 중 예외 발생: {e}")
                    finally:
                        pbar.update(1)

        remaining = len(get_users_missing_referral())
        if remaining > 0:
            print(f"⚠️ [{retry_count}]회차 후 아직 {remaining}명 남음, 다음 라운드 진행")
        else:
            print("✅ 모든 유저 코드가 성공적으로 업데이트됨!")
            break

    print("\n🎯 모든 처리 완료 요약")
    print(f"총 성공: {len(results_set)}명 / 실패: {len(user_ids) - len(results_set)}명")

    return results_set
