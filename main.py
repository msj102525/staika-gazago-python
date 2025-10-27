from service.referral_code_service import process_all_users_parallel

if __name__ == "__main__":
    process_all_users_parallel(30, 3)  # max_workers=, max_referral_retry=
