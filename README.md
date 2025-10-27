# 프로젝트명
가자고 파이썬 모듈
---

## 요구 사항

- Python 3.9 이상
- MySQL 5.7 이상 (또는 호환 가능)
- 주요 패키지: `pymysql`, `tqdm`

---

## 설치 및 가상환경 설정

```bash
# 1. 가상 환경 생성
python -m venv .venv

# 2. 가상 환경 활성화
# 윈도우
.venv\Scripts\activate
# 리눅스 / macOS
source .venv/bin/activate

# 3. pip 업그레이드
python -m pip install --upgrade pip

# 4. 필수 패키지 설치
pip install -r requirements.txt


# .env 파일

# db_uaa
UAA_HOST=
UAA_PORT=
UAA_USER=
UAA_PASSWORD=
UAA_DATABASE=

# db_gazago
GAZAGO_HOST=
GAZAGO_PORT=
GAZAGO_USER=
GAZAGO_PASSWORD=
GAZAGO_DATABASE=