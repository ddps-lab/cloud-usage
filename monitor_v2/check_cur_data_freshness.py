"""
monitor_v2/check_cur_data_freshness.py

전날(D-1) CUR 데이터가 Athena에 최종 반영되는 시각을 추적하는 스크립트.

- 1분마다 전날 total_cost / row_count 를 조회
- 직전 결과와 비교해 변화가 생기면 "갱신 감지" 로그 기록
- 연속 N회 동일하면 "데이터 안정화(최종 반영)" 로그 기록
- 로그는 이 스크립트와 같은 디렉터리의 cur_freshness.log 에 저장

환경변수 (data_cur.py 와 동일):
    ATHENA_DATABASE         예: hyu_ddps_logs
    ATHENA_OUTPUT_LOCATION  예: s3://my-bucket/athena-results/
    ATHENA_WORKGROUP        기본: primary
    ATHENA_REGION           기본: ap-northeast-2

실행:
    # 포그라운드
    python monitor_v2/check_cur_data_freshness.py

    # 백그라운드
    nohup python monitor_v2/check_cur_data_freshness.py > /dev/null 2>&1 &
    echo "PID: $!"
"""

import os
import sys
import time
import logging
import boto3
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

POLL_INTERVAL_SEC = 60       # 조회 주기 (초)
STABLE_COUNT      = 3        # 연속 동일 횟수 → 데이터 안정화 판정
ATHENA_MAX_WAIT   = 120      # Athena 쿼리 최대 대기 (초)
ATHENA_POLL       = 1.5

_DB        = 'hyu_ddps_logs'
_OUTPUT    = 's3://hyu-ddps-athena-results/cur-query-results/'
_WORKGROUP = 'primary'
_REGION    = os.environ.get('ATHENA_REGION', 'ap-northeast-2')

LOG_PATH  = Path(__file__).parent / 'cur_freshness.log'

# ---------------------------------------------------------------------------
# 로거 설정
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Athena 헬퍼
# ---------------------------------------------------------------------------

def _run_query(athena, sql: str) -> list:
    start_kwargs = {
        'QueryString': sql,
        'QueryExecutionContext': {'Database': _DB},
        'WorkGroup': _WORKGROUP,
    }
    if _OUTPUT:
        start_kwargs['ResultConfiguration'] = {'OutputLocation': _OUTPUT}

    resp    = athena.start_query_execution(**start_kwargs)
    exec_id = resp['QueryExecutionId']

    elapsed = 0.0
    while elapsed < ATHENA_MAX_WAIT:
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state  = status['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            break
        if state in ('FAILED', 'CANCELLED'):
            reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
            raise RuntimeError(f"Athena 쿼리 실패 [{state}]: {reason}")
        time.sleep(ATHENA_POLL)
        elapsed += ATHENA_POLL
    else:
        raise TimeoutError(f"Athena 쿼리 타임아웃 ({ATHENA_MAX_WAIT}s): exec_id={exec_id}")

    rows, headers, next_token = [], None, None
    while True:
        kwargs = {'QueryExecutionId': exec_id, 'MaxResults': 1000}
        if next_token:
            kwargs['NextToken'] = next_token
        result = athena.get_query_results(**kwargs)
        page   = result['ResultSet']['Rows']
        if headers is None:
            headers = [c.get('VarCharValue', '') for c in page[0]['Data']]
            page    = page[1:]
        for row in page:
            cells = [c.get('VarCharValue', '') for c in row['Data']]
            rows.append(dict(zip(headers, cells)))
        next_token = result.get('NextToken')
        if not next_token:
            break
    return rows


# ---------------------------------------------------------------------------
# 전날 데이터 조회
# ---------------------------------------------------------------------------

def fetch_yesterday_summary(athena, target: date) -> dict:
    """전날 row_count / total_cost / last_start / is_complete 반환."""
    year  = str(target.year)
    month = str(target.month)
    sql = f"""
        SELECT
            COUNT(*)                                AS row_count,
            SUM(line_item_unblended_cost)           AS total_cost,
            MAX(line_item_usage_start_date)         AS last_start,
            MAX(line_item_usage_start_date) = TIMESTAMP '{target} 23:00:00' AS is_complete
        FROM {_DB}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{target}')
    """
    rows = _run_query(athena, sql)
    if not rows:
        return {'row_count': '0', 'total_cost': '0', 'last_start': '', 'is_complete': 'false'}
    return rows[0]


# ---------------------------------------------------------------------------
# 메인 루프
# ---------------------------------------------------------------------------

def main():
    yesterday = date.today() - timedelta(days=1)
    log.info("=" * 60)
    log.info(f"CUR 데이터 반영 추적 시작  |  대상 날짜: {yesterday}")
    log.info(f"Athena DB: {_DB}  |  조회 주기: {POLL_INTERVAL_SEC}s  |  안정 판정: {STABLE_COUNT}회 연속 동일")
    log.info(f"로그 파일: {LOG_PATH}")
    log.info("=" * 60)

    athena = boto3.client('athena', region_name=_REGION)

    prev_key     = None
    stable_count = 0
    finalized    = False
    iteration    = 0

    while True:
        iteration += 1
        try:
            summary = fetch_yesterday_summary(athena, yesterday)
            row_count   = summary.get('row_count', '0')
            total_cost  = summary.get('total_cost', '0')
            last_start  = summary.get('last_start', '')
            is_complete = summary.get('is_complete', 'false')
            curr_key    = (row_count, total_cost)

            if prev_key is None:
                log.info(
                    f"[{iteration:04d}] 초기값  "
                    f"row_count={row_count}  total_cost={total_cost}  "
                    f"last_start={last_start}  is_complete={is_complete}"
                )
                prev_key     = curr_key
                stable_count = 1
            elif curr_key != prev_key:
                log.info(
                    f"[{iteration:04d}] *** 갱신 감지 ***  "
                    f"row_count: {prev_key[0]} → {row_count}  "
                    f"total_cost: {prev_key[1]} → {total_cost}  "
                    f"last_start={last_start}  is_complete={is_complete}"
                )
                prev_key     = curr_key
                stable_count = 1
                finalized    = False
            else:
                stable_count += 1
                log.info(
                    f"[{iteration:04d}] 변화 없음  "
                    f"row_count={row_count}  total_cost={total_cost}  "
                    f"last_start={last_start}  is_complete={is_complete}  "
                    f"(연속 {stable_count}회)"
                )
                if not finalized and stable_count >= STABLE_COUNT:
                    log.info(
                        f"[{iteration:04d}] === 데이터 안정화 (최종 반영 완료로 판단) ===  "
                        f"row_count={row_count}  total_cost={total_cost}  "
                        f"last_start={last_start}  is_complete={is_complete}"
                    )
                    finalized = True

        except Exception as e:
            log.error(f"[{iteration:04d}] 오류 발생: {e}")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == '__main__':
    main()
