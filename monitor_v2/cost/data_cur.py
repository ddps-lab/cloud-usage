"""
monitor_v2/cost/data_cur.py

Athena CUR 쿼리 기반 Cost 데이터 수집 모듈.

data.py (Cost Explorer API 방식)의 동등 구현.
반환 구조는 data.py 의 collect_all() 과 동일하므로
report.py 의 send_main1_report() 를 그대로 사용할 수 있다.

대상 테이블: hyu_ddps_logs.cur_logs
파티션:      year (STRING), month (STRING, 두 자리 zero-padded)

쿼리 대응 (queries.sql):
    Q1  fetch_daily_by_service          (d1)   → daily_d1
    Q2  fetch_daily_by_service          (d2)   → daily_d2
    Q3  fetch_daily_by_service_and_creator (d1) → by_creator
    Q4* fetch_daily_by_service_and_region  (d1) → by_region   (* SQL 수정: region 컬럼 사용)
    Q5  fetch_mtd_by_service_and_creator       → by_creator_mtd
    Q6  fetch_mtd_by_service_and_region        → by_region_mtd
    Q7  fetch_mtd_total                        → mtd_this
    Q8  fetch_cost_forecast → CE API 그대로 (data.py 공유)

환경변수:
    ATHENA_DATABASE        쿼리 대상 DB (기본: hyu_ddps_logs)
    ATHENA_OUTPUT_LOCATION S3 결과 저장 위치  예: s3://my-bucket/athena-results/
    ATHENA_WORKGROUP       Athena 워크그룹 (기본: primary)
"""

import os
import time
import boto3
from datetime import date, timedelta
import logging

from .data import fetch_cost_forecast  # Q8: CE API 재사용

log = logging.getLogger(__name__)

_ATHENA_DATABASE        = os.environ.get('ATHENA_DATABASE')
_ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION')
_ATHENA_WORKGROUP       = os.environ.get('ATHENA_WORKGROUP', 'primary')

_POLL_INTERVAL = 1.5   # seconds
_MAX_WAIT      = 120   # seconds


# ---------------------------------------------------------------------------
# Athena 실행 헬퍼
# ---------------------------------------------------------------------------

def _run_query(athena, sql: str) -> list:
    """
    Athena 쿼리를 실행하고 결과를 dict 리스트로 반환한다.
    헤더 행(첫 번째 row) 은 제외한다.

    Returns:
        [{'col': 'val', ...}, ...]
    """
    start_kwargs = {
        'QueryString': sql,
        'QueryExecutionContext': {'Database': _ATHENA_DATABASE},
        'WorkGroup': _ATHENA_WORKGROUP,
    }
    if _ATHENA_OUTPUT_LOCATION:
        start_kwargs['ResultConfiguration'] = {
            'OutputLocation': _ATHENA_OUTPUT_LOCATION,
        }

    resp    = athena.start_query_execution(**start_kwargs)
    exec_id = resp['QueryExecutionId']

    # 완료 대기
    elapsed = 0.0
    while elapsed < _MAX_WAIT:
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state  = status['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            break
        if state in ('FAILED', 'CANCELLED'):
            reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
            raise RuntimeError(
                f"Athena 쿼리 실패 [{state}]: {reason}\nSQL 앞 200자: {sql[:200]}"
            )
        time.sleep(_POLL_INTERVAL)
        elapsed += _POLL_INTERVAL
    else:
        raise TimeoutError(f"Athena 쿼리 타임아웃 ({_MAX_WAIT}s): exec_id={exec_id}")

    # 결과 수집 (페이지네이션)
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


def _partition(target: date) -> tuple:
    """(year_str, month_str) 파티션 값 반환. month는 zero-padding 없음 (예: '4')."""
    return str(target.year), str(target.month)


# ---------------------------------------------------------------------------
# 개별 쿼리 함수 (queries.sql 대응)
# ---------------------------------------------------------------------------

def fetch_daily_by_service_cur(athena, target_date: date) -> dict:
    """
    Q1 / Q2 해당.
    지정 날짜의 서비스별 일일 비용.

    Returns:
        {service: float}
    """
    year, month = _partition(target_date)
    #print("target_date", target_date)
    #print(year, month)
    sql = f"""
        SELECT
            product_product_name                  AS service,
            SUM(line_item_unblended_cost)         AS cost
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{target_date}')
        GROUP BY product_product_name
        HAVING SUM(line_item_unblended_cost) > 0.01
        ORDER BY cost DESC
    """
    rows = _run_query(athena, sql)
    return {
        r['service']: float(r['cost'])
        for r in rows
        if r.get('service') and r.get('cost')
    }


def fetch_daily_by_service_and_creator_cur(athena, d1_date: date) -> dict:
    """
    Q3 해당.
    D-1 서비스 + aws:createdBy 태그별 비용.

    Returns:
        {service: {creator: float}}
    """
    year, month = _partition(d1_date)
    sql = f"""
        SELECT
            product_product_name                                                AS service,
            CASE
                WHEN line_item_line_item_type = 'Tax'
                    THEN CONCAT('[Tax] ', product_product_name)
                WHEN product_product_name = 'AWS Data Transfer'
                    THEN '[공통] Data Transfer'
                WHEN product_product_name = 'AWS Cost Explorer'
                    THEN '[공통] Cost Explorer'
                WHEN product_product_name = 'AWS Support [Business]'
                    THEN '[공통] Support'
                WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
                    THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
                WHEN NULLIF(resource_tags_user_username, '') IS NOT NULL
                    THEN CONCAT('[username] ', resource_tags_user_username)
                WHEN NULLIF(resource_tags_user_requester, '') IS NOT NULL
                    THEN CONCAT('[requester] ', resource_tags_user_requester)
                WHEN NULLIF(resource_tags_user_project, '') IS NOT NULL
                    THEN CONCAT('[project] ', resource_tags_user_project)
                WHEN NULLIF(resource_tags_user_project_name, '') IS NOT NULL
                    THEN CONCAT('[project_name] ', resource_tags_user_project_name)
                WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
                    THEN CONCAT('[name] ', resource_tags_user_name)
                WHEN NULLIF(resource_tags_user_n_a_m_e, '') IS NOT NULL
                    THEN CONCAT('[n_a_m_e] ', resource_tags_user_n_a_m_e)
                WHEN NULLIF(resource_tags_user_environment, '') IS NOT NULL
                    THEN CONCAT('[environment] ', resource_tags_user_environment)
                WHEN line_item_line_item_type = 'Usage'
                    THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
                ELSE CONCAT(product_product_name, ' - 기타')
            END                                                                 AS creator,
            SUM(line_item_unblended_cost)                                       AS cost
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
          AND line_item_line_item_type != 'Tax'
        GROUP BY
            product_product_name,
            CASE
                WHEN line_item_line_item_type = 'Tax'
                    THEN CONCAT('[Tax] ', product_product_name)
                WHEN product_product_name = 'AWS Data Transfer'
                    THEN '[공통] Data Transfer'
                WHEN product_product_name = 'AWS Cost Explorer'
                    THEN '[공통] Cost Explorer'
                WHEN product_product_name = 'AWS Support [Business]'
                    THEN '[공통] Support'
                WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
                    THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
                WHEN NULLIF(resource_tags_user_username, '') IS NOT NULL
                    THEN CONCAT('[username] ', resource_tags_user_username)
                WHEN NULLIF(resource_tags_user_requester, '') IS NOT NULL
                    THEN CONCAT('[requester] ', resource_tags_user_requester)
                WHEN NULLIF(resource_tags_user_project, '') IS NOT NULL
                    THEN CONCAT('[project] ', resource_tags_user_project)
                WHEN NULLIF(resource_tags_user_project_name, '') IS NOT NULL
                    THEN CONCAT('[project_name] ', resource_tags_user_project_name)
                WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
                    THEN CONCAT('[name] ', resource_tags_user_name)
                WHEN NULLIF(resource_tags_user_n_a_m_e, '') IS NOT NULL
                    THEN CONCAT('[n_a_m_e] ', resource_tags_user_n_a_m_e)
                WHEN NULLIF(resource_tags_user_environment, '') IS NOT NULL
                    THEN CONCAT('[environment] ', resource_tags_user_environment)
                WHEN line_item_line_item_type = 'Usage'
                    THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
                ELSE CONCAT(product_product_name, ' - 기타')
            END
        HAVING SUM(line_item_unblended_cost) > 0.1
        ORDER BY service, cost DESC
    """
    rows   = _run_query(athena, sql)
    result = {}
    for r in rows:
        svc     = r.get('service', '')
        creator = r.get('creator', '')
        cost    = float(r.get('cost', 0) or 0)
        if creator:  # SQL에서 이미 분류된 creator 사용
            result.setdefault(svc, {})
            result[svc][creator] = result[svc].get(creator, 0.0) + cost
    return result


def fetch_daily_by_service_and_region_cur(athena, d1_date: date) -> dict:
    """
    Q4 해당 (product_region_code 사용, queries.sql Q4 오기 수정).
    D-1 서비스 + 리전별 비용.

    Returns:
        {service: {region: float}}
    """
    year, month = _partition(d1_date)
    sql = f"""
        SELECT
            product_product_name                                                AS service,
            COALESCE(NULLIF(product_region_code, ''), 'global')                AS region,
            SUM(line_item_unblended_cost)                                       AS cost
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
        GROUP BY
            product_product_name,
            COALESCE(NULLIF(product_region_code, ''), 'global')
        HAVING SUM(line_item_unblended_cost) > 0.01
        ORDER BY cost DESC
    """
    rows   = _run_query(athena, sql)
    result = {}
    for r in rows:
        svc    = r.get('service', '')
        region = r.get('region') or 'global'
        cost   = float(r.get('cost', 0) or 0)
        result.setdefault(svc, {})
        result[svc][region] = result[svc].get(region, 0.0) + cost
    return result


def fetch_mtd_by_service_and_creator_cur(athena, d1_date: date) -> dict:
    """
    Q5 해당.
    MTD 서비스 + 태그 기반 creator 분류 (세분화, Tax 제외).
    당월 1일 실행 시(범위 없음) {} 반환.

    Creator 분류는 fetch_daily_by_service_and_creator_cur()와 동일.

    Returns:
        {service: {creator: float}}
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start >= d1_date:
        return {}
    year, month = _partition(d1_date)
    sql = f"""
        SELECT
            product_product_name                                                AS service,
            CASE
                WHEN line_item_line_item_type = 'Tax'
                    THEN CONCAT('[Tax] ', product_product_name)
                WHEN product_product_name = 'AWS Data Transfer'
                    THEN '[공통] Data Transfer'
                WHEN product_product_name = 'AWS Cost Explorer'
                    THEN '[공통] Cost Explorer'
                WHEN product_product_name = 'AWS Support [Business]'
                    THEN '[공통] Support'
                WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
                    THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
                WHEN NULLIF(resource_tags_user_username, '') IS NOT NULL
                    THEN CONCAT('[username] ', resource_tags_user_username)
                WHEN NULLIF(resource_tags_user_requester, '') IS NOT NULL
                    THEN CONCAT('[requester] ', resource_tags_user_requester)
                WHEN NULLIF(resource_tags_user_project, '') IS NOT NULL
                    THEN CONCAT('[project] ', resource_tags_user_project)
                WHEN NULLIF(resource_tags_user_project_name, '') IS NOT NULL
                    THEN CONCAT('[project_name] ', resource_tags_user_project_name)
                WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
                    THEN CONCAT('[name] ', resource_tags_user_name)
                WHEN NULLIF(resource_tags_user_n_a_m_e, '') IS NOT NULL
                    THEN CONCAT('[n_a_m_e] ', resource_tags_user_n_a_m_e)
                WHEN NULLIF(resource_tags_user_environment, '') IS NOT NULL
                    THEN CONCAT('[environment] ', resource_tags_user_environment)
                WHEN line_item_line_item_type = 'Usage'
                    THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
                ELSE CONCAT(product_product_name, ' - 기타')
            END                                                                 AS creator,
            SUM(line_item_unblended_cost)                                       AS cost
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
          AND line_item_line_item_type != 'Tax'
        GROUP BY
            product_product_name,
            CASE
                WHEN line_item_line_item_type = 'Tax'
                    THEN CONCAT('[Tax] ', product_product_name)
                WHEN product_product_name = 'AWS Data Transfer'
                    THEN '[공통] Data Transfer'
                WHEN product_product_name = 'AWS Cost Explorer'
                    THEN '[공통] Cost Explorer'
                WHEN product_product_name = 'AWS Support [Business]'
                    THEN '[공통] Support'
                WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
                    THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
                WHEN NULLIF(resource_tags_user_username, '') IS NOT NULL
                    THEN CONCAT('[username] ', resource_tags_user_username)
                WHEN NULLIF(resource_tags_user_requester, '') IS NOT NULL
                    THEN CONCAT('[requester] ', resource_tags_user_requester)
                WHEN NULLIF(resource_tags_user_project, '') IS NOT NULL
                    THEN CONCAT('[project] ', resource_tags_user_project)
                WHEN NULLIF(resource_tags_user_project_name, '') IS NOT NULL
                    THEN CONCAT('[project_name] ', resource_tags_user_project_name)
                WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
                    THEN CONCAT('[name] ', resource_tags_user_name)
                WHEN NULLIF(resource_tags_user_n_a_m_e, '') IS NOT NULL
                    THEN CONCAT('[n_a_m_e] ', resource_tags_user_n_a_m_e)
                WHEN NULLIF(resource_tags_user_environment, '') IS NOT NULL
                    THEN CONCAT('[environment] ', resource_tags_user_environment)
                WHEN line_item_line_item_type = 'Usage'
                    THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
                ELSE CONCAT(product_product_name, ' - 기타')
            END
        HAVING SUM(line_item_unblended_cost) > 0.1
        ORDER BY service, cost DESC
    """
    rows   = _run_query(athena, sql)
    result = {}
    for r in rows:
        svc     = r.get('service', '')
        creator = r.get('creator', '')
        cost    = float(r.get('cost', 0) or 0)
        if creator:  # SQL에서 이미 분류된 creator 사용
            result.setdefault(svc, {})
            result[svc][creator] = result[svc].get(creator, 0.0) + cost
    return result


def fetch_mtd_by_service_and_region_cur(athena, d1_date: date) -> dict:
    """
    Q6 해당.
    MTD 서비스 + 리전별 비용.
    당월 1일 실행 시 {} 반환.

    Returns:
        {service: {region: float}}
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start >= d1_date:
        return {}
    year, month = _partition(d1_date)
    sql = f"""
        SELECT
            product_product_name                                                AS service,
            COALESCE(NULLIF(product_region_code, ''), 'global')                AS region,
            SUM(line_item_unblended_cost)                                       AS cost
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
        GROUP BY
            product_product_name,
            COALESCE(NULLIF(product_region_code, ''), 'global')
        HAVING SUM(line_item_unblended_cost) > 0.01
        ORDER BY cost DESC
    """
    rows   = _run_query(athena, sql)
    result = {}
    for r in rows:
        svc    = r.get('service', '')
        region = r.get('region') or 'global'
        cost   = float(r.get('cost', 0) or 0)
        result.setdefault(svc, {})
        result[svc][region] = result[svc].get(region, 0.0) + cost
    return result


def fetch_mtd_total_cur(athena, d1_date: date) -> float:
    """
    Q7 해당.
    MTD 총 비용 단일 합계.
    당월 1일 실행 시 0.0 반환.
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start >= d1_date:
        return 0.0
    year, month = _partition(d1_date)
    sql = f"""
        SELECT SUM(line_item_unblended_cost) AS mtd_total
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
    """
    rows = _run_query(athena, sql)
    if rows and rows[0].get('mtd_total'):
        return float(rows[0]['mtd_total'])
    return 0.0


# ---------------------------------------------------------------------------
# 일괄 수집
# ---------------------------------------------------------------------------

def collect_all(today_kst: date) -> dict:
    """
    Athena CUR 기반 데이터 일괄 수집.
    반환 구조는 data.py collect_all() 과 동일 → report.py 공유 사용 가능.

    CE 데이터 지연 보정:
        리포트 기준일(d1_date) = today_kst - 2일
        forecast 만 today_kst 기준 CE API 사용 (Q8)

    Returns:
        {
            'd1_date':        date,   # 리포트 대상일 (today - 2)
            'daily_d1':       dict,   # {service: float}
            'daily_d2':       dict,   # {service: float}
            'by_creator':     dict,   # {service: {creator: float}}
            'by_creator_mtd': dict,   # {service: {creator: float}}
            'by_region':      dict,   # {service: {region: float}}
            'by_region_mtd':  dict,   # {service: {region: float}}
            'mtd_this':       float,
            'forecast':       float,  # CE API (0.0 = 예측 불가)
        }
    """
    athena = boto3.client('athena', region_name='ap-northeast-2')
    ce     = boto3.client('ce',     region_name='us-east-1')

    d1_date = today_kst #- timedelta(days=1)
    d2_date = d1_date   - timedelta(days=1)

    return {
        'd1_date':        d1_date,
        'daily_d1':       fetch_daily_by_service_cur(athena, d1_date),
        'daily_d2':       fetch_daily_by_service_cur(athena, d2_date),
        'by_creator':     fetch_daily_by_service_and_creator_cur(athena, d1_date),
        'by_creator_mtd': fetch_mtd_by_service_and_creator_cur(athena, d1_date),
        'by_region':      fetch_daily_by_service_and_region_cur(athena, d1_date),
        'by_region_mtd':  fetch_mtd_by_service_and_region_cur(athena, d1_date),
        'mtd_this':       fetch_mtd_total_cur(athena, d1_date),
        'forecast':       fetch_cost_forecast(ce),
    }
