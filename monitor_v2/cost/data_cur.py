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
_ATHENA_REGION          = os.environ.get('ATHENA_REGION', 'ap-northeast-2')

_POLL_INTERVAL = 1.5   # seconds
_MAX_WAIT      = 120   # seconds


# ---------------------------------------------------------------------------
# 공용 creator fallback CASE (Q3/Q5/Q11/Q14/Q15/Q17 공유)
#
# 우선순위:
#   0. 공통 서비스 예외   → '[공통] Data Transfer / Cost Explorer / Support'
#   1. aws:createdBy      → IAM User name
#   2. lambda:createdBy   → '[Lambda] <value>'
#   3. Requester/Username → '[Requester] / [User] <value>'
#   4. Project*           → SPLIT_PART(value, '-', 1)  # 첫 토큰만
#   5. EKS                → '[EKS] <cluster>[/<nodegroup>]'
#   6. Elastic Beanstalk  → '[EB] <value>'
#   7. AWS 자동 관리       → '[MGN] / [SageMaker] managed-resource'
#   8. Service / Group    → '[Service] / [Group] <value>'
#   9. Env / STAGE / Deploy → '[Env] / [Deploy] <value>'
#  10. Name / NAME        → SPLIT_PART(value, '-', 1)  # 첫 토큰만
#  11. usage_type fallback (Usage 라인만)
#  12. 기타
#
# CUR 은 그 계정 리소스가 한 번이라도 그 태그 키를 사용했을 때만 해당
# resource_tags_* 컬럼을 생성한다. 따라서 계정별 cur_logs 스키마가 다름.
# information_schema.columns 로 사용 가능한 컬럼을 콜드스타트 시 1회 조회한 뒤,
# 존재하는 컬럼만 골라서 동적으로 CASE WHEN 을 조립한다.
# ---------------------------------------------------------------------------

# 정적 prefix/suffix — 모든 계정에서 항상 사용 가능한 컬럼만 참조
_CREATOR_CASE_PREFIX = """    CASE
        WHEN product_product_name = 'AWS Data Transfer'      THEN '[공통] Data Transfer'
        WHEN product_product_name = 'AWS Cost Explorer'      THEN '[공통] Cost Explorer'
        WHEN product_product_name = 'AWS Support [Business]' THEN '[공통] Support'

"""

_CREATOR_CASE_SUFFIX = """
        WHEN line_item_line_item_type = 'Usage'
            THEN CONCAT(product_product_name, ' - ', line_item_usage_type)

        ELSE CONCAT(product_product_name, ' - 기타')
    END"""

# EKS 는 cluster 필수 + nodegroup 옵셔널 구조라 별도 처리. 마커로 위치만 보존.
_EKS_RULE = '__EKS__'

# 순서대로 평가되는 동적 룰: (필수 컬럼, WHEN-THEN SQL).
# 컬럼이 cur_logs 에 존재할 때만 해당 절을 포함한다. 기존 우선순위 유지.
_CREATOR_RULES_ORDERED: list[tuple[str, str]] = [
    ("resource_tags_aws_created_by",
     "WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL\n"
     "            THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)"),
    ("resource_tags_user_lambda_created_by",
     "WHEN NULLIF(resource_tags_user_lambda_created_by, '') IS NOT NULL\n"
     "            THEN CONCAT('[Lambda] ', resource_tags_user_lambda_created_by)"),
    ("resource_tags_user_requester",
     "WHEN NULLIF(resource_tags_user_requester, '') IS NOT NULL\n"
     "            THEN CONCAT('[Requester] ', resource_tags_user_requester)"),
    ("resource_tags_user_username",
     "WHEN NULLIF(resource_tags_user_username, '') IS NOT NULL\n"
     "            THEN CONCAT('[User] ', resource_tags_user_username)"),
    ("resource_tags_user_project",
     "WHEN NULLIF(resource_tags_user_project, '') IS NOT NULL\n"
     "            THEN SPLIT_PART(resource_tags_user_project, '-', 1)"),
    ("resource_tags_user_project_name",
     "WHEN NULLIF(resource_tags_user_project_name, '') IS NOT NULL\n"
     "            THEN SPLIT_PART(resource_tags_user_project_name, '-', 1)"),
    (_EKS_RULE, ""),
    ("resource_tags_user_elasticbeanstalk_environment_name",
     "WHEN NULLIF(resource_tags_user_elasticbeanstalk_environment_name, '') IS NOT NULL\n"
     "            THEN CONCAT('[EB] ', resource_tags_user_elasticbeanstalk_environment_name)"),
    ("resource_tags_user_elasticbeanstalk_environment_id",
     "WHEN NULLIF(resource_tags_user_elasticbeanstalk_environment_id, '') IS NOT NULL\n"
     "            THEN CONCAT('[EB] ', resource_tags_user_elasticbeanstalk_environment_id)"),
    ("resource_tags_user_a_w_s_application_migration_service_managed",
     "WHEN NULLIF(resource_tags_user_a_w_s_application_migration_service_managed, '') IS NOT NULL\n"
     "            THEN '[MGN] ApplicationMigrationService'"),
    ("resource_tags_user_managed_by_amazon_sage_maker_resource",
     "WHEN NULLIF(resource_tags_user_managed_by_amazon_sage_maker_resource, '') IS NOT NULL\n"
     "            THEN '[SageMaker] managed-resource'"),
    ("resource_tags_user_service",
     "WHEN NULLIF(resource_tags_user_service, '') IS NOT NULL\n"
     "            THEN CONCAT('[Service] ', resource_tags_user_service)"),
    ("resource_tags_user_group",
     "WHEN NULLIF(resource_tags_user_group, '') IS NOT NULL\n"
     "            THEN CONCAT('[Group] ', resource_tags_user_group)"),
    ("resource_tags_user_environment",
     "WHEN NULLIF(resource_tags_user_environment, '') IS NOT NULL\n"
     "            THEN CONCAT('[Env] ', resource_tags_user_environment)"),
    ("resource_tags_user_s_t_a_g_e",
     "WHEN NULLIF(resource_tags_user_s_t_a_g_e, '') IS NOT NULL\n"
     "            THEN CONCAT('[Env] ', resource_tags_user_s_t_a_g_e)"),
    ("resource_tags_user_deploy",
     "WHEN NULLIF(resource_tags_user_deploy, '') IS NOT NULL\n"
     "            THEN CONCAT('[Deploy] ', resource_tags_user_deploy)"),
    ("resource_tags_user_name",
     "WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL\n"
     "            THEN SPLIT_PART(resource_tags_user_name, '-', 1)"),
    ("resource_tags_user_n_a_m_e",
     "WHEN NULLIF(resource_tags_user_n_a_m_e, '') IS NOT NULL\n"
     "            THEN SPLIT_PART(resource_tags_user_n_a_m_e, '-', 1)"),
]

# 모듈 전역 캐시 (콜드스타트 시 1회 채워짐)
_AVAILABLE_TAG_COLUMNS: set[str] | None = None


def _load_available_tag_columns(athena) -> set[str]:
    """cur_logs 의 resource_tags_* 컬럼 set 을 반환 (모듈 전역 캐시)."""
    global _AVAILABLE_TAG_COLUMNS
    if _AVAILABLE_TAG_COLUMNS is not None:
        return _AVAILABLE_TAG_COLUMNS
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{_ATHENA_DATABASE}'
          AND table_name   = 'cur_logs'
          AND column_name LIKE 'resource_tags_%'
    """
    rows = _run_query(athena, sql)
    _AVAILABLE_TAG_COLUMNS = {r['column_name'] for r in rows if r.get('column_name')}
    log.info("CUR resource_tags 컬럼 %d개 감지", len(_AVAILABLE_TAG_COLUMNS))
    return _AVAILABLE_TAG_COLUMNS


def _eks_clause(available: set[str]) -> str | None:
    """EKS 룰 — cluster 필수, nodegroup 옵셔널."""
    cluster   = "resource_tags_user_eks_cluster_name"
    nodegroup = "resource_tags_user_eks_nodegroup_name"
    if cluster not in available:
        return None
    if nodegroup in available:
        return (
            f"WHEN NULLIF({cluster}, '') IS NOT NULL\n"
            f"            THEN CONCAT(\n"
            f"                '[EKS] ',\n"
            f"                {cluster},\n"
            f"                CASE WHEN NULLIF({nodegroup}, '') IS NOT NULL\n"
            f"                     THEN CONCAT('/', {nodegroup})\n"
            f"                     ELSE '' END\n"
            f"            )"
        )
    return (
        f"WHEN NULLIF({cluster}, '') IS NOT NULL\n"
        f"            THEN CONCAT('[EKS] ', {cluster})"
    )


def _build_creator_case_sql(athena) -> str:
    """계정에 존재하는 resource_tags_* 컬럼만 사용하는 CASE WHEN SQL 을 조립."""
    available = _load_available_tag_columns(athena)
    middle: list[str] = []
    for col, when_then in _CREATOR_RULES_ORDERED:
        if col == _EKS_RULE:
            clause = _eks_clause(available)
            if clause:
                middle.append(clause)
        elif col in available:
            middle.append(when_then)
    middle_sql = "".join("        " + c + "\n" for c in middle)
    return _CREATOR_CASE_PREFIX + middle_sql + _CREATOR_CASE_SUFFIX


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
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{target_date}')
          AND line_item_line_item_type NOT IN ('Credit', 'Refund', 'Tax')
        GROUP BY product_product_name
        HAVING SUM(line_item_unblended_cost) > 0
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
    D-1 서비스 + 다단계 태그 fallback 으로 산출한 creator 별 비용.

    Fallback 우선순위 (위에서 아래):
        0. 공통 서비스 예외       → '[공통] Data Transfer / Cost Explorer / Support'
        1. aws:createdBy          → IAM User name
        2. lambda:createdBy       → '[Lambda] <value>'
        3. Requester / Username   → '[Requester] / [User] <value>'
        4. Project / ProjectName  → SPLIT_PART(value, '-', 1)  # 첫 토큰만
        5. eks:cluster-name(+nodegroup) → '[EKS] <cluster>/<nodegroup>'
        6. elasticbeanstalk:env-name/id → '[EB] <value>'
        7. AWS 자동 관리 (MGN, SageMaker) → '[MGN] / [SageMaker] managed-resource'
        8. Service / Group        → '[Service] / [Group] <value>'
        9. Environment / STAGE / Deploy → '[Env] / [Deploy] <value>'
        10. Name / NAME           → SPLIT_PART(value, '-', 1)  # 첫 토큰만
        11. usage_type            → '<service> - <usage_type>'
        12. 그 외                 → '<service> - 기타'

    NOTE: IAM User별 비용에 Tax 포함 계산 (Usage × 1.10)

    Returns:
        {service: {creator: float}}
    """
    year, month = _partition(d1_date)
    creator_case_sql = _build_creator_case_sql(athena)
    sql = f"""
        SELECT
            product_product_name AS service,
            {creator_case_sql} AS creator,
            SUM(line_item_unblended_cost) AS cost
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
          AND line_item_line_item_type NOT IN ('Credit', 'Refund', 'Tax')
        GROUP BY 1, 2
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
            # IAM User별 비용에 Tax 포함 (Usage × 1.10)
            cost_with_tax = cost * 1.10
            result.setdefault(svc, {})
            result[svc][creator] = result[svc].get(creator, 0.0) + cost_with_tax
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
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
          AND line_item_line_item_type NOT IN ('Credit', 'Refund', 'Tax')
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

    NOTE: IAM User별 비용에 Tax 포함 계산 (Usage × 1.10)

    Creator 분류는 fetch_daily_by_service_and_creator_cur()와 동일.

    Returns:
        {service: {creator: float}}
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start >= d1_date:
        return {}
    year, month = _partition(d1_date)
    creator_case_sql = _build_creator_case_sql(athena)
    sql = f"""
        SELECT
            product_product_name AS service,
            {creator_case_sql} AS creator,
            SUM(line_item_unblended_cost) AS cost
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
          AND line_item_line_item_type NOT IN ('Credit', 'Refund', 'Tax')
        GROUP BY 1, 2
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
            # IAM User별 비용에 Tax 포함 (Usage × 1.10)
            cost_with_tax = cost * 1.10
            result.setdefault(svc, {})
            result[svc][creator] = result[svc].get(creator, 0.0) + cost_with_tax
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
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
          AND line_item_line_item_type NOT IN ('Credit', 'Refund', 'Tax')
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
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
          AND line_item_line_item_type NOT IN ('Credit', 'Refund', 'Tax')
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
    athena = boto3.client('athena', region_name=_ATHENA_REGION)
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
