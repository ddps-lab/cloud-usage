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
from .owner_resolver import get_owner_maps
from .cur_schema import add_columns_ddl, find_missing_tag_columns

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
#   1. Owner / Engineer   → 값 그대로 (명시적 소유자 선언)
#   2. aws:createdBy      → IAM User name  ※ IAMUser/Root 일 때만 (아래 주의 참고)
#   3. 클러스터/ASG 매핑   → 생성 IAM User  (owner_resolver, CloudTrail 기반)
#   4. lambda:createdBy   → '[Lambda] <value>'
#   5. Requester/Username → '[Requester] / [User] <value>'
#   6. Project*           → SPLIT_PART(value, '-', 1)  # 첫 토큰만
#   7. EKS                → '[EKS] <cluster>[/<nodegroup>]'
#   8. Elastic Beanstalk  → '[EB] <value>'
#   9. AWS 자동 관리       → '[MGN] / [SageMaker] managed-resource'
#  10. Service / Group    → '[Service] / [Group] <value>'
#  11. Env / STAGE / Deploy → '[Env] / [Deploy] <value>'
#  12. Name / NAME        → SPLIT_PART(value, '-', 1)  # 첫 토큰만
#  13. usage_type fallback (Usage 라인만)
#  14. 기타
#
# ⚠ aws:createdBy 는 <principal-type>:<unique-id>:<friendly-name> 형식이고,
#   3번째 필드의 의미가 principal-type 에 따라 다르다.
#     IAMUser:AIDA...:mhsong          → 'mhsong'      (사람)
#     AssumedRole:AROA...:AutoScaling → 'AutoScaling' (역할 세션명 — 사람이 아님)
#   ASG/ELB 서비스 연결 역할이나 EKS 파드 세션이 사용자 이름 자리에 올라오는 것을
#   막기 위해 사람 판정은 principal-type = IAMUser/Root 로 한정한다. 걸러진 행은
#   태그 체인에서도 잡히지 않으면 12번 fallback('<service> - <usage_type>') 으로
#   내려간다 — 사람이 아닌 비용은 사용자 목록이 아니라 서비스명으로 표시된다.
#
# 3번(클러스터/ASG 매핑)은 연구자가 태그를 붙이는 협조 없이도 IAM User 단위
# 귀속이 되게 하기 위한 경로다. AWS 가 자동 부착하는 aws:eks:cluster-name /
# aws:autoscaling:groupName 을 CloudTrail 의 생성자 정보와 조인한다.
# 상세는 owner_resolver.py 참조. 매핑이 없으면 4번 이후 체인으로 내려간다.
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

# CloudTrail 기반 '클러스터/ASG → IAM User' 매핑도 런타임에 조립하므로 마커 사용.
_OWNER_MAP_RULE = '__OWNER_MAP__'

# 순서대로 평가되는 동적 룰: (필수 컬럼, WHEN-THEN SQL).
# 컬럼이 cur_logs 에 존재할 때만 해당 절을 포함한다. 기존 우선순위 유지.
_CREATOR_RULES_ORDERED: list[tuple[str, str]] = [
    # ── 사람 식별 (IAM User 단위 귀속) ───────────────────────────
    # Owner / owner / Engineer: 명시적 소유자 선언이라 aws:createdBy 보다 우선.
    # ※ CUR 컬럼명은 태그 키를 소문자화하므로 'Owner' 와 'owner' 가 같은
    #   resource_tags_user_owner 로 수렴할 수 있다. 컬럼이 나뉘어 생성되면
    #   그 컬럼명으로 룰을 하나 더 추가하면 된다 (없는 컬럼은 자동 skip).
    ("resource_tags_user_owner",
     "WHEN NULLIF(resource_tags_user_owner, '') IS NOT NULL\n"
     "            THEN resource_tags_user_owner"),
    ("resource_tags_user_engineer",
     "WHEN NULLIF(resource_tags_user_engineer, '') IS NOT NULL\n"
     "            THEN resource_tags_user_engineer"),
    # aws:createdBy 는 principal-type 이 사람일 때만 이름으로 사용한다.
    # AssumedRole 세션명(AutoScaling, default.*-pod-*, aws-go-sdk-* 등)은
    # 사람이 아니므로 여기서 걸러내고, 아래 태그 체인에서도 잡히지 않으면
    # 최종 fallback('<service> - <usage_type>') 으로 내려간다.
    ("resource_tags_aws_created_by",
     "WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL\n"
     "            AND SPLIT_PART(resource_tags_aws_created_by, ':', 1)\n"
     "                IN ('IAMUser', 'Root')\n"
     "            THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)"),

    # ── 자동 귀속: 클러스터 / ASG → 생성 IAM User ────────────────
    # AWS 가 사람 개입 없이 자동 부착하는 태그(aws:eks:cluster-name,
    # aws:autoscaling:groupName)를 CloudTrail 생성자 매핑과 조인한다.
    # 아래 태그 체인(Project/Name 등)보다 우선이라, 예컨대 pacsrun 클러스터
    # 비용은 'PACSrun'(Project 태그) 대신 실제 생성자 'jglee' 로 표시된다.
    (_OWNER_MAP_RULE, ""),

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
    available = {r['column_name'] for r in rows if r.get('column_name')}

    # 새로 활성화된 cost allocation tag 는 Parquet 에만 생기고 테이블에는 안 생긴다.
    # 그대로 두면 그 태그를 쓰는 룰이 조용히 빠지므로, manifest 와 비교해 추가한다.
    # 상세는 cur_schema.py 참조.
    missing = find_missing_tag_columns(
        _ATHENA_DATABASE, 'cur_logs', available, _ATHENA_REGION)
    if missing:
        try:
            _run_query(athena, add_columns_ddl(_ATHENA_DATABASE, 'cur_logs', missing))
            available |= set(missing)
            log.warning("cur_logs 에 없던 태그 컬럼 %d개 추가: %s", len(missing), missing)
        except Exception as e:                        # noqa: BLE001
            log.warning("cur_logs 태그 컬럼 추가 실패 → 이 컬럼을 쓰는 룰은 빠진다 %s: %s",
                        missing, e)

    _AVAILABLE_TAG_COLUMNS = available
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


def _sql_literal(text) -> str:
    """SQL 문자열 리터럴로 안전하게 인용 (single quote 이스케이프)."""
    return "'" + str(text).replace("'", "''") + "'"


def _mapping_clause(column: str, mapping: dict) -> str | None:
    """
    {태그값: IAM User} 매핑을 'IN 가드 + CASE' WHEN 절로 변환.

    예:
        WHEN resource_tags_aws_eks_cluster_name IN ('Theseus', 'pacsrun')
            THEN CASE resource_tags_aws_eks_cluster_name
                     WHEN 'Theseus' THEN 'swjeong'
                     WHEN 'pacsrun' THEN 'jglee'
                 END
    """
    items = [(k, v) for k, v in sorted(mapping.items()) if k and v]
    if not items:
        return None
    in_list = ", ".join(_sql_literal(k) for k, _ in items)
    whens = "\n".join(
        f"                     WHEN {_sql_literal(k)} THEN {_sql_literal(v)}"
        for k, v in items
    )
    return (
        f"WHEN {column} IN ({in_list})\n"
        f"            THEN CASE {column}\n"
        f"{whens}\n"
        f"                 END"
    )


def _owner_map_clauses(available: set[str]) -> list:
    """
    CloudTrail 로 만든 클러스터/ASG 소유자 매핑을 WHEN 절 리스트로 반환.

    매핑이 비어 있거나 해당 컬럼이 CUR 에 없으면 빈 리스트 → CASE 에서 생략.
    owner_resolver 는 실패 시 빈 매핑을 주므로 리포트를 막지 않는다.
    """
    clusters, asgs = get_owner_maps()
    clauses: list[str] = []
    for column, mapping in (
        ('resource_tags_aws_eks_cluster_name',       clusters),
        ('resource_tags_aws_autoscaling_group_name', asgs),
    ):
        if column not in available or not mapping:
            continue
        clause = _mapping_clause(column, mapping)
        if clause:
            clauses.append(clause)
    return clauses


def _build_creator_case_sql(athena) -> str:
    """계정에 존재하는 resource_tags_* 컬럼만 사용하는 CASE WHEN SQL 을 조립."""
    available = _load_available_tag_columns(athena)
    middle: list[str] = []
    for col, when_then in _CREATOR_RULES_ORDERED:
        if col == _EKS_RULE:
            clause = _eks_clause(available)
            if clause:
                middle.append(clause)
        elif col == _OWNER_MAP_RULE:
            middle.extend(_owner_map_clauses(available))
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
            if not page:                              # DDL 은 결과 행이 없다
                break
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
        1. Owner / Engineer       → 값 그대로 (명시적 소유자)
        2. aws:createdBy          → IAM User name
                                    ※ principal-type 이 IAMUser/Root 인 경우만.
        3. 클러스터 / ASG 매핑     → 생성 IAM User
                                    aws:eks:cluster-name / aws:autoscaling:groupName
                                    을 CloudTrail 생성자와 조인 (owner_resolver)
        4. lambda:createdBy       → '[Lambda] <value>'
        5. Requester / Username   → '[Requester] / [User] <value>'
        6. Project / ProjectName  → SPLIT_PART(value, '-', 1)  # 첫 토큰만
        7. eks:cluster-name(+nodegroup) → '[EKS] <cluster>/<nodegroup>'
        8. elasticbeanstalk:env-name/id → '[EB] <value>'
        9. AWS 자동 관리 (MGN, SageMaker) → '[MGN] / [SageMaker] managed-resource'
        10. Service / Group       → '[Service] / [Group] <value>'
        11. Environment / STAGE / Deploy → '[Env] / [Deploy] <value>'
        12. Name / NAME           → SPLIT_PART(value, '-', 1)  # 첫 토큰만
        13. usage_type            → '<service> - <usage_type>'
        14. 그 외                 → '<service> - 기타'

        ※ AssumedRole 세션(AutoScaling / ElasticLoadBalancing / default.*-pod-* /
          aws-go-sdk-* 등)은 2번에서 걸러진다. 3번 매핑에 걸리면 사람 이름이
          나오고, 걸리지 않으면 13번으로 내려가 서비스명으로 표시된다.

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
