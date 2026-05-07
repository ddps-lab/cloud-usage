"""
monitor_v2/cost/analysis.py

CUR Athena 기반 비용 분석 + Amazon Nova Micro LLM 요약.

LLM 입력 데이터 (AI 요약용):
    Q14  fetch_top_services_with_breakdown
         어제 절대 비용 Top N 서비스 + IAM × usage_type 분해
         → ■ 어제 비용 상위 (현황) 섹션 데이터
    Q15  fetch_month_new_costs
         이번 달 들어 처음 발생한 큰 비용 항목
         → ▲ 이번 달 신규 발생 섹션 데이터
    MTD  fetch_mtd_total_cur + fetch_cost_forecast
         이번 달 누계 + 월말 예상 → 월간 맥락 단락

Slack 테이블 raw 데이터 (LLM 입력 X, 표만 노출):
    Q9   fetch_service_diff       서비스별 어제 vs 그제 변화
    Q10  fetch_usage_type_diff    usage_type별 변화
    Q11  fetch_resource_diff      리소스 ID별 변화

환경변수:
    BEDROCK_MODEL_ID         기본: amazon.nova-micro-v1:0
    BEDROCK_REGION           기본: us-east-1
    NEW_COST_THRESHOLD       기본: 10        (이번 달 신규 판단 — 어제 ≥ $X)
"""

import os
import json
import re
from collections import defaultdict
from datetime import date, timedelta

import boto3
import logging

from .data_cur import (
    _run_query, _partition,
    _ATHENA_DATABASE, _ATHENA_REGION,
    fetch_mtd_total_cur,
)
from .data import fetch_cost_forecast

log = logging.getLogger(__name__)

_BEDROCK_MODEL_ID    = os.environ.get('BEDROCK_MODEL_ID')
_BEDROCK_REGION      = os.environ.get('BEDROCK_REGION')
_TOP_N               = 10
_NEW_COST_THRESHOLD  = float(os.environ.get('NEW_COST_THRESHOLD', '10'))
_NEW_COST_PRIOR_CUT  = 1.0   # 이번 달 1일~그제 누적 $1 미만이면 "사실상 안 쓴" 것으로 간주
_TOP_SERVICES_N      = 5     # ■ 현황 노출 서비스 수
_TOP_BREAKDOWN_N     = 5     # 한 서비스 내 IAM × usage_type drill-down 수


# ---------------------------------------------------------------------------
# 공유 헬퍼
# ---------------------------------------------------------------------------

def _parse_iam_user(raw: str) -> str:
    # "IAMUser:AIDA3OGATNBRMEBUIOEWO:mhsong" → "mhsong"
    if not raw:
        return ''
    parts = raw.split(':')
    return parts[2] if len(parts) >= 3 else raw


# ---------------------------------------------------------------------------
# Athena 쿼리 (Q9, Q10, Q11)
# ---------------------------------------------------------------------------

def fetch_service_diff(athena, d1_date: date, d2_date: date) -> list:
    """
    Q9: 서비스별 d1 vs d2 비용 차이.
    변동 절대값이 큰 순으로 TOP_N 반환.

    Returns:
        [{'service': str, 'cost_d1': float, 'cost_d2': float, 'diff': float}, ...]
    """
    year_d1, month_d1 = _partition(d1_date)
    year_d2, month_d2 = _partition(d2_date)
    months = f"'{month_d1}'" if month_d1 == month_d2 else f"'{month_d1}', '{month_d2}'"

    sql = f"""
        SELECT
            product_product_name AS service,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS diff
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year_d1}'
          AND month IN ({months})
          AND DATE(line_item_usage_start_date) IN (DATE('{d1_date}'), DATE('{d2_date}'))
        GROUP BY product_product_name
        HAVING ABS(
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
        ) > 0.01
        ORDER BY ABS(diff) DESC
        LIMIT {_TOP_N}
    """
    rows = _run_query(athena, sql)
    return [
        {
            'service': r['service'],
            'cost_d1': float(r.get('cost_d1') or 0),
            'cost_d2': float(r.get('cost_d2') or 0),
            'diff':    float(r.get('diff') or 0),
        }
        for r in rows if r.get('service')
    ]


def fetch_usage_type_diff(athena, d1_date: date, d2_date: date) -> list:
    """
    Q10: 리소스 타입별 d1 vs d2 비용 차이.
    예: BoxUsage:t3.medium, USW2-EBS:VolumeUsage.gp3

    Returns:
        [{'service': str, 'usage_type': str, 'cost_d1': float, 'cost_d2': float, 'diff': float}, ...]
    """
    year_d1, month_d1 = _partition(d1_date)
    year_d2, month_d2 = _partition(d2_date)
    months = f"'{month_d1}'" if month_d1 == month_d2 else f"'{month_d1}', '{month_d2}'"

    sql = f"""
        SELECT
            product_product_name AS service,
            line_item_usage_type AS usage_type,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS diff
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year_d1}'
          AND month IN ({months})
          AND DATE(line_item_usage_start_date) IN (DATE('{d1_date}'), DATE('{d2_date}'))
        GROUP BY product_product_name, line_item_usage_type
        HAVING ABS(
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
        ) > 0.01
        ORDER BY ABS(diff) DESC
        LIMIT {_TOP_N}
    """
    rows = _run_query(athena, sql)
    return [
        {
            'service':    r['service'],
            'usage_type': r.get('usage_type', ''),
            'cost_d1':    float(r.get('cost_d1') or 0),
            'cost_d2':    float(r.get('cost_d2') or 0),
            'diff':       float(r.get('diff') or 0),
        }
        for r in rows if r.get('service')
    ]


def fetch_resource_diff(athena, d1_date: date, d2_date: date) -> list:
    """
    Q11: 리소스 ID별 d1 vs d2 비용 차이 + aws:createdBy 태그(IAM User) 포함.
    예: i-005217980755bcf43 → mhsong이 만든 EC2 인스턴스

    Returns:
        [{'service': str, 'usage_type': str, 'resource_id': str,
          'iam_user': str, 'cost_d1': float, 'cost_d2': float, 'diff': float}, ...]
    """
    year_d1, month_d1 = _partition(d1_date)
    year_d2, month_d2 = _partition(d2_date)
    months = f"'{month_d1}'" if month_d1 == month_d2 else f"'{month_d1}', '{month_d2}'"

    sql = f"""
        SELECT
            product_product_name AS service,
            line_item_usage_type AS usage_type,
            line_item_resource_id AS resource_id,
            COALESCE(resource_tags_aws_created_by, '') AS iam_user,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END) AS diff
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year_d1}'
          AND month IN ({months})
          AND DATE(line_item_usage_start_date) IN (DATE('{d1_date}'), DATE('{d2_date}'))
          AND line_item_resource_id IS NOT NULL
          AND line_item_resource_id != ''
        GROUP BY product_product_name, line_item_usage_type, line_item_resource_id,
                 COALESCE(resource_tags_aws_created_by, '')
        HAVING ABS(
            SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d1_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
          - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('{d2_date}')
                     THEN line_item_unblended_cost ELSE 0 END)
        ) > 0.01
        ORDER BY ABS(diff) DESC
        LIMIT {_TOP_N}
    """

    rows = _run_query(athena, sql)
    return [
        {
            'service':     r['service'],
            'usage_type':  r.get('usage_type', ''),
            'resource_id': r.get('resource_id', ''),
            'iam_user':    _parse_iam_user(r.get('iam_user', '')),
            'cost_d1':     float(r.get('cost_d1') or 0),
            'cost_d2':     float(r.get('cost_d2') or 0),
            'diff':        float(r.get('diff') or 0),
        }
        for r in rows if r.get('service')
    ]


# ---------------------------------------------------------------------------
# Q14: 어제 절대 비용 Top N + 분해 / Q15: 이번 달 신규 발생
# ---------------------------------------------------------------------------

def fetch_top_services_with_breakdown(
    athena, d1_date: date, top_n: int = 5, breakdown_top: int = 5,
) -> list:
    """
    Q14: 어제(d1) 절대 비용 기준 Top N 서비스 + 각 서비스 내부 (IAM User × usage_type) 분해.

    "변화량은 적지만 지속적으로 비용이 큰 서비스"의 사용자/타입 분포를 LLM이 인지할 수 있도록
    Q9~Q11(변화량 축)과 별개로 절대값 축에서 데이터를 한 번 더 뽑는다.

    Returns:
        [
            {
                'service':    str,
                'cost_d1':    float,
                'rank':       int,
                'breakdowns': [
                    {
                        'iam_user':    str,
                        'usage_type':  str,
                        'usage_human': str,
                        'cost_d1':     float,
                        'count':       int,  # distinct resource_id 개수
                    },
                    ...
                ]
            },
            ...
        ]
    """
    year_d1, month_d1 = _partition(d1_date)

    sql = f"""
        WITH base AS (
            SELECT
                product_product_name                        AS service,
                line_item_usage_type                        AS usage_type,
                COALESCE(resource_tags_aws_created_by, '')  AS iam_user,
                line_item_resource_id                       AS resource_id,
                line_item_unblended_cost                    AS cost
            FROM {_ATHENA_DATABASE}.cur_logs
            WHERE year  = '{year_d1}'
              AND month = '{month_d1}'
              AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
              AND line_item_line_item_type != 'Tax'
        ),
        svc_total AS (
            SELECT service, SUM(cost) AS total
            FROM base
            GROUP BY service
            HAVING SUM(cost) > 0.01
            ORDER BY total DESC
            LIMIT {top_n}
        )
        SELECT
            b.service       AS service,
            b.usage_type    AS usage_type,
            b.iam_user      AS iam_user,
            COUNT(DISTINCT b.resource_id) AS resource_count,
            SUM(b.cost)     AS cost_d1,
            t.total         AS service_total
        FROM base b
        JOIN svc_total t ON b.service = t.service
        GROUP BY b.service, b.usage_type, b.iam_user, t.total
        HAVING SUM(b.cost) > 0.01
        ORDER BY t.total DESC, cost_d1 DESC
    """
    rows = _run_query(athena, sql)

    from collections import defaultdict
    svc_total_map = {}
    raw_acc = defaultdict(list)
    for r in rows:
        svc = r.get('service')
        if not svc:
            continue
        svc_total_map[svc] = float(r.get('service_total') or 0)
        raw_acc[svc].append({
            'usage_type':  r.get('usage_type', '') or '',
            'iam_user':    _parse_iam_user(r.get('iam_user', '')),
            'cost_d1':     float(r.get('cost_d1') or 0),
            'count':       int(r.get('resource_count') or 0),
        })

    result = []
    for svc, items in raw_acc.items():
        agg = defaultdict(lambda: {'cost_d1': 0.0, 'count': 0})
        for item in items:
            usage_human = _humanize_usage_type(item['usage_type'])
            key = (item['iam_user'], usage_human, item['usage_type'])
            agg[key]['cost_d1'] += item['cost_d1']
            agg[key]['count']   += item['count']

        merged = [
            {
                'iam_user':    iu,
                'usage_human': uh,
                'usage_type':  ut,
                'cost_d1':     v['cost_d1'],
                'count':       v['count'],
            }
            for (iu, uh, ut), v in agg.items()
        ]
        merged.sort(key=lambda x: x['cost_d1'], reverse=True)
        result.append({
            'service':    svc,
            'cost_d1':    svc_total_map[svc],
            'breakdowns': merged[:breakdown_top],
        })

    result.sort(key=lambda x: x['cost_d1'], reverse=True)
    for idx, item in enumerate(result, 1):
        item['rank'] = idx
    return result


def fetch_month_new_costs(athena, d1_date: date) -> list:
    """
    Q15: 이번 달 들어 처음 발생한 큰 비용 항목.

    판단 단위: (service, IAM User, usage_type)
    조건:
        - 이번 달 1일 ~ 그제 누적 < _NEW_COST_PRIOR_CUT (기본: $1)
        - 어제 비용 ≥ _NEW_COST_THRESHOLD (기본: $10, 환경변수)
    정렬: 어제 비용 내림차순

    Returns:
        [
            {
                'service':     str,
                'iam_user':    str,
                'usage_type':  str,
                'usage_human': str,
                'cost_d1':     float,
                'prior_cost':  float,   # 이번 달 1일~그제 누적
                'count':       int,     # distinct resource_id 개수
            },
            ...
        ]
    """
    mtd_start = d1_date.replace(day=1)
    d2_date   = d1_date - timedelta(days=1)
    year_d1, month_d1 = _partition(d1_date)

    # mtd_start 부터 d1까지의 파티션 month 집합 (이번 달이므로 단일 month)
    months = f"'{month_d1}'"

    # 그제(d2)가 전월에 속하면 (= 이번 달 1일이 d1) Q15 의미 없음
    if mtd_start > d2_date:
        return []

    sql = f"""
        WITH month_to_yesterday AS (
            SELECT
                product_product_name                        AS service,
                line_item_usage_type                        AS usage_type,
                COALESCE(resource_tags_aws_created_by, '')  AS iam_user,
                line_item_resource_id                       AS resource_id,
                DATE(line_item_usage_start_date)            AS dt,
                line_item_unblended_cost                    AS cost
            FROM {_ATHENA_DATABASE}.cur_logs
            WHERE year  = '{year_d1}'
              AND month IN ({months})
              AND DATE(line_item_usage_start_date) BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
              AND line_item_line_item_type != 'Tax'
        )
        SELECT
            service,
            usage_type,
            iam_user,
            COUNT(DISTINCT resource_id)                                    AS resource_count,
            SUM(CASE WHEN dt = DATE('{d1_date}') THEN cost ELSE 0 END)     AS cost_d1,
            SUM(CASE WHEN dt <  DATE('{d1_date}') THEN cost ELSE 0 END)    AS prior_cost
        FROM month_to_yesterday
        GROUP BY service, usage_type, iam_user
        HAVING SUM(CASE WHEN dt <  DATE('{d1_date}') THEN cost ELSE 0 END) < {_NEW_COST_PRIOR_CUT}
           AND SUM(CASE WHEN dt = DATE('{d1_date}') THEN cost ELSE 0 END) >= {_NEW_COST_THRESHOLD}
        ORDER BY cost_d1 DESC
        LIMIT {_TOP_N}
    """
    rows = _run_query(athena, sql)
    result = []
    for r in rows:
        if not r.get('service'):
            continue
        result.append({
            'service':     r['service'],
            'usage_type':  r.get('usage_type', '') or '',
            'iam_user':    _parse_iam_user(r.get('iam_user', '')),
            'usage_human': _humanize_usage_type(r.get('usage_type', '') or ''),
            'cost_d1':     float(r.get('cost_d1') or 0),
            'prior_cost':  float(r.get('prior_cost') or 0),
            'count':       int(r.get('resource_count') or 0),
        })
    return result


def fetch_service_mtd_breakdown(athena, d1_date: date) -> dict:
    """
    Q16: 이번 달 1일 ~ d1 까지 서비스별 누계 비용.
    "지속적으로 큰 서비스" 판단을 위한 MTD 페이스 데이터.

    Returns:
        {service_name: mtd_total_float, ...}
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start > d1_date:
        return {}

    year_d1, month_d1 = _partition(d1_date)

    sql = f"""
        SELECT
            product_product_name           AS service,
            SUM(line_item_unblended_cost)  AS mtd_total
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year_d1}'
          AND month = '{month_d1}'
          AND DATE(line_item_usage_start_date) BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
          AND line_item_line_item_type != 'Tax'
        GROUP BY product_product_name
        HAVING SUM(line_item_unblended_cost) > 0.01
    """
    rows = _run_query(athena, sql)
    return {
        r['service']: float(r.get('mtd_total') or 0)
        for r in rows if r.get('service')
    }


# ---------------------------------------------------------------------------
# 프롬프트 구성
# ---------------------------------------------------------------------------

def _fmt_sign(v: float) -> str:
    return f"+${v:,.2f}" if v >= 0 else f"-${abs(v):,.2f}"


_REGION_CODES = {
    'APN1': '도쿄(ap-northeast-1)',
    'APN2': '서울(ap-northeast-2)',
    'APN3': '오사카(ap-northeast-3)',
    'APS1': '싱가포르(ap-southeast-1)',
    'APS2': '시드니(ap-southeast-2)',
    'USE1': '미국 동부(us-east-1)',
    'USE2': '미국 동부(us-east-2)',
    'USW1': '미국 서부(us-west-1)',
    'USW2': '미국 서부(us-west-2)',
    'EUW1': '유럽 서부(eu-west-1)',
    'EUC1': '유럽 중부(eu-central-1)',
    'SAE1': '상파울루(sa-east-1)',
}


def _humanize_usage_type(raw: str) -> str:
    """
    AWS CUR line_item_usage_type 코드를 한국어 자연어로 변환.
    LLM이 raw 코드를 해석하지 않아도 되도록 Python에서 미리 처리.

    예)
      USW2-SpotUsage:c8gd.48xlarge  → 미국 서부(us-west-2) c8gd.48xlarge Spot 인스턴스
      APN2-USW2-AWS-Out-Bytes        → 미국 서부(us-west-2)에서 서울(ap-northeast-2)로 나가는 데이터 전송
      USE1-Bedrock-ModelUnit         → 미국 동부(us-east-1) Bedrock 모델 호출
    """
    if not raw:
        return ''

    parts = raw.split('-')
    regions, remaining = [], []
    for p in parts:
        if p in _REGION_CODES and len(regions) < 2:
            regions.append(p)
        else:
            remaining.append(p)
    rest = '-'.join(remaining)
    region_prefix = _REGION_CODES.get(regions[0], '') if regions else ''

    # 리전간 데이터 전송 (리전 2개 + Out/In/Bytes)
    if len(regions) == 2 and ('Bytes' in rest or 'Out' in rest or 'In' in rest):
        src = _REGION_CODES.get(regions[0], regions[0])
        dst = _REGION_CODES.get(regions[1], regions[1])
        return f"{src}에서 {dst}로 나가는 데이터 전송"

    if 'SpotUsage:' in rest:
        itype = rest.split('SpotUsage:')[-1]
        return f"{region_prefix + ' ' if region_prefix else ''}{itype} Spot 인스턴스"

    if 'BoxUsage:' in rest:
        itype = rest.split('BoxUsage:')[-1]
        return f"{region_prefix + ' ' if region_prefix else ''}{itype} 온디맨드 인스턴스"

    if 'VolumeUsage' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}EBS 볼륨"

    if 'LoadBalancerUsage' in rest or 'LCUUsage' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}로드 밸런서"

    if 'VpcEndpoint' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}VPC 엔드포인트 사용 시간"

    if 'PublicIPv4:InUseAddress' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}사용 중 Public IPv4 주소"

    if 'PublicIPv4:IdleAddress' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}유휴 Public IPv4 주소"

    if 'NatGateway-Hours' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}NAT Gateway 사용 시간"

    if 'NatGateway-Bytes' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}NAT Gateway 데이터 처리"

    if 'SnapshotUsage' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}EBS 스냅샷"

    if 'TimedStorage' in rest or 'StorageObjectCount' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}S3 스토리지"

    if 'Requests-Tier1' in rest or 'Requests-Tier2' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}S3 요청"

    if 'Bedrock' in rest and 'ModelUnit' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}Bedrock 모델 호출"

    if 'Lambda' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}Lambda 함수 실행"

    if 'CloudWatch' in rest or 'Metrics' in rest or 'Logs' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}CloudWatch 모니터링"

    if 'CostExplorer' in rest or 'Cost-Explorer' in rest:
        return 'Cost Explorer API 조회'

    if 'Route53' in rest or 'DNS-Queries' in rest:
        return 'Route 53 DNS 쿼리'

    if 'Bytes' in rest or 'DataTransfer' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}데이터 전송"

    return raw  # 해석 불가 시 원본 반환


# usage_type 이 인스턴스/볼륨/스냅샷처럼 "개수" 단위로 셀 수 있는지 판별
_COUNTABLE_USAGE_PATTERNS = (
    'BoxUsage:', 'SpotUsage:', 'DedicatedUsage:', 'VolumeUsage',
    'SnapshotUsage', 'PublicIPv4', 'NatGateway-Hours',
)


def _is_countable(usage_type: str) -> bool:
    if not usage_type:
        return False
    return any(p in usage_type for p in _COUNTABLE_USAGE_PATTERNS)


# IAM User 매핑이 본질적으로 불가능한(공통 인프라/요청 기반) 항목
# → "(생성자 미상)" 라벨을 붙이지 않고 IAM 라인 자체를 생략
_IAM_AGNOSTIC_PATTERNS = (
    'DataTransfer', 'Bytes', 'Requests-Tier', 'CloudWatch', 'Metrics', 'Logs',
    'CostExplorer', 'Cost-Explorer', 'DNS-Queries', 'Route53',
)


def _is_iam_agnostic(usage_type: str) -> bool:
    if not usage_type:
        return False
    return any(p in usage_type for p in _IAM_AGNOSTIC_PATTERNS)


_SYSTEM_PROMPT = """\
당신은 AWS 비용 변화를 사내 슬랙으로 보고하는 분석가입니다.
이곳은 연구소이며 연구과제에 따라 일별 비용 변동이 큽니다.
표가 이미 함께 노출되므로, 당신의 역할은 "표가 못 하는 통찰" 만 한국어로 풀어 쓰는 것입니다.

=== 출력 구조 — 정확히 3문단 ===

1문단: 결론 한 줄 + 월간 흐름 (1~3문장)
2문단: 어제 비용의 핵심 driver 분석 (2~4문장)
3문단: 특이사항 — 이번 달 신규 발생 / 관찰된 신호 (0~3문장, 없으면 생략 가능)

각 문단은 빈 줄로 구분.

=== 1문단 작성 ===

- 첫 문장: "어제(<날짜>) AWS 비용은 $X.XX였습니다." 같은 결론
- 이어서: 입력의 "=== 월간 맥락 ===" 사실만 사용해
  "이번 달 N일 동안 $X.XX를 사용했으며, 이 추세가 이어지면 월말 약 $Y가 예상됩니다." 형식으로 1~2문장
- 일평균 수치는 자연스러울 때만 인용 (반복 금지)

=== 2문단 작성 — 가장 중요 ===

목적: 어제 비용이 "어디로 갔는지" + "어떤 driver가 있는지" + **"평소 페이스 대비 어떤지"** 통찰형 서술.

다룰 범위 (반드시 준수):
- 비중 순으로 어제 비용 ≥ $5 인 서비스는 **모두** 다룬다 (보통 2~4개)
- 한 서비스 내에서는 비용 ≥ $5 인 (IAM × 타입) 조합을 **모두** 언급
- $5 미만은 "그 외 작은 항목 합쳐 약 $X" 식으로 묶어서 한 문구만
- 입력 "=== 관찰된 신호 ===" 의 "[<서비스> 페이스]" 항목이 있으면
  반드시 한 서비스에 대해 "평소 페이스 대비 어떤지" 1문장으로 녹여 서술
  (예: "EC2는 이번 달 일평균 $250 수준으로 발생 중이며, 어제 $181은 평소 안의 흐름입니다.")

인과 연결어 적극 사용:
"주된 원인은", "핵심은", "대부분이", "상당 부분이", "비중이 가장 큰 ~",
"주도하고 있는 것은", "그 외", "함께", "합쳐"

묶음 패턴:
- 같은 인스턴스 타입이 여러 사용자에 분산되어 있으면 묶어서 서술
  (예: "m5.8xlarge 20대 (mhsong 10대 + 태그 없음 10대)")
- "관찰된 신호" 입력에 [동일 사용자] 같은 신호가 있으면 자연스럽게 녹여 서술
  단 "한 프로젝트", "ML 워크로드" 같은 단정 표현은 금지

서술 형식 강제 (표 형식 절대 금지):
- "EC2  $181.37  ▸ 78%" 같은 헤더 라인 출력 금지
- "    mhsong: m5.8xlarge ×10개  $43" 들여쓰기 라인 금지
- 서비스별로 헤더 + 들여쓰기 항목 구조 금지
- 모든 수치는 "**문장 안에**" 자연스럽게 녹여 서술
- 좋은 예: "EC2가 $181로 어제의 78%를 차지하며, 미국 서부 m5.8xlarge 20대(mhsong 10대 + 태그 없음 10대)가 핵심이고, kernel-fusion-benchmark의 inf2.xlarge 11대($19)와 yjjung의 g4dn.xlarge 2대($5)도 함께 비중을 차지합니다."
- 나쁜 예 (금지): "EC2  $181.37  ▸ 78%" 헤더 형식

=== 3문단 작성 (선택) ===

다룰 거리:
- "=== 이번 달 신규 발생 ===" 입력에 항목이 있으면 → "특이사항으로 X가 이번 달 처음 등장했습니다" 식
- "=== 관찰된 신호 ===" 입력의 [태그 미커버] 신호가 있으면 → "태그 없는 리소스가 어제의 N%를 차지해 책임 추적 점검이 필요해 보입니다"
- 둘 다 없으면 3문단 자체를 생략

=== 단정 금지 — 반드시 준수 ===

입력 데이터에 명시되지 않은 것을 단정하는 표현은 모두 금지합니다.
관찰은 가능하나, 의미 부여·원인 단정은 약한 표현으로만 가능합니다.

금지(단정):                       허용(약한 표현):
"~입니다 (원인 단정)"             "~로 보입니다", "~가능성이 있습니다"
"한 프로젝트의 비용 구조입니다"     "동일 사용자에 집중되어 있습니다"
"ML 워크로드입니다"                (입력에 없으면 언급 자체 금지)
"이는 ~ 때문입니다"                "확인이 필요해 보입니다"
"~를 의미합니다"                   "~로 추정됩니다 (사실에 가까울 때만)"

=== 절대 금지 ===

- 통계 표현: σ, μ, 평균, 정상 범위, 이상치, 표준편차
- 시기 비교: 지난 달, 전월 동일일, 작년, 분기
- 일별 비교: "전일 대비 X% 증가/감소", "그제 대비"
- 마크다운(# ## ### ** *) 사용
- 입력에 없는 수치·이름·리소스 ID(i-xxx, vol-xxx, arn:...) 추가
- $1 미만 항목 언급
- "감소", "어제 사용 없음", "중단" 표현
- 항목별 라인 출력, 들여쓰기 나열, 헤더-디테일 형식
- "한 프로젝트", "ML 워크로드", "학습 작업", "추론 작업" 등 입력에 없는 추측 단어

=== 표현 가이드 ===

- "(생성자 미상)" 라벨이 입력에 보이면 → "**태그 없는** {리소스명}" 으로 변환
  (예: "태그 없는 m5.8xlarge 10대"). "(생성자 미상)" 단어 출력 금지.
- 데이터 전송·CloudWatch 등 IAM agnostic 항목은 IAM 이름 빼고 서술
  (예: "미국 서부 데이터 전송")
- 인스턴스/볼륨/스냅샷 등 개수가 의미 있는 항목만 "N대"·"N개" 표기
- 같은 서비스에 IAM User 여러 명이면 큰 순으로 1~2명만 언급
- 단, 묶음 패턴(같은 인스턴스 타입이 분산)이 입력 신호로 들어왔다면 묶어서 표현

=== 출력 예시 (이 텍스트 자체는 출력 금지) ===

[입력]
어제(2026-05-06) AWS 비용  $233.36
=== 월간 맥락 ===
이번 달 6일 동안 $3,654.47 사용. 이대로 진행 시 월말 예상 약 $18,199.64.
이번 달 일평균: $609.08 / 월말 총 예상: $18,199.64
=== 이번 달 신규 발생 ===
EKS  $12.40  mhsong: 미국 서부(us-west-2) EKS 클러스터
=== 어제 비용 상위 (현황 raw) ===
EC2  $181.37  ▸ 어제의 78%
    태그 없는 m5.8xlarge 온디맨드 인스턴스 ×10개  $97.89
    mhsong: 미국 서부(us-west-2) m5.8xlarge 온디맨드 인스턴스 ×10개  $43.19
    kernel-fusion-benchmark: 미국 서부(us-west-2) inf2.xlarge 온디맨드 인스턴스 ×11개  $18.81
    태그 없는 미국 서부(us-west-2) EBS 볼륨 ×54개  $6.53
    yjjung: 미국 서부(us-west-2) g4dn.xlarge 온디맨드 인스턴스 ×2개  $5.36
S3  $46.87  ▸ 어제의 20%
    mhsong: 미국 서부(us-west-2) 데이터 전송  $41.49
    swjeong: 미국 서부(us-west-2) S3 스토리지  $3.10
=== 관찰된 신호 ===
- [EC2 페이스] 이번 달 누계 $1,940.00 / 일평균 $323.33 / 어제 $181.37 (일평균의 56%)
- [S3 페이스] 이번 달 누계 $312.00 / 일평균 $52.00 / 어제 $46.87 (일평균의 90%)
- [EC2] 미국 서부(us-west-2) m5.8xlarge 온디맨드 인스턴스 총 20대 ($141.08) — 태그 없음 10대, mhsong 10대
- [태그 미커버] 어제 비용 중 $104.42 (45%)가 태그 없는 리소스에서 발생 — 책임 추적이 어려운 상태
- [동일 사용자] mhsong이 EC2, S3, EKS에 걸쳐 $97.08 발생

[모범 출력]
어제(2026-05-06) AWS 비용은 $233.36였습니다. 이번 달 6일 동안 $3,654를 사용했으며, 이 추세가 이어지면 월말 약 $18,200이 예상됩니다.

어제 비용은 EC2가 78%($181)로 압도적이며, 미국 서부의 m5.8xlarge 20대(mhsong 10대 + 태그 없음 10대)가 핵심 driver입니다. 그 외 kernel-fusion-benchmark의 inf2.xlarge 11대($19), yjjung의 g4dn.xlarge 2대($5), 태그 없는 EBS 볼륨 54개($7)가 함께 비중을 차지합니다. EC2는 이번 달 일평균 $323 수준으로 발생 중이며 어제 $181은 평소보다 다소 낮은 흐름입니다. S3 비용 $47의 대부분은 mhsong의 미국 서부 데이터 전송($41)에서 발생했고 swjeong의 스토리지 $3이 일부를 차지합니다.

특이사항으로 EKS가 이번 달 처음 등장($12)해 mhsong이 새 클러스터를 가동했을 가능성이 있어 보입니다. 또한 태그 없는 리소스가 어제 비용의 45%를 차지하고 있어 태그 정책 점검이 필요해 보입니다.
"""


# 서비스명 단축 — Python에서 미리 처리해 LLM에 전달
_SVC_SHORT = {
    'Amazon Elastic Compute Cloud':                    'EC2',
    'Amazon Elastic Compute Cloud - Compute':          'EC2',
    'EC2 - Other':                                     'EC2-Other',
    'Amazon Simple Storage Service':                   'S3',
    'AWS Lambda':                                      'Lambda',
    'Elastic Load Balancing':                          'ELB',
    'Amazon Virtual Private Cloud':                    'VPC',
    'AWS Cost Explorer':                               'Cost Explorer',
    'AmazonCloudWatch':                                'CloudWatch',
    'Amazon CloudFront':                               'CloudFront',
    'Amazon Bedrock':                                  'Bedrock',
    'Amazon Elastic Container Service for Kubernetes': 'EKS',
    'Amazon Elastic Container Service':                'ECS',
    'Amazon Relational Database Service':              'RDS',
    'Amazon DynamoDB':                                 'DynamoDB',
    'Amazon Route 53':                                 'Route 53',
    'Amazon Simple Notification Service':              'SNS',
    'Amazon Simple Queue Service':                     'SQS',
    'Amazon SageMaker':                                'SageMaker',
    'Amazon API Gateway':                              'API Gateway',
    'AWS Key Management Service':                      'KMS',
    'AWS Secrets Manager':                             'Secrets Manager',
}


def _format_breakdown_line(d: dict) -> str:
    """
    Q14 / Q15 의 한 (IAM User × usage_type) 행을 LLM 입력 라인으로 포맷.

    "(생성자 미상)" 라벨 회피:
      - IAM agnostic usage_type (데이터 전송, CloudWatch 등) → IAM 정보 없이 usage_human만
      - 그 외 IAM 비어 있음 → "태그 없는 {usage_human}" (LLM이 그대로 사용)
      - IAM 있음 → "{iam}: {usage_human}"

    카운트:
      - countable usage_type (BoxUsage, SpotUsage, VolumeUsage, ...) 만 ×N개 표기
    """
    iam         = d.get('iam_user', '') or ''
    usage_type  = d.get('usage_type', '') or ''
    usage_human = d.get('usage_human', '') or usage_type
    count       = int(d.get('count', 1) or 1)

    count_str = f" ×{count}개" if (count > 1 and _is_countable(usage_type)) else ''

    if not iam:
        if _is_iam_agnostic(usage_type):
            return f"{usage_human}{count_str}"
        return f"태그 없는 {usage_human}{count_str}"
    return f"{iam}: {usage_human}{count_str}"


def _fmt_top_services(rows: list, d1_total: float) -> str:
    """
    Q14 결과 → LLM 입력 텍스트.
    각 서비스 헤더에 어제 총비용 대비 비중(%) 포함.
    """
    if not rows:
        return '(없음)'
    blocks = []
    for svc in rows:
        short = _SVC_SHORT.get(svc['service'], svc['service'])
        share = (svc['cost_d1'] / d1_total * 100) if d1_total > 0 else 0
        header = f"{short}  ${svc['cost_d1']:,.2f}  ▸ 어제의 {share:.0f}%"
        lines = []
        for d in svc.get('breakdowns', []):
            if d.get('cost_d1', 0) < 1.0:
                continue
            line = _format_breakdown_line(d)
            lines.append(f"    {line}  ${d['cost_d1']:,.2f}")
        if lines:
            blocks.append(header + '\n' + '\n'.join(lines))
        else:
            blocks.append(header)
    return '\n'.join(blocks)


def _fmt_new_costs(rows: list) -> str:
    """
    Q15 결과 → LLM 입력 텍스트.
    이번 달 들어 처음 발생한 (service, IAM, usage_type) 항목.
    """
    if not rows:
        return '(없음)'
    lines = []
    for r in rows:
        short = _SVC_SHORT.get(r['service'], r['service'])
        line  = _format_breakdown_line(r)
        lines.append(f"{short}  ${r['cost_d1']:,.2f}  {line}")
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# 그룹 묶음 / 패턴 신호 계산
# ---------------------------------------------------------------------------
#
# LLM 입력에 미리 계산해 넣을 신호들 — LLM이 직접 추론하지 않도록 사실 기반으로 제공.
# 모든 신호는 "단정"이 아니라 "관찰된 사실"로만 표현됨.

def _detect_signals(
    top_services: list,
    d1_total: float,
    service_mtd: dict = None,
    mtd_days_elapsed: int = 0,
) -> list:
    """
    Q14 + Q16 데이터에서 LLM이 단독 추론하기 어려운 패턴/위험/페이스 신호를
    사실 기반으로 추출. 단정 표현 없음.

    반환되는 신호 종류:
        [페이스] 서비스별 이번 달 누계 / 일평균 / 어제 비중 — "지속" 판단 근거
        [묶음]   같은 (서비스, usage_human) 안에서 IAM 분포
        [태그 미커버] 어제 비용 중 IAM 매핑 없는 항목 비율
        [동일 사용자]  같은 IAM이 여러 서비스에 등장
    """
    signals = []
    if not top_services or d1_total <= 0:
        return signals

    # 0) 서비스별 페이스 — Q14 어제 Top + Q16 MTD 누계로 "지속/돌발" 판단 근거
    if service_mtd and mtd_days_elapsed >= 2:
        for svc in top_services:
            mtd = service_mtd.get(svc['service'], 0.0)
            if mtd <= 0.01:
                continue
            short     = _SVC_SHORT.get(svc['service'], svc['service'])
            daily_avg = mtd / mtd_days_elapsed
            ratio_pct = (svc['cost_d1'] / daily_avg * 100) if daily_avg > 0 else 0
            signals.append(
                f"[{short} 페이스] 이번 달 누계 ${mtd:,.2f} / 일평균 ${daily_avg:,.2f} / "
                f"어제 ${svc['cost_d1']:,.2f} (일평균의 {ratio_pct:.0f}%)"
            )

    # 1) 인스턴스 타입 묶음 — 같은 (service, usage_human) 안에 여러 행이 있는지
    type_groups = defaultdict(list)
    for svc in top_services:
        for d in svc.get('breakdowns', []):
            if d.get('cost_d1', 0) < 1.0:
                continue
            key = (svc['service'], d.get('usage_human', ''))
            type_groups[key].append(d)

    for (service, usage_human), entries in type_groups.items():
        if len(entries) < 2:
            continue
        if not _is_countable(entries[0].get('usage_type', '')):
            continue
        total_cost  = sum(e['cost_d1'] for e in entries)
        total_count = sum(e.get('count', 0) for e in entries)
        if total_count < 2:
            continue
        short = _SVC_SHORT.get(service, service)
        parts = []
        for e in sorted(entries, key=lambda x: x['cost_d1'], reverse=True):
            iam = e.get('iam_user') or '태그 없음'
            parts.append(f"{iam} {e.get('count', 0)}대")
        parts_str = ', '.join(parts)
        signals.append(
            f"[{short}] {usage_human} 총 {total_count}대 (${total_cost:,.2f}) — {parts_str}"
        )

    # 2) 태그 미커버 비율
    untagged_cost = 0.0
    for svc in top_services:
        for d in svc.get('breakdowns', []):
            usage_type = d.get('usage_type', '')
            if d.get('iam_user'):
                continue
            if _is_iam_agnostic(usage_type):
                continue  # 데이터 전송 등은 본질적으로 IAM 매핑 불가 → 위험 신호 아님
            untagged_cost += d.get('cost_d1', 0)
    if untagged_cost > 0 and d1_total > 0:
        share = untagged_cost / d1_total * 100
        if share >= 5.0:  # 5% 이상일 때만 신호로 (작으면 노이즈)
            signals.append(
                f"[태그 미커버] 어제 비용 중 ${untagged_cost:,.2f} ({share:.0f}%)가 "
                f"태그 없는 리소스에서 발생 — 책임 추적이 어려운 상태"
            )

    # 3) 멀티 서비스 IAM — 같은 IAM이 2개 이상 서비스에 비용 ≥ $1로 등장
    iam_services = defaultdict(lambda: {'services': set(), 'cost': 0.0})
    for svc in top_services:
        for d in svc.get('breakdowns', []):
            iam = d.get('iam_user', '')
            if not iam:
                continue
            if d.get('cost_d1', 0) < 1.0:
                continue
            iam_services[iam]['services'].add(_SVC_SHORT.get(svc['service'], svc['service']))
            iam_services[iam]['cost'] += d.get('cost_d1', 0)
    for iam, info in iam_services.items():
        if len(info['services']) >= 2:
            svc_list = ', '.join(sorted(info['services']))
            signals.append(
                f"[동일 사용자] {iam}이 {svc_list}에 걸쳐 ${info['cost']:,.2f} 발생"
            )

    return signals


def _fmt_signals(signals: list) -> str:
    if not signals:
        return '(특이 사항 없음)'
    return '\n'.join(f"- {s}" for s in signals)


def _calc_pace_context(
    d1_total: float, mtd_total: float, mtd_days_elapsed: int, forecast_total: float,
) -> str:
    """
    어제 비용이 이번 달 페이스 대비 어떤 위치인지 사실로만 표현.
    "이상치"·"평균보다 X% 큼" 같은 단정 표현 금지 — LLM이 자연어로 가공.
    """
    if mtd_days_elapsed < 1 or mtd_total <= 0:
        return '(이번 달 페이스 데이터 없음)'

    daily_avg = mtd_total / mtd_days_elapsed
    parts = [f"이번 달 일평균: ${daily_avg:,.2f}"]
    if forecast_total > 0:
        parts.append(f"월말 총 예상: ${forecast_total:,.2f}")
    return ' / '.join(parts)


# ---------------------------------------------------------------------------
# LLM 입력 메시지 구성
# ---------------------------------------------------------------------------

def _build_user_message(
    d1_date: date,
    d1_total: float,
    top_services: list,
    new_costs: list,
    mtd_total: float,
    mtd_days_elapsed: int,
    forecast_total: float,
    service_mtd: dict = None,
) -> str:
    """
    LLM 입력 메시지.

    LLM에 raw 데이터 + 미리 계산된 그룹/패턴/페이스 신호를 같이 전달한다.
    LLM은 이 데이터를 바탕으로 "3문단 통찰형 요약"을 생성.
    """
    top_text     = _fmt_top_services(top_services, d1_total)
    new_text     = _fmt_new_costs(new_costs)
    signals      = _detect_signals(
        top_services, d1_total,
        service_mtd=service_mtd, mtd_days_elapsed=mtd_days_elapsed,
    )
    signals_text = _fmt_signals(signals)
    pace_text    = _calc_pace_context(d1_total, mtd_total, mtd_days_elapsed, forecast_total)

    if mtd_days_elapsed >= 1 and mtd_total > 0:
        mtd_line = f"이번 달 {mtd_days_elapsed}일 동안 ${mtd_total:,.2f} 사용."
    else:
        mtd_line = "이번 달 누계 데이터 없음."

    forecast_line = (
        f" 이대로 진행 시 월말 예상 약 ${forecast_total:,.2f}."
        if forecast_total > 0 else ""
    )
    monthly_block = mtd_line + forecast_line

    return f"""어제({d1_date}) AWS 비용  ${d1_total:,.2f}

=== 월간 맥락 ===
{monthly_block}
{pace_text}

=== 이번 달 신규 발생 ===
{new_text}

=== 어제 비용 상위 (현황 raw) ===
{top_text}

=== 관찰된 신호 (사실 기반, 단정 아님) ===
{signals_text}

위 입력만을 사용해 시스템 지시에 따라 3문단 한국어 보고를 작성하세요."""


# ---------------------------------------------------------------------------
# Bedrock Nova Micro 호출
# ---------------------------------------------------------------------------

def summarize(
    d1_date: date,
    d1_total: float,
    top_services: list,
    new_costs: list,
    mtd_total: float,
    mtd_days_elapsed: int,
    forecast_total: float,
    service_mtd: dict = None,
) -> str:
    """
    Nova Micro에 비용 요약 요청.
    실패 시 폴백 텍스트 반환 (Lambda 전체 실패 방지).
    """
    user_message = _build_user_message(
        d1_date=d1_date,
        d1_total=d1_total,
        top_services=top_services,
        new_costs=new_costs,
        mtd_total=mtd_total,
        mtd_days_elapsed=mtd_days_elapsed,
        forecast_total=forecast_total,
        service_mtd=service_mtd,
    )
    try:
        bedrock = boto3.client('bedrock-runtime', region_name=_BEDROCK_REGION)
        body = json.dumps({
            'system':   [{'text': _SYSTEM_PROMPT}],
            'messages': [{'role': 'user', 'content': [{'text': user_message}]}],
            'inferenceConfig': {
                'max_new_tokens': 800,
                'temperature': 0,
            },
        })
        resp   = bedrock.invoke_model(
            modelId=_BEDROCK_MODEL_ID,
            body=body,
            contentType='application/json',
            accept='application/json',
        )
        result = json.loads(resp['body'].read())
        text   = result['output']['message']['content'][0]['text'].strip()
        text   = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
        return text.strip()

    except Exception as e:
        log.error("Bedrock 호출 실패: %s", e)
        return f"LLM 분석 실패 (Bedrock 오류). 어제 총비용 ${d1_total:,.2f}."


# ---------------------------------------------------------------------------
# 일괄 수집 진입점
# ---------------------------------------------------------------------------

def collect_all(d1_date: date) -> dict:
    """
    Athena 쿼리 + CE Forecast + Nova Micro 요약 일괄 수집.

    LLM 입력으로 사용:
        Q14  fetch_top_services_with_breakdown — 어제 절대값 Top + IAM 분해
        Q15  fetch_month_new_costs              — 이번 달 신규 발생
        MTD  fetch_mtd_total_cur                — 이번 달 누계
        FCST fetch_cost_forecast (CE)           — 월말 예상

    Slack 테이블 raw 데이터로만 사용 (LLM 미입력):
        Q9, Q10, Q11

    Returns:
        {
            'd1_date':           date,
            'd2_date':           date,
            'd1_total':          float,
            'd2_total':          float,
            'service_rows':      list,   # Q9
            'usage_type_rows':   list,   # Q10
            'resource_rows':     list,   # Q11
            'top_services':      list,   # Q14
            'new_costs':         list,   # Q15
            'mtd_total':         float,
            'mtd_days_elapsed':  int,
            'forecast_total':    float,
            'summary':           str,
        }
    """
    d2_date = d1_date - timedelta(days=1)
    athena  = boto3.client('athena', region_name=_ATHENA_REGION)
    ce      = boto3.client('ce', region_name='us-east-1')

    service_rows    = fetch_service_diff(athena, d1_date, d2_date)
    usage_type_rows = fetch_usage_type_diff(athena, d1_date, d2_date)
    resource_rows   = fetch_resource_diff(athena, d1_date, d2_date)
    top_services    = fetch_top_services_with_breakdown(
        athena, d1_date,
        top_n=_TOP_SERVICES_N, breakdown_top=_TOP_BREAKDOWN_N,
    )
    new_costs       = fetch_month_new_costs(athena, d1_date)
    service_mtd     = fetch_service_mtd_breakdown(athena, d1_date)  # Q16

    d1_total = sum(r['cost_d1'] for r in service_rows)
    d2_total = sum(r['cost_d2'] for r in service_rows)

    mtd_total        = fetch_mtd_total_cur(athena, d1_date)
    mtd_days_elapsed = d1_date.day

    # CE Forecast = "오늘부터 월말까지" 예상. mtd_total + forecast = 월말 총 예상
    try:
        forecast = fetch_cost_forecast(ce)
    except Exception as exc:
        log.warning("CE forecast 실패 (무시): %s", exc)
        forecast = 0.0
    forecast_total = mtd_total + forecast if forecast > 0 else 0.0

    summary = summarize(
        d1_date=d1_date,
        d1_total=d1_total,
        top_services=top_services,
        new_costs=new_costs,
        mtd_total=mtd_total,
        mtd_days_elapsed=mtd_days_elapsed,
        forecast_total=forecast_total,
        service_mtd=service_mtd,
    )

    return {
        'd1_date':          d1_date,
        'd2_date':          d2_date,
        'd1_total':         d1_total,
        'd2_total':         d2_total,
        'service_rows':     service_rows,
        'usage_type_rows':  usage_type_rows,
        'resource_rows':    resource_rows,
        'top_services':     top_services,
        'new_costs':        new_costs,
        'service_mtd':      service_mtd,
        'mtd_total':        mtd_total,
        'mtd_days_elapsed': mtd_days_elapsed,
        'forecast_total':   forecast_total,
        'summary':          summary,
    }