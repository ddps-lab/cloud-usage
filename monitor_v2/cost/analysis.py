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
    CREATOR_CASE_SQL,
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
    # SQL CREATOR_CASE_SQL 가 이미 파싱해 넘기므로 대부분 그대로 반환.
    # 레거시 raw 포맷("IAMUser:AIDA...:user", "AssumedRole:...:role")만 추가 파싱.
    if not raw:
        return ''
    if raw.startswith('IAMUser:') or raw.startswith('AssumedRole:'):
        parts = raw.split(':')
        return parts[2] if len(parts) >= 3 else raw
    return raw


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
            {CREATOR_CASE_SQL} AS iam_user,
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
        GROUP BY 1, 2, 3, 4
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
                        'count':       int,    # distinct resource_id 개수
                        'usage_hours': float,  # 사용 시간 (BoxUsage/SpotUsage 등 시간 단위 항목만 의미)
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
                {CREATOR_CASE_SQL}                          AS iam_user,
                line_item_resource_id                       AS resource_id,
                line_item_unblended_cost                    AS cost,
                line_item_usage_amount                      AS usage_amount
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
            SUM(b.usage_amount) AS usage_amount_total,
            t.total         AS service_total
        FROM base b
        JOIN svc_total t ON b.service = t.service
        GROUP BY b.service, b.usage_type, b.iam_user, t.total
        HAVING SUM(b.cost) > 0.01
        ORDER BY t.total DESC, cost_d1 DESC
    """
    rows = _run_query(athena, sql)

    svc_total_map = {}
    raw_acc = defaultdict(list)
    for r in rows:
        svc = r.get('service')
        if not svc:
            continue
        svc_total_map[svc] = float(r.get('service_total') or 0)
        raw_acc[svc].append({
            'usage_type':   r.get('usage_type', '') or '',
            'iam_user':     _parse_iam_user(r.get('iam_user', '')),
            'cost_d1':      float(r.get('cost_d1') or 0),
            'count':        int(r.get('resource_count') or 0),
            'usage_amount': float(r.get('usage_amount_total') or 0),
        })

    result = []
    for svc, items in raw_acc.items():
        agg = defaultdict(lambda: {'cost_d1': 0.0, 'count': 0, 'usage_amount': 0.0})
        for item in items:
            usage_human = _humanize_usage_type(item['usage_type'])
            key = (item['iam_user'], usage_human, item['usage_type'])
            agg[key]['cost_d1']      += item['cost_d1']
            agg[key]['count']        += item['count']
            agg[key]['usage_amount'] += item['usage_amount']

        merged = []
        for (iu, uh, ut), v in agg.items():
            merged.append({
                'iam_user':    iu,
                'usage_human': uh,
                'usage_type':  ut,
                'cost_d1':     v['cost_d1'],
                'count':       v['count'],
                # 시간 단위가 의미 있는 항목(BoxUsage/SpotUsage/NatGateway-Hours 등)만 노출
                'usage_hours': v['usage_amount'] if _is_hourly(ut) else 0.0,
            })
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
                {CREATOR_CASE_SQL}                          AS iam_user,
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
# Q17: 이번 달 누계 Top 사용자 + 서비스/타입 분해
# ---------------------------------------------------------------------------

def fetch_mtd_top_users_with_breakdown(
    athena, d1_date: date, top_n: int = 5, breakdown_top: int = 5,
) -> list:
    """
    Q17: 이번 달 1일 ~ d1 까지 누계 비용 Top N 사용자 + 각 사용자가 쓴 서비스/usage_type 분해.

    Q14 (어제 절대값 + IAM 분해) 의 누계 버전, 단 축이 user-first.
    "이번 달 누구한테 비용이 가장 많이 쌓였는가" 를 LLM 이 인식하도록 함.

    creator 분류는 CREATOR_CASE_SQL fallback 체인 사용 (Q3/Q5/Q11/Q14/Q15 와 동일).

    Returns:
        [
            {
                'iam_user':   str,    # creator 라벨 (예: 'swjeong', '[Project] criu', '[EKS] ...')
                'mtd_total':  float,  # 이 사용자의 MTD 누계
                'rank':       int,
                'breakdowns': [
                    {
                        'service':     str,
                        'usage_type':  str,
                        'usage_human': str,   # humanized
                        'cost':        float,
                        'count':       int,   # distinct resource_id 개수
                    },
                    ...
                ]
            },
            ...
        ]
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start > d1_date:
        return []

    year_d1, month_d1 = _partition(d1_date)

    sql = f"""
        WITH base AS (
            SELECT
                product_product_name                        AS service,
                line_item_usage_type                        AS usage_type,
                {CREATOR_CASE_SQL}                          AS iam_user,
                line_item_resource_id                       AS resource_id,
                line_item_unblended_cost                    AS cost
            FROM {_ATHENA_DATABASE}.cur_logs
            WHERE year  = '{year_d1}'
              AND month = '{month_d1}'
              AND DATE(line_item_usage_start_date) BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
              AND line_item_line_item_type != 'Tax'
        ),
        user_total AS (
            SELECT iam_user, SUM(cost) AS total
            FROM base
            WHERE iam_user IS NOT NULL AND iam_user != ''
            GROUP BY iam_user
            HAVING SUM(cost) > 0.1
            ORDER BY total DESC
            LIMIT {top_n}
        )
        SELECT
            b.iam_user                       AS iam_user,
            b.service                        AS service,
            b.usage_type                     AS usage_type,
            COUNT(DISTINCT b.resource_id)    AS resource_count,
            SUM(b.cost)                      AS cost,
            ut.total                         AS user_total
        FROM base b
        JOIN user_total ut ON b.iam_user = ut.iam_user
        GROUP BY 1, 2, 3, ut.total
        HAVING SUM(b.cost) > 0.1
        ORDER BY ut.total DESC, cost DESC
    """

    rows = _run_query(athena, sql)

    user_total_map = {}
    raw_acc = defaultdict(list)
    for r in rows:
        user = r.get('iam_user')
        if not user:
            continue
        user_total_map[user] = float(r.get('user_total') or 0)
        raw_acc[user].append({
            'service':    r.get('service', '') or '',
            'usage_type': r.get('usage_type', '') or '',
            'count':      int(r.get('resource_count') or 0),
            'cost':       float(r.get('cost') or 0),
        })

    result = []
    for user, items in raw_acc.items():
        # (service, usage_human) 단위로 묶음 — humanize 후 같은 라벨로 합산
        agg = defaultdict(lambda: {'cost': 0.0, 'count': 0, 'usage_type': ''})
        for item in items:
            usage_human = _humanize_usage_type(item['usage_type'])
            key = (item['service'], usage_human)
            agg[key]['cost']       += item['cost']
            agg[key]['count']      += item['count']
            agg[key]['usage_type']  = item['usage_type']  # 대표 1건

        merged = []
        for (svc, uh), v in agg.items():
            merged.append({
                'service':     svc,
                'usage_type':  v['usage_type'],
                'usage_human': uh,
                'cost':        v['cost'],
                'count':       v['count'],
            })
        merged.sort(key=lambda x: x['cost'], reverse=True)

        result.append({
            'iam_user':   user,
            'mtd_total':  user_total_map[user],
            'breakdowns': merged[:breakdown_top],
        })

    result.sort(key=lambda x: x['mtd_total'], reverse=True)
    for idx, item in enumerate(result, 1):
        item['rank'] = idx
    return result


# ---------------------------------------------------------------------------
# 프롬프트 구성
# ---------------------------------------------------------------------------

def _fmt_sign(v: float) -> str:
    return f"+${v:,.2f}" if v >= 0 else f"-${abs(v):,.2f}"


_REGION_CODES = {
    'APN1': 'ap-northeast-1',
    'APN2': 'ap-northeast-2',
    'APN3': 'ap-northeast-3',
    'APS1': 'ap-southeast-1',
    'APS2': 'ap-southeast-2',
    'USE1': 'us-east-1',
    'USE2': 'us-east-2',
    'USW1': 'us-west-1',
    'USW2': 'us-west-2',
    'EUW1': 'eu-west-1',
    'EUC1': 'eu-central-1',
    'SAE1': 'sa-east-1',
}


def _humanize_usage_type(raw: str) -> str:
    """
    AWS CUR line_item_usage_type 코드를 한국어 자연어로 변환.
    LLM이 raw 코드를 해석하지 않아도 되도록 Python에서 미리 처리.

    예)
      USW2-SpotUsage:c8gd.48xlarge  → us-west-2 c8gd.48xlarge Spot 인스턴스
      APN2-USW2-AWS-Out-Bytes        → us-west-2에서 ap-northeast-2로 나가는 데이터 전송
      USE1-Bedrock-ModelUnit         → us-east-1 Bedrock 모델 호출
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

    if 'AmazonEKS-Hours' in rest or 'EKS-Hours' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}EKS 클러스터 운영 시간"

    if 'EKS-Pod' in rest or 'AmazonEKS-vCPU' in rest or 'AmazonEKS-Memory' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}EKS 컴퓨팅 사용"

    if 'ECS' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}ECS 사용"

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


# usage_amount 가 "시간(hours)" 단위로 의미 있는 항목만 식별
# (인스턴스 운영 시간, NAT Gateway 운영 시간 등)
# VolumeUsage(GB-Month), DataTransfer(GB) 등은 시간 단위 아니므로 제외
_HOURLY_USAGE_PATTERNS = (
    'BoxUsage:', 'SpotUsage:', 'DedicatedUsage:', 'HostBoxUsage:',
    'NatGateway-Hours', 'LoadBalancerUsage', 'LCUUsage',
    'AmazonEKS-Hours',
)


def _is_hourly(usage_type: str) -> bool:
    if not usage_type:
        return False
    return any(p in usage_type for p in _HOURLY_USAGE_PATTERNS)


_SYSTEM_PROMPT = """\
당신은 AWS 비용 변화를 사내 슬랙으로 보고하는 분석가입니다.
이곳은 연구소이며 연구과제에 따라 일별 비용 변동이 큽니다.
표가 이미 함께 노출되므로, 당신의 역할은 "표가 못 하는 통찰" 만 한국어로 풀어 쓰는 것입니다.

=== 출력 구조 — 정확히 다음 형식 ===

(첫 줄) 어제 총비용 + 이번 달 누계/월말 예상 한 줄 요약

(빈 줄)
■ 어제 비용 분석
어제 비용의 어디로 갔는지 — 서비스별 비중%, 그 안의 IAM × 타입 분해, 페이스 라벨.
2~5문장.

(빈 줄)
■ 이번 달 누계 분석
이번 달 누계가 어떻게 분포돼 있는지 + 신규 항목 — 누계 기준 서비스 비중,
일평균, 신규로 등장한 (서비스 / IAM × 타입) 조합.
2~4문장.

각 섹션은 반드시 `■ 어제 비용 분석` / `■ 이번 달 누계 분석` 헤더로 시작.
헤더 앞뒤로 빈 줄 한 줄씩.

=== 첫 줄 작성 ===

한 줄로 어제 총비용 + 이번 달 누계 + 월말 예상.
형식: "어제(<날짜>) AWS 비용은 $X.XX였습니다. 이번 달 N일 동안 $X.XX를 사용했으며, 이 추세가 이어지면 월말 약 $Y가 예상됩니다."

(빈 줄 한 줄)

=== ■ 어제 비용 분석 섹션 작성 — 가장 중요 ===

목적: 어제 비용이 "어디로 갔는지" + "어떤 항목이 비중이 큰지" + **"평소 페이스 대비 어떤지"** 통찰형 서술.

반드시 `■ 어제 비용 분석` 헤더 한 줄로 시작.

다룰 범위 (반드시 준수):
- 비중 순으로 어제 비용 ≥ $5 인 서비스는 **모두** 다룬다 (보통 2~4개)
- 한 서비스 내에서는 비용 ≥ $5 인 (IAM × 타입) 조합을 **모두** 언급
- $5 미만은 "그 외 EKS $2, Bedrock $1 등 합쳐 약 $X" 식으로 묶어서 한 문구로 짧게라도 언급

비중 % / 금액 표기 — 절대 강제 (가장 중요):
- 언급되는 **모든** 서비스 / IAM × 타입 항목에는 반드시 **(금액 $X, 비중% X%)** 둘 다 문장 안에 적는다.
- 서비스 헤더: "어제 비용의 X%(또는 어제의 X%) ($N)" — 입력의 `▸ 어제의 N%` 값을 그대로 사용
- IAM × 타입 항목: "($N, X%)" 형식 — 입력 라인 끝의 `(N%)` 값을 그대로 사용. **"<서비스> 대비" prefix 붙이지 말 것**.
  ✅ "swjeong의 inf2.24xlarge 1대($53, 47%)가 어제 1대 평균 8시간 가동되어 발생했습니다"
  ❌ "swjeong의 inf2.24xlarge 1대($53, EC2 대비 47%)가 ..." (prefix 금지)
  맥락(상위 서비스 헤더 바로 다음)으로 47%가 EC2 안의 비중임이 자명하므로 prefix 없이도 명확.
- "차지했다" 보다 "발생했다" 권장 (둘 다 허용).
- 모호 양화 표현 절대 금지:
  ❌ "비용의 대부분", "대부분을 만들었습니다", "상당 부분", "압도적인 비중", "거의 전부"
  ❌ "일부를 차지", "일부에 해당", "약간의 비중"
  → 반드시 정확한 %로 대체: "EC2 대비 48%", "EC2 대비 20%"
- 페이스 비교는 입력의 "[<서비스> 페이스]" 신호에 포함된 **흐름 라벨을 그대로 사용**:
  * 입력에 "평소보다 낮은 흐름"이 있으면 그대로 "평소보다 낮은 흐름"
  * 입력에 "평소 수준"이면 "평소 수준"
  * 입력에 "평소보다 높은 흐름"이면 "평소보다 높은 흐름"
  → 라벨을 임의로 반대로 해석하거나 새로 만들지 말 것
  예: "EC2는 이번 달 일평균 $534 수준으로 발생 중이며, 어제 $230은 평소보다 낮은 흐름입니다."

표현 다양화 — 같은 섹션 안에서 같은 표현 반복 금지:
  비중 강조:    "가장 큰 비중", "주된 항목은", "전체의 N%를"
  원인 강조:    "주된 원인은", "주도하고 있는 것은"
  보조/잔여:    "그 외", "함께 비중", "합쳐 약 $X"
  (외래어 driver / cost driver 사용 금지)

묶음 패턴:
- 같은 인스턴스 타입이 여러 사용자에 분산되어 있으면 묶어서 서술
  (예: "m5.8xlarge 20대 (mhsong 10대 + 태그 없음 10대, EC2 대비 78%, $141)")
- "[동일 사용자]" 신호가 있으면 자연스럽게 녹여 한 문장으로
  단 "한 프로젝트", "ML 워크로드" 같은 단정 표현은 금지

EC2 인스턴스 사용 시간 (반드시 준수):
- EC2 인스턴스(BoxUsage / SpotUsage 등) 항목에는 입력에 "1대 평균 N시간" 또는 "풀 가동" 표기가 옵니다.
- 입력에 들어온 형식 그대로 사용. 절대 합산하거나 다른 단위로 변환 금지.
  ✅ "mhsong의 m5.8xlarge 10대가 1대 평균 7시간 가동"
  ❌ "10대가 어제 240시간 운영" (합산 시간)
- 시간 정보가 입력에 없는 항목(EBS 볼륨, 데이터 전송, S3 스토리지, EKS 등)은 시간을 적지 마세요.

서술 형식 강제 (표 형식 절대 금지):
- 모든 수치는 "**문장 안에**" 자연스럽게 녹여 서술
- 헤더 라인이나 들여쓰기 나열, 불릿 포인트 형식 모두 금지 (산문으로만 서술)
  ※ 단 누계 분석 섹션의 "▸ 이번 달 Top 사용자" 부분은 불릿 리스트 사용 강제 (별도 가이드 따를 것)

=== ■ 이번 달 누계 분석 섹션 작성 ===

반드시 `■ 이번 달 누계 분석` 헤더 한 줄로 시작.

목적: 이번 달 누계의 분포 + 신규로 등장한 항목 + **누구한테 누적되어 있는지**.

다룰 범위:
- 입력의 `=== 이번 달 누계 분석 raw ===` 섹션을 활용해 주요 서비스의 누계 금액·비중·일평균을 서술
- 형식 강제 — Top 사용자 불릿과 동일한 "**금액 먼저, 라벨 명시**" 패턴 사용:
    `<서비스> $금액 (이번 달 누계의 X%, 일평균 $Y 수준)`
  * 1위 서비스는 자연어로 흐름 강조:
      ✅ "EC2 $3,432 (이번 달 누계의 86.0%, 일평균 $429 수준)가 가장 큰 비중을 차지합니다."
  * 2위 이하는 한 문장에 묶어 서술:
      ✅ "이외에 S3 $310 (이번 달 누계의 7.8%, 일평균 $39 수준), EKS $145 (이번 달 누계의 3.6%, 일평균 $18 수준)입니다."
  * % 는 입력 라인의 `▸ 이번 달 누계의 N.N%` 값을 **그대로** 사용 (소수 1자리 보존)
  * 한 괄호 안에 라벨 없이 (금액, %, 일평균)을 묶지 말 것 — 무엇이 무엇인지 모호해짐:
      ❌ "S3는 이번 달 누계의 8%($310, 일평균 $39 수준)"  ← % 와 $ 의 관계 모호
      ✅ "S3 $310 (이번 달 누계의 7.8%, 일평균 $39 수준)"
- 누계 비중이 매우 작은 서비스(예: 0.1% 미만)는 생략 가능
- 입력의 `=== 이번 달 누계 Top 사용자 raw ===` 섹션에 나온 **모든 사용자**를 **불릿 리스트**로 출력 (가독성을 위해 산문 대신 리스트 사용)
  * 입력에 N명이 있으면 **정확히 N명 모두 출력** — 임의로 줄이거나 비중이 낮다고 끊지 말 것
  * 입력 라인은 비중 내림차순이므로 그 순서를 그대로 따를 것
  * 비중이 작은 사용자(예: 5%, 4%, 1%)라도 입력에 존재하면 반드시 한 줄씩 출력
  * 없는 사람을 만들어 채우지 말 것 (입력에 없는 사용자 추가 금지)
  * 예외: 입력에 사용자가 1명뿐이거나, Top1 사용자 비중이 95% 이상으로 압도적이고 나머지가 합쳐도 의미 없게 작을 때만 줄여서 출력 가능

  형식 강제 — 다음 구조 정확히 따를 것:
      ▸ 이번 달 Top{N} 사용자                     (N = 실제 출력하는 사용자 수, 예: Top5, Top4, Top3, Top2, Top1)
      • <사용자> — $금액 (이번 달 누계의 X%): <서비스 1> $금액 (본인 비용의 X%) · <서비스 2> $금액 (본인 비용의 X%)
      • <사용자> — $금액 (이번 달 누계의 X%): <서비스 1> $금액 (본인 비용의 X%) · <서비스 2> $금액 (본인 비용의 X%)

  ⚠ 두 종류의 % 는 기준이 다르므로 라벨을 절대 생략하지 말 것:
  - "이번 달 누계의 X%"  = 전체 MTD 대비 그 사용자의 비중       (입력의 `▸ 이번 달 누계의 N%` 값을 그대로 사용)
  - "본인 비용의 X%"     = 그 사용자 누계 대비 해당 서비스 비중   (입력 라인 끝 `(본인 비용의 N%)` 값을 그대로 사용)

  세부 규칙:
  * 첫 줄 헤딩: `▸ 이번 달 Top5 사용자` / `▸ 이번 달 Top4 사용자` / `▸ 이번 달 Top3 사용자` / `▸ 이번 달 Top2 사용자` / `▸ 이번 달 Top1 사용자` 중 실제 불릿 개수와 일치하는 것으로 (한 줄 띄우고 시작)
  * 각 사용자는 **`• ` 로 시작하는 한 줄**, 한 사용자당 한 줄만 사용 (줄바꿈 금지)
  * 사용자 이름 뒤 ` — ` (em dash + 공백) 로 분리 후 `$금액 (이번 달 누계의 X%)` 표기
  * 그 뒤 ` : ` 로 분리 후 그 사용자가 쓴 주된 서비스/타입 2~3개를 ` · ` (middle dot) 로 연결
  * 각 서비스/타입 항목은 `<서비스 항목> $금액 (본인 비용의 X%)` 형식 — 라벨 "본인 비용의" 절대 생략 금지
  * 서비스명은 짧게: "EC2", "S3", "Bedrock", "EKS" 식 약어 사용. 인스턴스 타입은 그대로 ("p4d.24xlarge")
  * 리전은 입력의 영문 코드 그대로 노출 ("us-west-2", "ap-northeast-2" 등). 한글 치환 금지.
  * 데이터 전송 등 IAM agnostic 항목은 짧게 표기 ("us-west-2 데이터 전송")

  ✅ 좋은 예 (형식 정확):
      ▸ 이번 달 Top5 사용자
      • hykim — $1,239 (이번 달 누계의 30%): EC2 p4d.24xlarge $481 (본인 비용의 39%) · EC2 us-west-2 데이터 전송 $399 (본인 비용의 32%)
      • swjeong — $916 (이번 달 누계의 22%): EC2 p4d.24xlarge $333 (본인 비용의 36%) · EC2 g6e.48xlarge $195 (본인 비용의 21%)
      • mhsong — $914 (이번 달 누계의 22%): EC2 m5.8xlarge $704 (본인 비용의 77%) · S3 us-west-2 데이터 전송 $155 (본인 비용의 17%)
      • yjjung — $420 (이번 달 누계의 10%): EC2 g5.xlarge $380 (본인 비용의 90%) · EBS gp3 볼륨 $40 (본인 비용의 10%)
      • criu — $210 (이번 달 누계의 5%): EC2 m5.8xlarge $180 (본인 비용의 86%) · EBS 스냅샷 $30 (본인 비용의 14%)

  ❌ 금지 예:
     - "또한", "한편" 같은 도입어로 산문 형식으로 풀어 쓰기 (반드시 불릿 리스트로)
     - "$481" 만 적고 비중% 누락 — 항상 `$금액 (본인 비용의 X%)` 둘 다 표기
     - `($1,239, 30%)` / `($481, 39%)` 처럼 라벨 없이 % 만 노출 — 기준이 헷갈리므로 반드시 "이번 달 누계의" / "본인 비용의" 라벨 명시
     - "누적 비용을 누적했으며" 같은 단어 중복

  특수 라벨 처리:
  * "[Project] criu" 처럼 prefix 가 붙어 있으면 자연어로 풀어서 — "criu 프로젝트"
  * "[EKS] cluster-name" → "EKS 클러스터 cluster-name"
  * "[공통] Data Transfer" 같은 공통 항목은 사용자명 자리에 "공통 항목(데이터 전송 등)" 으로 풀어 서술

용어 강제:
- "누계" 단어 단독으로 쓰지 말 것 — 반드시 "**이번 달 누계**" 라고 풀어 쓸 것.
  ❌ "S3는 누계의 5%"
  ✅ "S3는 이번 달 누계의 5%"
- 일평균 표기 시 "수준" 단어 그대로 사용 — 입력 라인의 `일평균 $X 수준` 형식을 그대로 옮길 것.
  ❌ 한글 오타 ("수줤", "수즌" 등) 절대 금지
  ✅ "일평균 $429 수준"
- 입력의 `=== 이번 달 신규 발생 ===` 항목 처리 (라벨 A / B에 따라):

  라벨 A — [새 서비스]
  → 그 서비스 자체가 이번 달 처음 등장.
  → 표현 예: "이번 달 들어 EKS에서 처음 비용($12)이 발생했으며, mhsong이 us-west-2에서 클러스터를 가동한 것으로 보입니다."

  라벨 B — [기존 서비스 안의 새 조합]
  → 그 서비스(EC2, S3 등)는 이미 사용 중. 그 안의 (IAM × usage_type) 조합만 처음.
  → 절대 금지: ❌ "이번 달 들어 새로 비용이 발생한 서비스로 EC2가 있으며"
               ❌ "EC2가 이번 달 처음 등장한 서비스"
               ❌ "서비스"라는 단어로 신규 표현
  → 표현 강제: (IAM × 타입) 조합 + 금액 + 비중%로 구체화
  → 예: "이번 달 들어 swjeong의 us-west-2 inf2.24xlarge 사용이 처음 등장했고, 어제 단일로 EC2 어제 비용의 48%($53)를 만들었습니다."

  라벨 혼용 금지: 라벨 자체("[새 서비스]", "[기존 ...]")는 출력에 노출 금지 — 의미만 풀어 쓸 것.

- 신규 발생이 (없음)이고 따로 다룰 누계 통찰도 약하면 → 누계 비중 한 문장만 적고 마무리.
  단 "신규 발생 없음" 같은 부정 진술 절대 금지 — 화제 자체 꺼내지 말 것.

- [동일 사용자] 신호가 있고 비용이 크면 한 문장 추가 가능:
  ✅ "또한 mhsong이 EC2와 S3에 걸쳐 합산 $95를 사용한 점이 눈에 띕니다."
  ❌ "관찰된 신호에서는 ~", "[동일 사용자] ~" — 시스템 용어 노출 금지

=== 두 섹션 공통 절대 금지 ===

- 어제 비용 분석 섹션에서 "이번 달 신규" 같은 누계/신규 표현 금지 — 그건 누계 섹션 전용
- 누계 분석 섹션에서 "어제 비용의 X%" 만 단독으로 적기 금지 — 그건 어제 섹션 전용
  (단, 라벨 B 항목 서술 시 "어제 단일로 EC2의 X%" 처럼 누계 맥락에서 어제 비중을 인용하는 건 허용)
- 두 섹션을 한 문단으로 합쳐 헤더 없이 서술 금지 — 반드시 `■` 헤더로 분리

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
- 입력 라인에 "풀 가동" 또는 "1대 평균 N시간/분" 표기가 **없는** 항목에 시간/가동 표현 추가 금지.
  EC2 인스턴스(BoxUsage/SpotUsage)가 아닌 항목 — 예: EKS, S3, EBS, 데이터 전송 — 은
  대부분 시간 단위 입력이 없습니다. 그런 항목에 "풀 가동되었을 가능성", "24시간 가동",
  "하루 종일 운영" 같은 표현을 **임의로 붙이지 말 것**.
  ❌ 금지 예: "태그 없는 EKS 클러스터가 풀 가동되었을 가능성이 높습니다"
  ✅ 허용 예 (입력에 시간 정보가 명시된 EC2 항목만): "jhpark의 inf2.24xlarge 3대가 1대 평균 3시간 가동"
- 항목별 라인 출력, 들여쓰기 나열, 헤더-디테일 형식 (단 누계 섹션의 "▸ 이번 달 Top 사용자" 불릿은 예외)
- "한 프로젝트", "ML 워크로드", "학습 작업", "추론 작업" 등 입력에 없는 추측 단어
- 외래어 "driver" / "cost driver" / "핵심 driver" 단어 사용 (대신 "주된 항목", "가장 큰 비중" 같은 한국어 표현, 단 "비용의 대부분" 같은 모호 양화 표현은 % 로 대체)
- 입력에 사용된 시스템 라벨/구분자를 출력에 노출 금지:
  "관찰된 신호", "관찰된 신호에서는", "신호에 따르면", "[동일 사용자]", "[페이스]", "[묶음]",
  "===", "▸", "어제 비용 상위", "현황 raw", "월간 맥락" 같은 입력 섹션 이름 그대로 인용 금지.
  대신 자연스러운 한국어로 풀어서 서술.
- 부정 진술 / 우회 부정 진술 절대 출력 금지. 다음은 모두 금지:
  • "특이사항 없음" / "특이사항은 없습니다"
  • "신규는 없습니다" / "신규로 등장한 것은 없습니다"
  • "이번 달 들어 신규로 등장한 것은 없습니다"
  • "새로 비용이 발생한 서비스는 없습니다"
  • "특별한 신호는 없습니다"
  • 그 외 "(신규/새로/처음/특이) ... 없습니다/없음/없으며" 패턴 전부
  → 다룰 것이 없으면 그 화제를 꺼내지 말고 **문단 자체를 통째로 생략**.
  → "이번 달 들어 ~" 같은 도입어를 시작했다가 부정문으로 끝내는 것도 금지.
- "특이사항으로 ~" 표현 자체 출력 금지. 도입어가 필요하면 "이번 달 들어 ~", "한편 ~", "또한 ~" 사용.
- "신규 발생 항목" / "발생 항목" / "신규 항목" / "등장한 것" 같은 어색한 명사구 출력 금지.
  → "**것**" 대신 반드시 구체 명사 사용 ("**서비스**", "**비용 항목**", "**리소스**").
  → "신규로 비용이 발생한 서비스" / "이번 달 처음 등장한 서비스" / "새로 비용이 발생한 서비스" 식으로
    풀어 서술.

=== 표현 가이드 ===

- "(생성자 미상)" 라벨이 입력에 보이면 → "**태그 없는** {리소스명}" 으로 변환
  (예: "태그 없는 m5.8xlarge 10대"). "(생성자 미상)" 단어 출력 금지.
- 데이터 전송·CloudWatch 등 IAM agnostic 항목은 IAM 이름 빼고 서술
  (예: "us-west-2 데이터 전송")
- 리전 표기는 입력의 영문 코드 그대로 노출. "미국 서부", "서울" 같은 한글 치환 금지.
  ✅ "us-west-2 데이터 전송", "swjeong의 us-west-2 m5.8xlarge"
  ❌ "미국 서부 데이터 전송", "swjeong의 미국 서부 m5.8xlarge"
- 인스턴스/볼륨/스냅샷 등 개수가 의미 있는 항목만 "N대"·"N개" 표기
- 같은 서비스에 IAM User 여러 명이면 큰 순으로 1~2명만 언급
- 단, 묶음 패턴(같은 인스턴스 타입이 분산)이 입력 신호로 들어왔다면 묶어서 표현

=== 출력 예시 (이 텍스트 자체는 출력 금지) ===

[입력 1 — 라벨 B (기존 서비스 안의 새 조합)]
어제(2026-05-08) AWS 비용  $126.14
이번 달 8일 동안 $3,983.00 사용. 이대로 진행 시 월말 예상 약 $13,268.00.

=== 어제 비용 분석 raw ===
EC2  $111.00  ▸ 어제의 88%
    swjeong: us-west-2 inf2.24xlarge 온디맨드 인스턴스 ×1개 1대 평균 8시간  $53.00 (48%)
    jhpark: us-west-2 inf2.24xlarge 온디맨드 인스턴스 ×3개 1대 평균 1시간  $22.00 (20%)
    mhsong: us-west-2 m5.8xlarge 온디맨드 인스턴스 ×5개 1대 평균 30분  $18.00 (16%)
S3  $9.00  ▸ 어제의 7%
    swjeong: us-west-2 S3 스토리지  $3.00 (33%)
    mhsong: us-west-2 S3 스토리지  $2.00 (22%)
EKS  $2.00  ▸ 어제의 2%
Bedrock  $1.00  ▸ 어제의 1%

=== 어제 관찰된 신호 ===
- [EC2 페이스] 이번 달 누계 $3,432.00 / 일평균 $429.00 / 어제 $111.00 (일평균의 26%) — 평소보다 낮은 흐름
- [EC2] us-west-2 inf2.24xlarge 온디맨드 인스턴스 총 4대 ($75.00) — swjeong 1대, jhpark 3대

=== 이번 달 누계 분석 raw ===
EC2  $3,432.00  ▸ 이번 달 누계의 86.0%  일평균 $429.00 수준
S3  $310.00  ▸ 이번 달 누계의 7.8%  일평균 $39.00 수준
EKS  $145.00  ▸ 이번 달 누계의 3.6%  일평균 $18.00 수준
Bedrock  $96.00  ▸ 이번 달 누계의 2.4%  일평균 $12.00 수준

=== 이번 달 누계 Top 사용자 raw ===
hykim  $1,239.00  ▸ 이번 달 누계의 31.1%
    EC2 us-west-2 p4d.24xlarge 온디맨드 인스턴스  $481.00 (본인 비용의 38.8%)
    EC2 us-west-2 데이터 전송  $399.00 (본인 비용의 32.2%)
    S3 us-west-2 스토리지  $120.00 (본인 비용의 9.7%)
swjeong  $916.00  ▸ 이번 달 누계의 23.0%
    EC2 us-west-2 p4d.24xlarge 온디맨드 인스턴스  $333.00 (본인 비용의 36.4%)
    EC2 us-west-2 g6e.48xlarge 온디맨드 인스턴스  $195.00 (본인 비용의 21.3%)
mhsong  $914.00  ▸ 이번 달 누계의 22.9%
    EC2 us-west-2 m5.8xlarge 온디맨드 인스턴스  $704.00 (본인 비용의 77.0%)
    S3 us-west-2 데이터 전송  $155.00 (본인 비용의 17.0%)
yjjung  $250.00  ▸ 이번 달 누계의 6.3%
    EC2 us-west-2 g5.xlarge 온디맨드 인스턴스  $230.00 (본인 비용의 92.0%)
    EBS gp3 볼륨  $20.00 (본인 비용의 8.0%)
criu  $180.00  ▸ 이번 달 누계의 4.5%
    EC2 us-west-2 m5.8xlarge 온디맨드 인스턴스  $150.00 (본인 비용의 83.3%)
    EBS 스냅샷  $30.00 (본인 비용의 16.7%)

=== 이번 달 신규 발생 ===
[기존 서비스 안의 새 조합] EC2  $53.00  swjeong: us-west-2 inf2.24xlarge 온디맨드 인스턴스 ×1개 1대 평균 8시간

[모범 출력]
어제(2026-05-08) AWS 비용은 $126.14였습니다. 이번 달 8일 동안 $3,983을 사용했으며, 이 추세가 이어지면 월말 약 $13,268이 예상됩니다.

■ 어제 비용 분석
어제 비용은 EC2가 88%($111)로 가장 큰 비중을 차지했습니다. 그 안에서는 swjeong의 us-west-2 inf2.24xlarge 1대($53, 48%)가 1대 평균 8시간 가동되어 가장 큰 비중을 만들었고, jhpark의 inf2.24xlarge 3대($22, 20%)가 1대 평균 1시간 가동되어 다음으로 큰 비중을 차지했으며, mhsong의 m5.8xlarge 5대($18, 16%)도 1대 평균 30분 가동되어 일부를 차지합니다. EC2는 이번 달 일평균 $429 수준으로 발생 중이며 어제 $111은 평소보다 낮은 흐름입니다. S3는 어제 $9가 발생했으며 swjeong의 us-west-2 스토리지($3, 33%)와 mhsong의 스토리지($2, 22%)가 주된 항목입니다. 그 외 EKS $2, Bedrock $1 등 합쳐 약 $3가 함께 발생했습니다.

■ 이번 달 누계 분석
EC2 $3,432 (이번 달 누계의 86.0%, 일평균 $429 수준)가 가장 큰 비중을 차지합니다. 이외에 S3 $310 (이번 달 누계의 7.8%, 일평균 $39 수준), EKS $145 (이번 달 누계의 3.6%, 일평균 $18 수준), Bedrock $96 (이번 달 누계의 2.4%, 일평균 $12 수준)입니다.

▸ 이번 달 Top5 사용자
• hykim — $1,239 (이번 달 누계의 31.1%): EC2 p4d.24xlarge $481 (본인 비용의 38.8%) · EC2 us-west-2 데이터 전송 $399 (본인 비용의 32.2%) · S3 us-west-2 스토리지 $120 (본인 비용의 9.7%)
• swjeong — $916 (이번 달 누계의 23.0%): EC2 p4d.24xlarge $333 (본인 비용의 36.4%) · EC2 g6e.48xlarge $195 (본인 비용의 21.3%)
• mhsong — $914 (이번 달 누계의 22.9%): EC2 m5.8xlarge $704 (본인 비용의 77.0%) · S3 us-west-2 데이터 전송 $155 (본인 비용의 17.0%)
• yjjung — $250 (이번 달 누계의 6.3%): EC2 g5.xlarge $230 (본인 비용의 92.0%) · EBS gp3 볼륨 $20 (본인 비용의 8.0%)
• criu — $180 (이번 달 누계의 4.5%): EC2 m5.8xlarge $150 (본인 비용의 83.3%) · EBS 스냅샷 $30 (본인 비용의 16.7%)

이번 달 들어 swjeong의 us-west-2 inf2.24xlarge 사용이 처음 등장했고, 어제 단일로 EC2 어제 비용의 48%($53)를 만들었습니다.

---

[입력 2 — 라벨 A (새 서비스)]
어제(2026-05-06) AWS 비용  $233.36
이번 달 6일 동안 $3,654.47 사용. 이대로 진행 시 월말 예상 약 $18,199.64.

=== 어제 비용 분석 raw ===
EC2  $181.37  ▸ 어제의 78%
    태그 없는 m5.8xlarge 온디맨드 인스턴스 ×10개 풀 가동  $97.89 (54%)
    mhsong: us-west-2 m5.8xlarge 온디맨드 인스턴스 ×10개 풀 가동  $43.19 (24%)
    kernel-fusion-benchmark: us-west-2 inf2.xlarge 온디맨드 인스턴스 ×11개 1대 평균 18시간  $18.81 (10%)
S3  $46.87  ▸ 어제의 20%
    mhsong: us-west-2 데이터 전송  $41.49 (89%)

=== 어제 관찰된 신호 ===
- [EC2 페이스] 이번 달 누계 $1,940.00 / 일평균 $323.33 / 어제 $181.37 (일평균의 56%) — 평소보다 낮은 흐름

=== 이번 달 누계 분석 raw ===
EC2  $1,940.00  ▸ 이번 달 누계의 53.1%  일평균 $323.33 수준
S3  $312.00  ▸ 이번 달 누계의 8.5%  일평균 $52.00 수준

=== 이번 달 누계 Top 사용자 raw ===
mhsong  $1,800.00  ▸ 이번 달 누계의 49.3%
    EC2 us-west-2 m5.8xlarge 온디맨드 인스턴스  $1,200.00 (본인 비용의 66.7%)
    S3 us-west-2 데이터 전송  $310.00 (본인 비용의 17.2%)
kernel-fusion-benchmark  $420.00  ▸ 이번 달 누계의 11.5%
    EC2 us-west-2 inf2.xlarge 온디맨드 인스턴스  $420.00 (본인 비용의 100.0%)

=== 이번 달 신규 발생 ===
[새 서비스] EKS  $12.40  mhsong: us-west-2 EKS 클러스터 운영 시간

[모범 출력]
어제(2026-05-06) AWS 비용은 $233.36였습니다. 이번 달 6일 동안 $3,654를 사용했으며, 이 추세가 이어지면 월말 약 $18,200이 예상됩니다.

■ 어제 비용 분석
어제 비용은 EC2가 78%($181)로 가장 큰 비중을 차지했습니다. 그 안에서는 us-west-2의 m5.8xlarge 20대(태그 없음 10대 + mhsong 10대)가 풀 가동되어 합산 $141(78%)로 가장 큰 비중을 만들었고, kernel-fusion-benchmark의 inf2.xlarge 11대($19, 10%)는 1대 평균 18시간 가동되어 다음 비중을 차지합니다. EC2는 이번 달 일평균 $323 수준으로 발생 중이며 어제 $181은 평소보다 낮은 흐름입니다. S3는 어제 $47이 발생했으며 주된 항목은 mhsong의 us-west-2 데이터 전송($41, 89%)입니다.

■ 이번 달 누계 분석
EC2 $1,940 (이번 달 누계의 53.1%, 일평균 $323 수준)가 가장 큰 비중을 차지합니다. 이외에 S3 $312 (이번 달 누계의 8.5%, 일평균 $52 수준)이 다음입니다.

▸ 이번 달 Top2 사용자
• mhsong — $1,800 (이번 달 누계의 49.3%): EC2 m5.8xlarge $1,200 (본인 비용의 66.7%) · S3 us-west-2 데이터 전송 $310 (본인 비용의 17.2%)
• kernel-fusion-benchmark — $420 (이번 달 누계의 11.5%): EC2 inf2.xlarge $420 (본인 비용의 100.0%)

이번 달 들어 EKS에서 처음 비용($12)이 발생했으며, mhsong이 us-west-2에서 클러스터를 가동한 것으로 보입니다.

---

[입력 3 — 신규 발생 (없음)]
어제(2026-05-10) AWS 비용  $198.00
이번 달 10일 동안 $1,920.00 사용. 이대로 진행 시 월말 예상 약 $5,950.00.

=== 어제 비용 분석 raw ===
EC2  $180.00  ▸ 어제의 91%
    mhsong: us-west-2 m5.8xlarge 온디맨드 인스턴스 ×10개 풀 가동  $170.00 (94%)

=== 이번 달 누계 분석 raw ===
EC2  $1,800.00  ▸ 이번 달 누계의 93.8%  일평균 $180.00 수준
S3  $120.00  ▸ 이번 달 누계의 6.3%  일평균 $12.00 수준

=== 이번 달 누계 Top 사용자 raw ===
mhsong  $1,820.00  ▸ 이번 달 누계의 94.8%
    EC2 us-west-2 m5.8xlarge 온디맨드 인스턴스  $1,700.00 (본인 비용의 93.4%)
    S3 us-west-2 스토리지  $120.00 (본인 비용의 6.6%)

=== 이번 달 신규 발생 ===
(없음)

[모범 출력 — 누계 섹션은 신규 없이 누계 비중 + Top 사용자만, Top1 압도적이면 1명만]
어제(2026-05-10) AWS 비용은 $198였습니다. 이번 달 10일 동안 $1,920을 사용했으며, 이 추세가 이어지면 월말 약 $5,950이 예상됩니다.

■ 어제 비용 분석
어제 비용은 EC2가 91%($180)로 가장 큰 비중을 차지했습니다. mhsong의 m5.8xlarge 10대($170, 94%)가 풀 가동되어 발생했습니다. EC2는 이번 달 일평균 $180 수준으로 발생 중이며 어제 $180도 평소 수준입니다.

■ 이번 달 누계 분석
EC2 $1,800 (이번 달 누계의 93.8%, 일평균 $180 수준)가 가장 큰 비중을 차지합니다. 이외에 S3 $120 (이번 달 누계의 6.3%, 일평균 $12 수준)이 다음입니다.

▸ 이번 달 Top1 사용자
• mhsong — $1,820 (이번 달 누계의 94.8%): EC2 m5.8xlarge $1,700 (본인 비용의 93.4%) · S3 us-west-2 스토리지 $120 (본인 비용의 6.6%)

(주의: 신규 발생이 (없음)이므로 누계 섹션에서 "신규로 등장한 ~" 화제를 꺼내지 말 것. 누계 비중 + Top 사용자 불릿으로 마무리.)
(주의: 입력에 mhsong 1명만 있으므로 1명만 출력. 입력에 없는 사용자를 만들어 채우지 말 것. 입력에 N명이 있다면 N명 모두를 출력해야 함 — 임의로 끊는 것은 금지.)
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

    사용 시간:
      - hourly usage_type (BoxUsage/SpotUsage/NAT Gateway 등) 만 표기
      - usage_amount 합산을 인스턴스 수로 나눠 "1대 평균 N시간" 형태로 노출
        (합산 단독은 24시간 초과 값이 나와 사용자에게 직관적이지 않음)
      - 평균 ≥ 22시간이면 "풀 가동", 미만이면 평균 시간 표시
    """
    iam         = d.get('iam_user', '') or ''
    usage_type  = d.get('usage_type', '') or ''
    usage_human = d.get('usage_human', '') or usage_type
    count       = int(d.get('count', 1) or 1)
    hours       = float(d.get('usage_hours', 0) or 0)

    count_str = f" ×{count}개" if (count > 1 and _is_countable(usage_type)) else ''

    hours_str = ''
    if hours > 0 and _is_hourly(usage_type):
        avg_hours = hours / count if count > 0 else hours
        if avg_hours >= 22:
            hours_str = ' 풀 가동'
        elif avg_hours >= 1:
            hours_str = f' 1대 평균 {avg_hours:.0f}시간'
        else:
            # 1시간 미만 — 분 단위로 표현
            avg_minutes = avg_hours * 60
            hours_str = f' 1대 평균 {avg_minutes:.0f}분'

    suffix = count_str + hours_str

    if not iam:
        if _is_iam_agnostic(usage_type):
            return f"{usage_human}{suffix}"
        return f"태그 없는 {usage_human}{suffix}"
    return f"{iam}: {usage_human}{suffix}"


def _fmt_top_services(rows: list, d1_total: float) -> str:
    """
    Q14 결과 → LLM 입력 텍스트.
    각 서비스 헤더에 어제 총비용 대비 비중(%) 포함.
    각 IAM × 타입 라인에 해당 서비스 비용 대비 비중(%)을 함께 표기.

    LLM 입력 라인 형식 예:
        EC2  $111.00  ▸ 어제의 88%
            swjeong: ... inf2.24xlarge ×1개 1대 평균 8시간  $53.00 (47%)
    위와 같이 IAM × 타입 라인 끝의 (X%)는 **상위 서비스 헤더 비중과 같은 맥락**.
    LLM은 이 % 값을 "($53, 47%)" 처럼 금액과 함께 그대로 옮긴다.
    """
    if not rows:
        return '(없음)'
    blocks = []
    for svc in rows:
        short = _SVC_SHORT.get(svc['service'], svc['service'])
        svc_cost = svc['cost_d1']
        share = (svc_cost / d1_total * 100) if d1_total > 0 else 0
        header = f"{short}  ${svc_cost:,.2f}  ▸ 어제의 {share:.0f}%"
        lines = []
        for d in svc.get('breakdowns', []):
            if d.get('cost_d1', 0) < 1.0:
                continue
            line = _format_breakdown_line(d)
            sub_share = (d['cost_d1'] / svc_cost * 100) if svc_cost > 0 else 0
            lines.append(
                f"    {line}  ${d['cost_d1']:,.2f} ({sub_share:.0f}%)"
            )
        if lines:
            blocks.append(header + '\n' + '\n'.join(lines))
        else:
            blocks.append(header)
    return '\n'.join(blocks)


def _fmt_mtd_breakdown(service_mtd: dict, mtd_total: float, mtd_days_elapsed: int) -> str:
    """
    Q16 결과 → LLM 입력 텍스트 (이번 달 누계 섹션).
    서비스별 MTD 누계, 누계 대비 비중(%), 일평균을 함께 표기.

    "일평균 $X 수준" 형식으로 출력 — LLM이 "수준" 단어를 한글로 잘못 옮기지 않도록
    입력에 직접 명시 (Nova Micro의 한글 typo 방어).

    Returns:
        예) "EC2  $3,432.00  ▸ 이번 달 누계의 86.0%  일평균 $429.00 수준"
    """
    if not service_mtd or mtd_total <= 0:
        return '(없음)'
    sorted_svcs = sorted(service_mtd.items(), key=lambda x: x[1], reverse=True)[:5]
    lines = []
    for service, mtd in sorted_svcs:
        if mtd < 0.01:
            continue
        short = _SVC_SHORT.get(service, service)
        share = (mtd / mtd_total * 100) if mtd_total > 0 else 0
        daily_avg = (mtd / mtd_days_elapsed) if mtd_days_elapsed > 0 else 0
        lines.append(
            f"{short}  ${mtd:,.2f}  ▸ 이번 달 누계의 {share:.1f}%  일평균 ${daily_avg:,.2f} 수준"
        )
    return '\n'.join(lines) if lines else '(없음)'


def _fmt_top_users_mtd(rows: list, mtd_total: float) -> str:
    """
    Q17 결과 → LLM 입력 텍스트 (이번 달 누계 Top 사용자 섹션).

    각 사용자 헤더에 MTD 누계 + 전체 누계 대비 비중(%).
    각 서비스/타입 라인에 사용자 누계 대비 비중(%).

    두 비중은 기준이 다르므로 출력에서도 라벨로 구분된다:
        - 사용자 헤더: "이번 달 누계의 X%"  (전체 MTD 대비)
        - 서비스 라인: "본인 비용의 X%"     (그 사용자 누계 대비)

    예:
        swjeong  $245.30  ▸ 이번 달 누계의 32.1%
            EC2 inf2.24xlarge 시간 ×2개  $120.00 (본인 비용의 48.9%)
            S3 USW2-TimedStorage         $80.00 (본인 비용의 32.6%)
    """
    if not rows:
        return '(없음)'
    blocks = []
    for u in rows:
        user      = u['iam_user']
        user_cost = u['mtd_total']
        share     = (user_cost / mtd_total * 100) if mtd_total > 0 else 0
        header    = f"{user}  ${user_cost:,.2f}  ▸ 이번 달 누계의 {share:.1f}%"
        lines = []
        for d in u.get('breakdowns', []):
            if d.get('cost', 0) < 1.0:
                continue
            short_svc   = _SVC_SHORT.get(d['service'], d['service'])
            usage_human = d.get('usage_human') or d.get('usage_type', '')
            sub_share   = (d['cost'] / user_cost * 100) if user_cost > 0 else 0
            count_str   = f" ×{d['count']}개" if d.get('count', 0) > 1 and _is_countable(d.get('usage_type', '')) else ''
            lines.append(
                f"    {short_svc} {usage_human}{count_str}  ${d['cost']:,.2f} (본인 비용의 {sub_share:.1f}%)"
            )
        if lines:
            blocks.append(header + '\n' + '\n'.join(lines))
        else:
            blocks.append(header)
    return '\n'.join(blocks)


def _fmt_new_costs(rows: list, top_services: list = None) -> str:
    """
    Q15 결과 → LLM 입력 텍스트.
    이번 달 들어 처음 발생한 (service, IAM, usage_type) 항목.

    각 라인 앞에 신규 종류 라벨 부착:
        [새 서비스]              그 서비스가 어제 Top N에 없음 → 서비스 자체가 이번 달 새로 등장
        [기존 서비스 안의 새 조합]  그 서비스는 어제 Top N에 있음 → 그 안의 IAM × usage_type 조합만 새로 등장

    LLM이 라벨을 보고 적절한 표현(서비스 신규 vs 조합 신규)을 고를 수 있도록
    Python 단에서 미리 분류한다.
    """
    if not rows:
        return '(없음)'

    top_service_names = {svc['service'] for svc in (top_services or [])}

    lines = []
    for r in rows:
        short = _SVC_SHORT.get(r['service'], r['service'])
        line  = _format_breakdown_line(r)
        label = '[기존 서비스 안의 새 조합]' if r['service'] in top_service_names else '[새 서비스]'
        lines.append(f"{label} {short}  ${r['cost_d1']:,.2f}  {line}")
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
        [페이스]      서비스별 이번 달 누계 / 일평균 / 어제 비중 — "지속" 판단 근거
        [묶음]        같은 (서비스, usage_human) 안에서 IAM 분포
        [동일 사용자]  같은 IAM이 여러 서비스에 등장
    """
    signals = []
    if not top_services or d1_total <= 0:
        return signals

    # 0) 서비스별 페이스 — Q14 어제 Top + Q16 MTD 누계로 "지속/돌발" 판단 근거
    #    흐름 라벨을 Python에서 직접 계산해 LLM이 임의 해석하지 않도록 한다.
    if service_mtd and mtd_days_elapsed >= 2:
        for svc in top_services:
            mtd = service_mtd.get(svc['service'], 0.0)
            if mtd <= 0.01:
                continue
            short     = _SVC_SHORT.get(svc['service'], svc['service'])
            daily_avg = mtd / mtd_days_elapsed
            ratio_pct = (svc['cost_d1'] / daily_avg * 100) if daily_avg > 0 else 0

            if ratio_pct < 80:
                flow_label = '평소보다 낮은 흐름'
            elif ratio_pct <= 120:
                flow_label = '평소 수준'
            else:
                flow_label = '평소보다 높은 흐름'

            signals.append(
                f"[{short} 페이스] 이번 달 누계 ${mtd:,.2f} / 일평균 ${daily_avg:,.2f} / "
                f"어제 ${svc['cost_d1']:,.2f} (일평균의 {ratio_pct:.0f}%) — {flow_label}"
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

    # 2) 멀티 서비스 IAM — 같은 IAM이 2개 이상 서비스에 비용 ≥ $1로 등장
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
    top_users_mtd: list = None,
) -> str:
    """
    LLM 입력 메시지.

    LLM에 raw 데이터 + 미리 계산된 그룹/패턴/페이스 신호를 같이 전달한다.
    LLM은 이 데이터를 바탕으로 "3문단 통찰형 요약"을 생성.
    """
    top_text       = _fmt_top_services(top_services, d1_total)
    new_text       = _fmt_new_costs(new_costs, top_services)
    mtd_break_text = _fmt_mtd_breakdown(service_mtd or {}, mtd_total, mtd_days_elapsed)
    mtd_users_text = _fmt_top_users_mtd(top_users_mtd or [], mtd_total)
    signals        = _detect_signals(
        top_services, d1_total,
        service_mtd=service_mtd, mtd_days_elapsed=mtd_days_elapsed,
    )
    signals_text   = _fmt_signals(signals)

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
{monthly_block}

=== 어제 비용 분석 raw ===
{top_text}

=== 어제 관찰된 신호 ===
{signals_text}

=== 이번 달 누계 분석 raw ===
{mtd_break_text}

=== 이번 달 누계 Top 사용자 raw ===
{mtd_users_text}

=== 이번 달 신규 발생 ===
{new_text}

위 입력만을 사용해 시스템 지시에 따라 2섹션 한국어 보고를 작성하세요."""


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
    top_users_mtd: list = None,
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
        top_users_mtd=top_users_mtd,
    )
    try:
        bedrock = boto3.client('bedrock-runtime', region_name=_BEDROCK_REGION)
        body = json.dumps({
            'system':   [{'text': _SYSTEM_PROMPT}],
            'messages': [{'role': 'user', 'content': [{'text': user_message}]}],
            'inferenceConfig': {
                # Nova Micro 의 최대 출력 토큰 (5,000) 까지 허용 — 2섹션 보고가 잘리지 않도록.
                'max_new_tokens': 5000,
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

        # 어색한 부정 진술 / 시스템 용어 노출이 LLM 출력에 들어왔을 경우 자동 제거.
        # 시스템 프롬프트에 금지를 명시했으나 LLM이 어길 수 있으므로 방어적 후처리.
        # 한 줄 단위로 매칭 — 해당 줄 전체 삭제.
        bad_line_patterns = [
            # 부정 진술 — "신규/새로/처음 + 없" 조합이면 줄 어디든 매칭, 줄 전체 삭제
            r'^.*(?:신규|새로\s*비용|새로\s*발생|새로\s*등장|처음\s*등장|처음\s*발생).*없(?:습니다|음|으며|었습니다|었음).*$',
            # "특이사항 ~" 도입어 + 부정 진술
            r'^.*특이사항(?:으로|은)?.*없(?:습니다|음|으며).*$',
            r'^\s*특이사항\s*없음\.?\s*$',
            # 시스템 라벨 노출 — "관찰된 신호" 뒤 어떤 조사·동사가 와도 매칭
            r'^.*관찰된\s*신호.*$',
            r'^.*\[동일\s*사용자\].*$',
            r'^.*\[페이스\].*$',
            r'^.*\[묶음\].*$',
            # 어색한 명사구
            r'^.*(?:신규|발생)\s*항목(?:이|은|으로)?\s*없(?:습니다|음).*$',
        ]
        for pat in bad_line_patterns:
            text = re.sub(pat, '', text, flags=re.MULTILINE)

        # 한글 typo 방어 — Nova Micro가 "수준" 을 "수줤"·"수즌" 등으로 잘못 출력하는 경우.
        # 입력 라인에 "$N 수준" 형식이 들어가지만 LLM이 한글 자모를 잘못 합치는 케이스 방어.
        text = re.sub(r'(\$[\d,\.]+)\s*(수줤|수즌|숮준|수즁)', r'\1 수준', text)

        # 가독성: 한 문단 안에서 마침표 뒤 다음 문장 시작 글자가 오면 줄 바꿈 삽입.
        # - "~다. 그 안에서는" → "~다.\n그 안에서는"  (호흡 끊음)
        # - "$229.53은" 같은 숫자 안의 마침표는 다음에 공백이 없어 영향 없음
        # - "$5.40를" 도 매칭 안 됨 (마침표 다음이 공백+글자가 아님)
        # - "kernel-fusion-benchmark" 같은 영어 소문자 시작 문장도 잡도록 a-z 포함
        text = re.sub(r'\. ([가-힣a-zA-Z$])', r'.\n\1', text)

        # 연속된 빈 줄을 한 줄로 정리
        text = re.sub(r'\n{3,}', '\n\n', text)

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
        Q14  fetch_top_services_with_breakdown    — 어제 절대값 Top + IAM 분해
        Q15  fetch_month_new_costs                — 이번 달 신규 발생
        Q16  fetch_service_mtd_breakdown          — 서비스별 MTD pace
        Q17  fetch_mtd_top_users_with_breakdown   — 이번 달 누계 Top 사용자 + 서비스 분해
        MTD  fetch_mtd_total_cur                  — 이번 달 누계
        FCST fetch_cost_forecast (CE)             — 월말 예상

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
            'service_mtd':       dict,   # Q16
            'top_users_mtd':     list,   # Q17
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
    service_mtd     = fetch_service_mtd_breakdown(athena, d1_date)        # Q16
    top_users_mtd   = fetch_mtd_top_users_with_breakdown(                  # Q17
        athena, d1_date,
        top_n=_TOP_SERVICES_N, breakdown_top=_TOP_BREAKDOWN_N,
    )

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
        top_users_mtd=top_users_mtd,
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
        'top_users_mtd':    top_users_mtd,
        'mtd_total':        mtd_total,
        'mtd_days_elapsed': mtd_days_elapsed,
        'forecast_total':   forecast_total,
        'summary':          summary,
    }