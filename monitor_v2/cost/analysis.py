"""
monitor_v2/cost/analysis.py

CUR Athena 기반 비용 증감 원인 분석 + Amazon Nova Micro LLM 요약.

3단계 드릴다운:
    Q9   서비스별           (product_product_name)
    Q10  리소스 타입별      (line_item_usage_type)
    Q11  리소스 ID별        (line_item_resource_id)

신규 분석:
    Q12  최근 30일 일별 총비용 → 이상치 탐지 (μ ± kσ)
    Q13  전월 동일일 총비용    → 전월 동일일 비교

환경변수:
    BEDROCK_MODEL_ID    기본: amazon.nova-micro-v1:0
    BEDROCK_REGION      기본: us-east-1  (Nova Micro 지원 리전)
    ANOMALY_SIGMA       기본: 2.0        (이상치 판단 σ 배수)
    ANOMALY_HIST_DAYS   기본: 30         (히스토리 기간)
"""

import os
import json
import math
from calendar import monthrange
from pprint import pprint

import boto3
import logging
from datetime import date, timedelta

from .data_cur import _run_query, _partition, _ATHENA_DATABASE, _ATHENA_REGION

log = logging.getLogger(__name__)

_BEDROCK_MODEL_ID  = os.environ.get('BEDROCK_MODEL_ID')
_BEDROCK_REGION    = os.environ.get('BEDROCK_REGION')
_TOP_N             = 10
_ANOMALY_SIGMA     = float(os.environ.get('ANOMALY_SIGMA', '1.5'))
_ANOMALY_HIST_DAYS = int(os.environ.get('ANOMALY_HIST_DAYS', '30'))


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

    def _parse_iam_user(raw: str) -> str:
        # "IAMUser:AIDA3OGATNBRMEBUIOEWO:mhsong" → "mhsong"
        parts = raw.split(':')
        return parts[2] if len(parts) >= 3 else raw

    rows = _run_query(athena, sql)
    return [
        {
            'service':     r['service'],
            'usage_type':  r.get('usage_type', ''),
            'resource_id': r.get('resource_id', ''),
            'iam_user':    _parse_iam_user(r['iam_user']) if r.get('iam_user') else '',
            'cost_d1':     float(r.get('cost_d1') or 0),
            'cost_d2':     float(r.get('cost_d2') or 0),
            'diff':        float(r.get('diff') or 0),
        }
        for r in rows if r.get('service')
    ]


# ---------------------------------------------------------------------------
# 신규 Athena 쿼리 (Q12, Q13) + 순수 계산
# ---------------------------------------------------------------------------

_WEEKDAY_LABEL = ['월', '화', '수', '목', '금', '토', '일']


def _day_context(d: date) -> str:
    wd = d.weekday()
    label = _WEEKDAY_LABEL[wd]
    if wd >= 5:
        return f"{label}요일 (주말)"
    return f"{label}요일 (주중)"


def _same_day_last_month(d: date) -> date:
    year, month = (d.year, d.month - 1) if d.month > 1 else (d.year - 1, 12)
    max_day = monthrange(year, month)[1]
    return date(year, month, min(d.day, max_day))


def fetch_historical_stats(athena, d1_date: date, d1_total: float) -> dict:
    """
    Q12: 최근 ANOMALY_HIST_DAYS일 일별 총비용 (d1 제외) → μ, σ, is_anomaly 계산.

    Returns:
        {
            'mu_7': float, 'sigma_7': float,
            'mu_30': float, 'sigma_30': float,
            'is_anomaly_7': bool, 'is_anomaly_30': bool,
            'anomaly_direction': str | None,  # 'high' | 'low' | None
            'hist_days': int,
        }
    """
    d2_date    = d1_date - timedelta(days=1)
    start_date = d1_date - timedelta(days=_ANOMALY_HIST_DAYS)

    years_set  = set()
    months_set = set()
    cur = start_date
    while cur <= d2_date:
        years_set.add(str(cur.year))
        months_set.add(str(cur.month))
        cur += timedelta(days=1)

    year_range  = ', '.join(f"'{y}'" for y in sorted(years_set))
    month_range = ', '.join(f"'{m}'" for m in sorted(months_set))

    sql = f"""
        SELECT
            DATE(line_item_usage_start_date) AS cost_date,
            SUM(line_item_unblended_cost)    AS daily_total
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  IN ({year_range})
          AND month IN ({month_range})
          AND DATE(line_item_usage_start_date) BETWEEN DATE('{start_date}') AND DATE('{d2_date}')
        GROUP BY DATE(line_item_usage_start_date)
        ORDER BY cost_date DESC
    """
    rows = _run_query(athena, sql)
    hist = [float(r['daily_total']) for r in rows if r.get('daily_total')]

    n = len(hist)

    empty = {
        'mu_7': 0.0, 'sigma_7': 0.0,
        'mu_30': 0.0, 'sigma_30': 0.0,
        'is_anomaly_7': False, 'is_anomaly_30': False,
        'anomaly_direction': None, 'hist_days': n,
    }
    if n < 7:
        return empty

    def _stats(values):
        mu = sum(values) / len(values)
        if len(values) < 2:
            return mu, 0.0
        variance = sum((v - mu) ** 2 for v in values) / (len(values) - 1)
        return mu, math.sqrt(variance)

    hist7  = hist[:min(7, n)]
    hist30 = hist

    mu_7, sigma_7   = _stats(hist7)
    mu_30, sigma_30 = _stats(hist30)

    k = _ANOMALY_SIGMA
    is_anomaly_7  = d1_total > mu_7  + k * sigma_7
    is_anomaly_30 = d1_total > mu_30 + k * sigma_30

    direction = 'high' if (is_anomaly_7 or is_anomaly_30) else None

    return {
        'mu_7':      mu_7,
        'sigma_7':   sigma_7,
        'mu_30':     mu_30,
        'sigma_30':  sigma_30,
        'is_anomaly_7':  is_anomaly_7,
        'is_anomaly_30': is_anomaly_30,
        'anomaly_direction': direction,
        'hist_days': n,
    }


def fetch_lm_same_day(athena, d1_date: date) -> tuple:
    """
    Q13: 전월 동일일 총비용.

    Returns:
        (lm_date: date, lm_total: float)
    """
    lm_date = _same_day_last_month(d1_date)
    lm_year, lm_month = _partition(lm_date)

    sql = f"""
        SELECT SUM(line_item_unblended_cost) AS cost
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{lm_year}'
          AND month = '{lm_month}'
          AND DATE(line_item_usage_start_date) = DATE('{lm_date}')
    """
    rows = _run_query(athena, sql)
    if rows and rows[0].get('cost'):
        return lm_date, float(rows[0]['cost'])
    return lm_date, 0.0


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

    if 'Bedrock' in rest and 'ModelUnit' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}Bedrock 모델 호출"

    if 'Lambda' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}Lambda 함수 실행"

    if 'CloudWatch' in rest or 'Metrics' in rest or 'Logs' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}CloudWatch 모니터링"

    if 'CostExplorer' in rest or 'Cost-Explorer' in rest:
        return 'Cost Explorer API 조회'

    if 'Bytes' in rest or 'DataTransfer' in rest:
        return f"{region_prefix + ' ' if region_prefix else ''}데이터 전송"

    return raw  # 해석 불가 시 원본 반환


def _get_change_type(cost_d1: float, cost_d2: float) -> str:
    """
    어제(d1)와 그제(d2) 비용으로 변화 유형 판별.

    Returns:
      'new'     : 그제 $0 → 어제 처음 발생
      'stopped' : 어제 $0 → 그제에만 사용, 어제 중단
      'changed' : 양일 모두 비용 존재, 증감
    """
    if cost_d2 == 0 and cost_d1 > 0:
        return 'new'
    if cost_d1 == 0 and cost_d2 > 0:
        return 'stopped'
    return 'changed'


_CHANGE_LABEL = {
    'new':     '어제 처음 발생',
    'stopped': '어제 사용 없음',
    'changed': '증감',
}


def _aggregate_details(details: list) -> list:
    """
    동일 (usage_human, iam_user) 조합을 하나로 집계.
    resource_id별 중복 행을 제거하고 비용을 합산한다.
    """
    from collections import defaultdict
    agg = defaultdict(lambda: {'diff': 0.0, 'cost_d1': 0.0, 'cost_d2': 0.0, 'count': 0})
    for d in details:
        key = (d['usage_human'], d['iam_user'])
        agg[key]['diff']    += d['diff']
        agg[key]['cost_d1'] += d['cost_d1']
        agg[key]['cost_d2'] += d['cost_d2']
        agg[key]['count']   += 1

    result = []
    for (usage_human, iam_user), v in agg.items():
        result.append({
            'usage_human': usage_human,
            'iam_user':    iam_user,
            'diff':        v['diff'],
            'cost_d1':     v['cost_d1'],
            'cost_d2':     v['cost_d2'],
            'change_type': _get_change_type(v['cost_d1'], v['cost_d2']),
            'count':       v['count'],
        })
    return sorted(result, key=lambda x: abs(x['diff']), reverse=True)


def _merge_rows(service_rows: list, resource_rows: list) -> list:
    """
    Q9(서비스 총합)와 Q11(리소스+IAM User)를 service 기준으로 통합.

    LLM이 섹션 간 cross-reference 추론 없이 한 블록에서 읽을 수 있도록
    Python 단에서 미리 join한다.
    """
    service_map = {
        r['service']: {
            'service':     r['service'],
            'total_diff':  r['diff'],
            'cost_d1':     r['cost_d1'],
            'cost_d2':     r['cost_d2'],
            'change_type': _get_change_type(r['cost_d1'], r['cost_d2']),
            'details':     [],
        }
        for r in service_rows
    }

    for r in resource_rows:
        svc = r['service']
        if svc in service_map:
            service_map[svc]['details'].append({
                'usage_human': _humanize_usage_type(r.get('usage_type', '')),
                'iam_user':    r.get('iam_user', ''),
                'diff':        r['diff'],
                'cost_d1':     r['cost_d1'],
                'cost_d2':     r['cost_d2'],
                'change_type': _get_change_type(r['cost_d1'], r['cost_d2']),
            })

    for svc_data in service_map.values():
        svc_data['details'] = _aggregate_details(svc_data['details'])

    return sorted(service_map.values(), key=lambda x: abs(x['total_diff']), reverse=True)


_SYSTEM_PROMPT = """\
당신은 AWS 클라우드 비용 분석 전문가입니다.
어제 비용 데이터와 다양한 비교 지표(전일 대비, 7일/30일 평균, 전월 동일일)를 종합하여
비용 변화의 원인을 깊이 있게 분석하고 한국어로 보고합니다.

=== 분석 원칙 ===

단순 나열이 아닌 원인 분석:
  - 어제/그제 증감 수치만 반복하지 말 것.
  - 비교 지표(7일/30일 평균, 전월 동일일)를 근거로 삼아
    "이 변화가 일시적인지, 추세인지, 예외적인 사건인지"를 판단해 서술합니다.
  - IAM User와 리소스를 연결해 "누가, 무엇을, 왜 변화시켰는지"를 구체적으로 서술합니다.

비교 지표 활용 방법:
  - 전월 동일일 비교: "전월 같은 날($X.XX)보다 Y% 높다/낮다" → 월간 패턴 변화 여부 판단
  - 7일/30일 이상치: 단순 전일 대비가 아닌 최근 경향 기준으로 이례성 판단

=== 출력 구조 ===

[첫 줄 — 두괄식 한 줄 요약. 반드시 정상/이상 판단 포함]

  이상치(7일 또는 30일 기준 고비용)인 경우:
    "어제 AWS 비용은 $X.XX로, 최근 [7일/30일] 평균($Y.YY)을 크게 초과한 이상 수준입니다. [서비스명]에서 급증이 발생했습니다."
  정상인 경우:
    "어제 AWS 비용은 $X.XX로, 최근 30일 평균($Y.YY) 내 정상 범위입니다. [서비스명 2건]에서 주요 변동이 있었습니다."
  어제가 주말이고 정상인 경우:
    "어제는 주말로 전반적인 비용이 줄어 $X.XX를 기록했으며, 최근 30일 평균($Y.YY) 내 정상 범위입니다."

[두 번째 단락 — 비교 지표 기반 맥락 분석. 아래 중 해당하는 것만 자연스럽게 서술]
  - 전월 동일일 대비 차이가 ±10% 이상이면 반드시 언급
  - 이상치일 때 "이 추세가 지속되면 비용이 계속 높아질 수 있습니다" 등 전망 포함

빈 줄
▲ 증가 원인
서비스명 — $금액  원인 분석 (IAM User, 리소스 타입, 이전 패턴 대비 설명)

=== 서비스 서술 범위 규칙 (반드시 준수) ===

[이상치인 경우]
  - ▲ 증가 원인: 고비용을 유발한 서비스와 IAM User 중심으로 원인 분석 서술.
  - 주말임에도 이상치인 경우: "주말임에도 불구하고" 표현 사용.

[정상 범위인 경우]
  - ▲ 증가 원인만 서술합니다. 감소 원인은 서술하지 않습니다.
  - 비용 증가 절대값 상위 2건 서비스와 IAM User를 서술합니다.
  - 3번째 이하 서비스는 생략합니다.

=== 서비스별 서술 방식 ===

서비스 타입:
  [신규] — 그제 $0 → 어제 처음 발생. "처음 사용됨" 등.
  [중단] — 어제 $0 → 그제까지 사용. "어제 사용 없었음" 등.
  [증가] / [감소] — 두 날 모두 비용 존재. 방향에 맞게 서술.

usage detail 타입:
  [신규 발생] — 어제 처음 등장. "처음 켜짐" 등. 절대 [중단]에 "처음" 사용 금지.
  [어제 중단] — 어제 사용 없음. "중단됨" 등.
  [증가] / [감소] — 방향에 맞게 서술.

완전한 문장으로, 동사형 종결("~했습니다", "~줄었습니다")로 마무리합니다.
생성자(IAM User)가 있으면 반드시 이름과 행동을 함께 서술합니다.

=== 입출력 예시 (이 텍스트는 출력하지 말 것) ===

[이상치 예시 입력]
어제(2026-04-09, 목요일 (주중)) AWS 비용: $180.00
그제(2026-04-08, 수요일 (주중)) AWS 비용: $61.80
전일 대비: +$118.20 (+191.3%)

=== 비교 지표 ===
최근 7일 평균: $45.00 (σ=8.50)
  → 어제 비용이 7일 기준 이상치 ▲ 고비용 (μ + 2.0σ=$62.00 초과)
최근 30일 평균: $48.00 (σ=10.00)
  → 어제 비용이 30일 기준 이상치 ▲ 고비용 (μ + 2.0σ=$68.00 초과)
전월 동일일(2026-03-09) 비용: $52.00 (+246.2%)

=== 비용이 증가한 서비스 ===
EC2  $125.00  [증가]  어제 $155.00 / 그제 $30.00
    mhsong: 미국 서부(us-west-2) c8gd.48xlarge Spot 인스턴스 ×10개  [신규 발생]  어제 $120.00 / 그제 $0.00
    jhpark: 미국 서부(us-west-2) inf2.8xlarge 온디맨드 인스턴스  [증가]  어제 $35.00 / 그제 $30.00

[이상치 예시 출력]
어제 AWS 비용은 $180.00로, 최근 7일 평균($45.00)의 4배를 초과한 이상 수준입니다. EC2에서 대규모 인스턴스 신규 가동이 발생했습니다.

전월 동일일($52.00) 대비 246% 높은 수준입니다. 이 추세가 이어진다면 비용이 계속 높아질 수 있습니다.

▲ 증가 원인
EC2 — $125.00  mhsong이 미국 서부에서 c8gd.48xlarge Spot 인스턴스 10대를 어제 처음 가동하여 $120.00의 비용이 발생했습니다. jhpark의 inf2.8xlarge 인스턴스도 소폭 증가했습니다.

---

[정상 예시 입력]
어제(2026-04-12, 일요일 (주말)) AWS 비용: $32.69
그제(2026-04-11, 토요일 (주말)) AWS 비용: $61.80
전일 대비: -$29.11 (-47.1%)

=== 비교 지표 ===
최근 7일 평균: $45.00 (σ=8.50)
  → 어제 비용이 7일 기준 정상 범위 (μ - 2.0σ=$28.00 ~ μ + 2.0σ=$62.00)
최근 30일 평균: $48.00 (σ=10.00)
  → 어제 비용이 30일 기준 정상 범위 (μ - 2.0σ=$28.00 ~ μ + 2.0σ=$68.00)
전월 동일일(2026-03-12) 비용: $38.00 (-14.0%)

=== 비용이 증가한 서비스 ===
Bedrock  $0.04  [신규]  어제 $0.04 / 그제 $0.00

[정상 예시 출력]
어제는 주말로 전반적인 비용이 줄어 $32.69를 기록했으며, 최근 30일 평균($48.00) 내 정상 범위입니다. $1 이상 증가 서비스가 없었습니다.

전월 동일 주말($38.00)과 비교해도 14% 낮은 수준으로, 주말 비용 패턴은 안정적입니다.

=== 금지 사항 ===

리소스 ID(i-xxx, vol-xxx, arn:...) 포함 금지.
금액은 항상 양수. 부호는 ▲/▼ 소제목으로만 구분.
마크다운(# ## ### ** *) 사용 금지. 특히 첫 줄에 ### 절대 금지.
"해당 서비스 사용량 변화" 사용 금지.
"주말 비용 패턴" 표현 금지. 대신 "주말로 전반적인 비용이 줄었습니다" 등으로 서술.
"어제 중단으로 감소했습니다" 표현 금지. 서비스가 어제 사용되지 않았다면 "어제 사용 내역이 없었습니다" 등으로 서술.
생성자가 있는데 이름 빠뜨리기 금지.
비교 지표 수치를 단순 나열만 하고 해석하지 않는 것 금지.
$1 미만 변동 서비스 언급 금지."""


# 서비스명 단축 — Python에서 미리 처리해 LLM에 전달
_SVC_SHORT = {
    'Amazon Elastic Compute Cloud': 'EC2',
    'Amazon Simple Storage Service': 'S3',
    'AWS Lambda': 'Lambda',
    'Elastic Load Balancing': 'ELB',
    'Amazon Virtual Private Cloud': 'VPC',
    'AWS Cost Explorer': 'Cost Explorer',
    'AmazonCloudWatch': 'CloudWatch',
    'Amazon CloudFront': 'CloudFront',
    'Amazon Bedrock': 'Bedrock',
}


def _svc_label(svc: dict) -> str:
    if svc['change_type'] == 'new':
        return '신규'
    if svc['change_type'] == 'stopped':
        return '중단'
    return '증가' if svc['total_diff'] > 0 else '감소'


def _detail_label(d: dict) -> str:
    if d['change_type'] == 'new':
        return '신규 발생'
    if d['change_type'] == 'stopped':
        return '어제 중단'
    return '증가' if d['diff'] > 0 else '감소'


def _fmt_section(rows: list) -> str:
    if not rows:
        return '  (없음)'
    blocks = []
    for svc in rows:
        short  = _SVC_SHORT.get(svc['service'], svc['service'])
        amount = abs(svc['total_diff'])
        label  = _svc_label(svc)
        header = (
            f"{short}  ${amount:,.2f}  [{label}]"
            f"  어제 ${svc['cost_d1']:,.2f} / 그제 ${svc['cost_d2']:,.2f}"
        )
        detail_lines = []
        for d in svc['details']:
            who       = d['iam_user'] if d.get('iam_user') else '(생성자 미상)'
            count_str = f" ×{d['count']}개" if d.get('count', 1) > 1 else ""
            dlabel    = _detail_label(d)
            detail_lines.append(
                f"    {who}: {d['usage_human']}{count_str}"
                f"  [{dlabel}]  어제 ${d['cost_d1']:,.2f} / 그제 ${d['cost_d2']:,.2f}"
            )
        blocks.append(header + ('\n' + '\n'.join(detail_lines) if detail_lines else ''))
    return '\n'.join(blocks)


def _anomaly_text(mu: float, sigma: float, d1_total: float) -> str:
    k = _ANOMALY_SIGMA
    lower = mu - k * sigma
    upper = mu + k * sigma
    if d1_total > upper:
        return f"이상치 ▲ 고비용 (μ + {k}σ={upper:.2f} 초과)"
    return f"정상 범위 (μ - {k}σ={lower:.2f} ~ μ + {k}σ={upper:.2f})"



def _build_user_message(
    d1_date: date, d2_date: date,
    d1_total: float, d2_total: float,
    service_rows: list, usage_type_rows: list, resource_rows: list,
    anomaly_stats: dict = None,
    lm_date: date = None, lm_total: float = 0.0,
    d1_day_context: str = '', d2_day_context: str = '',
) -> str:
    diff = d1_total - d2_total
    pct  = (diff / d2_total * 100) if d2_total else 0.0

    is_anomaly = bool(
        anomaly_stats
        and (anomaly_stats.get('is_anomaly_7') or anomaly_stats.get('is_anomaly_30'))
    )

    merged = _merge_rows(service_rows, resource_rows)

    increase_rows = [s for s in merged if s['total_diff'] > 0]

    # 정상 범위일 때: 증가 상위 2건만 LLM에 전달 (소액 나열 방지)
    if not is_anomaly:
        increase_rows = increase_rows[:2]

    increase_text = _fmt_section(increase_rows)

    # 비교 지표 섹션
    comparison_lines = []
    if anomaly_stats and anomaly_stats.get('hist_days', 0) >= 7:
        mu_7     = anomaly_stats['mu_7']
        sigma_7  = anomaly_stats['sigma_7']
        mu_30    = anomaly_stats['mu_30']
        sigma_30 = anomaly_stats['sigma_30']
        comparison_lines += [
            f"최근 7일 평균: ${mu_7:.2f} (σ={sigma_7:.2f})",
            f"  → 어제 비용이 7일 기준 {_anomaly_text(mu_7, sigma_7, d1_total)}",
            f"최근 30일 평균: ${mu_30:.2f} (σ={sigma_30:.2f})",
            f"  → 어제 비용이 30일 기준 {_anomaly_text(mu_30, sigma_30, d1_total)}",
        ]
    else:
        comparison_lines.append("최근 히스토리 데이터 부족 (이상치 판단 생략)")

    if lm_date and lm_total > 0:
        lm_diff = d1_total - lm_total
        lm_pct  = (lm_diff / lm_total * 100) if lm_total else 0.0
        comparison_lines.append(f"전월 동일일({lm_date}) 비용: ${lm_total:.2f} ({lm_pct:+.1f}%)")

    comparison_section = '\n'.join(comparison_lines) if comparison_lines else '(없음)'

    d1_label = f"{d1_date}, {d1_day_context}" if d1_day_context else str(d1_date)
    d2_label = f"{d2_date}, {d2_day_context}" if d2_day_context else str(d2_date)

    return f"""어제({d1_label}) AWS 비용: ${d1_total:,.2f}
그제({d2_label}) AWS 비용: ${d2_total:,.2f}
전일 대비: {_fmt_sign(diff)} ({pct:+.1f}%)

=== 비교 지표 ===

{comparison_section}

=== 비용이 증가한 서비스 ===

{increase_text}

위 데이터를 요약하세요."""


# ---------------------------------------------------------------------------
# Bedrock Nova Micro 호출
# ---------------------------------------------------------------------------

def summarize(
    d1_date: date, d2_date: date,
    d1_total: float, d2_total: float,
    service_rows: list, usage_type_rows: list, resource_rows: list,
    anomaly_stats: dict = None,
    lm_date: date = None, lm_total: float = 0.0,
    d1_day_context: str = '', d2_day_context: str = '',
) -> str:
    """
    Nova Micro에게 비용 증감 원인 분석을 요청하고 요약 텍스트를 반환한다.

    실패 시 폴백 텍스트 반환 (Lambda 전체 실패 방지).
    """
    user_message = _build_user_message(
        d1_date, d2_date, d1_total, d2_total,
        service_rows, usage_type_rows, resource_rows,
        anomaly_stats=anomaly_stats,
        lm_date=lm_date, lm_total=lm_total,
        d1_day_context=d1_day_context,
        d2_day_context=d2_day_context,
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
        # LLM이 출력한 마크다운 헤딩(# ## ###)을 제거
        import re
        text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
        return text.strip()

    except Exception as e:
        log.error("Bedrock 호출 실패: %s", e)
        diff = d1_total - d2_total
        direction = "증가" if diff >= 0 else "감소"
        return f"LLM 분석 실패 (Bedrock 오류). 전일 대비 ${abs(diff):,.2f} {direction}."


# ---------------------------------------------------------------------------
# 일괄 수집 진입점
# ---------------------------------------------------------------------------

def collect_all(d1_date: date) -> dict:
    """
    Athena 쿼리 실행 (Q9~Q13) + CE Forecast + Nova Micro 요약 생성.
    mtd_this / forecast 는 내부에서 직접 수집한다.

    Args:
        d1_date: 리포트 기준일

    Returns:
        {
            'd1_date':         date,
            'd2_date':         date,
            'd1_total':        float,
            'd2_total':        float,
            'service_rows':    list,
            'usage_type_rows': list,
            'resource_rows':   list,
            'anomaly_stats':   dict,
            'lm_total':        float,
            'lm_date':         date,
            'd1_day_context':  str,
            'd2_day_context':  str,
            'summary':         str,
        }
    """
    d2_date = d1_date - timedelta(days=1)
    athena  = boto3.client('athena', region_name=_ATHENA_REGION)

    service_rows    = fetch_service_diff(athena, d1_date, d2_date)
    usage_type_rows = fetch_usage_type_diff(athena, d1_date, d2_date)
    resource_rows   = fetch_resource_diff(athena, d1_date, d2_date)

    d1_total = sum(r['cost_d1'] for r in service_rows)
    d2_total = sum(r['cost_d2'] for r in service_rows)

    anomaly_stats      = fetch_historical_stats(athena, d1_date, d1_total)
    lm_date, lm_total = fetch_lm_same_day(athena, d1_date)

    d1_day_context = _day_context(d1_date)
    d2_day_context = _day_context(d2_date)

    summary = summarize(
        d1_date, d2_date, d1_total, d2_total,
        service_rows, usage_type_rows, resource_rows,
        anomaly_stats=anomaly_stats,
        lm_date=lm_date, lm_total=lm_total,
        d1_day_context=d1_day_context,
        d2_day_context=d2_day_context,
    )

    return {
        'd1_date':         d1_date,
        'd2_date':         d2_date,
        'd1_total':        d1_total,
        'd2_total':        d2_total,
        'service_rows':    service_rows,
        'usage_type_rows': usage_type_rows,
        'resource_rows':   resource_rows,
        'anomaly_stats':   anomaly_stats,
        'lm_total':        lm_total,
        'lm_date':         lm_date,
        'd1_day_context':  d1_day_context,
        'd2_day_context':  d2_day_context,
        'summary':         summary,
    }
