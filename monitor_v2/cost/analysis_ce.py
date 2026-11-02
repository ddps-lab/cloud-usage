"""
monitor_v2/cost/analysis_ce.py

Main 3 의 CUR 조회(Q9~Q17)를 Cost Explorer 로 대체하는 fallback.

언제 쓰는가:
    CUR 에 d1 데이터가 아직 없을 때. AWS 는 CUR 을 하루 몇 번 S3 에 덮어쓰는데,
    월이 바뀐 직후에는 새 달의 첫 파일이 하루 넘게 늦게 온다 (2026-11-02 22:30 KST
    까지 month=11 파일이 없었다). 그동안 CUR 쿼리는 빈 결과를 돌려주고, 그대로
    두면 Main 3 가 "어제 $0.00" 을 보고한다. CE 는 같은 날 이미 값이 있다.

CUR 대비 잃는 것:
    - Q11 리소스 ID 별 변화: CE 의 리소스 단위 조회(GetCostAndUsageWithResources)는
      이 계정에서 막혀 있다 ("not supported when accessing proforma data"). 빈 목록.
    - 생성자: CUR 은 _build_creator_case_sql 의 태그 fallback 체인과 owner_resolver
      로 EKS/ASG 노드까지 사람에게 돌리지만, CE 는 aws:createdBy 태그 하나만 본다.
      EKS 노드는 'AutoScaling' 으로, 태그 없는 리소스는 빈 생성자로 나온다.
    - 리소스 개수 / 사용 시간: CE 는 distinct resource 를 세지 못한다. count=1,
      usage_hours=0 으로 두어 "×N개", "1대 평균 N시간" 표기가 나오지 않게 한다.
    - Q15 / Q17 의 분해 축: CE GroupBy 는 최대 2개라서 Q15 는 (service, usage_type),
      Q17 은 (user, service) 까지만 나눈다.

반환 모양은 analysis.py 의 CUR 함수들과 같다. 서비스 이름만 CE 의 SERVICE 값
(예: 'Amazon Elastic Compute Cloud - Compute', 'EC2 - Other')을 그대로 쓴다.

금액 기준도 CUR 쿼리와 맞춘다: RECORD_TYPE 이 Credit / Refund / Tax 인 행을 빼고
UnblendedCost 를 더한다. CUR 의 line_item_line_item_type NOT IN (...) 과 같은 조건이다.
"""

from collections import defaultdict
from datetime import date, timedelta

from .analysis import (
    _TAX_MULTIPLIER, _TOP_N, _NEW_COST_THRESHOLD, _NEW_COST_PRIOR_CUT,
    _humanize_usage_type, _parse_iam_user,
)

_EXCLUDE_RECORD_TYPES = {
    'Not': {'Dimensions': {'Key': 'RECORD_TYPE', 'Values': ['Credit', 'Refund', 'Tax']}},
}
_TAG_CREATED_BY = 'aws:createdBy'


# ---------------------------------------------------------------------------
# CE 호출 헬퍼
# ---------------------------------------------------------------------------

def _filter(service: str | None = None) -> dict:
    if service is None:
        return _EXCLUDE_RECORD_TYPES
    return {'And': [
        _EXCLUDE_RECORD_TYPES,
        {'Dimensions': {'Key': 'SERVICE', 'Values': [service]}},
    ]}


def _group_by(*keys: str) -> list:
    """'SERVICE', 'USAGE_TYPE' 은 DIMENSION, 그 밖의 키는 TAG 로 본다."""
    return [
        {'Type': 'DIMENSION' if k in ('SERVICE', 'USAGE_TYPE') else 'TAG', 'Key': k}
        for k in keys
    ]


def _tag_value(raw: str) -> str:
    """CE 의 TAG 키는 'aws:createdBy$<value>' 로 온다. 태그가 없으면 value 가 빈 문자열."""
    return raw.split('$', 1)[1] if '$' in raw else raw


def _ce_rows(ce, start: date, end_inclusive: date, granularity: str,
             group_keys: tuple, service: str | None = None) -> list:
    """
    get_cost_and_usage 를 NextPageToken 끝까지 불러 행 목록으로 편다.

    Returns:
        [{'day': 'YYYY-MM-DD', 'keys': [...], 'cost': float, 'qty': float}, ...]
        TAG 키는 _tag_value 로 값만 남긴다.
    """
    kwargs = {
        'TimePeriod':  {'Start': str(start), 'End': str(end_inclusive + timedelta(days=1))},
        'Granularity': granularity,
        'Metrics':     ['UnblendedCost', 'UsageQuantity'],
        'GroupBy':     _group_by(*group_keys),
        'Filter':      _filter(service),
    }
    tag_idx = [i for i, k in enumerate(group_keys) if k not in ('SERVICE', 'USAGE_TYPE')]
    rows = []
    while True:
        resp = ce.get_cost_and_usage(**kwargs)
        for period in resp.get('ResultsByTime', []):
            day = period['TimePeriod']['Start']
            for g in period.get('Groups', []):
                keys = list(g['Keys'])
                for i in tag_idx:
                    keys[i] = _tag_value(keys[i])
                rows.append({
                    'day':  day,
                    'keys': keys,
                    'cost': float(g['Metrics']['UnblendedCost']['Amount']),
                    'qty':  float(g['Metrics'].get('UsageQuantity', {}).get('Amount', 0) or 0),
                })
        token = resp.get('NextPageToken')
        if not token:
            return rows
        kwargs['NextPageToken'] = token


def _diff_rows(rows: list, d1_date: date, d2_date: date, key_names: tuple) -> list:
    """일별 행을 키별 cost_d1 / cost_d2 / diff 로 접고, |diff| 큰 순 Top N (Q9 / Q10 규칙)."""
    acc = defaultdict(lambda: {'cost_d1': 0.0, 'cost_d2': 0.0})
    for r in rows:
        col = 'cost_d1' if r['day'] == str(d1_date) else 'cost_d2' if r['day'] == str(d2_date) else None
        if col:
            acc[tuple(r['keys'])][col] += r['cost']
    out = []
    for keys, v in acc.items():
        diff = v['cost_d1'] - v['cost_d2']
        if abs(diff) <= 0.01:
            continue
        out.append({**dict(zip(key_names, keys)),
                    'cost_d1': v['cost_d1'], 'cost_d2': v['cost_d2'], 'diff': diff})
    out.sort(key=lambda x: abs(x['diff']), reverse=True)
    return out[:_TOP_N]


# ---------------------------------------------------------------------------
# Q9 ~ Q17 대체
# ---------------------------------------------------------------------------

def fetch_daily_totals_ce(ce, d1_date: date, d2_date: date) -> tuple:
    """(d1_total, d2_total, service_rows[Q9]). 총액은 Top N 이 아니라 모든 서비스 합이다."""
    rows = _ce_rows(ce, d2_date, d1_date, 'DAILY', ('SERVICE',))
    d1_total = sum(r['cost'] for r in rows if r['day'] == str(d1_date))
    d2_total = sum(r['cost'] for r in rows if r['day'] == str(d2_date))
    return d1_total, d2_total, _diff_rows(rows, d1_date, d2_date, ('service',))


def fetch_usage_type_diff_ce(ce, d1_date: date, d2_date: date) -> list:
    """Q10 대체."""
    rows = _ce_rows(ce, d2_date, d1_date, 'DAILY', ('SERVICE', 'USAGE_TYPE'))
    return _diff_rows(rows, d1_date, d2_date, ('service', 'usage_type'))


def fetch_top_services_with_breakdown_ce(
    ce, d1_date: date, top_n: int = 5, breakdown_top: int = 5,
) -> list:
    """
    Q14 대체. 어제 Top N 서비스를 먼저 고르고, 서비스마다 한 번씩
    (aws:createdBy × USAGE_TYPE) 로 나눈다. GroupBy 가 2개까지라 서비스는 Filter 로 건다.
    """
    by_svc = defaultdict(float)
    for r in _ce_rows(ce, d1_date, d1_date, 'DAILY', ('SERVICE',)):
        by_svc[r['keys'][0]] += r['cost']
    top = sorted(((s, c) for s, c in by_svc.items() if c > 0.01),
                 key=lambda x: x[1], reverse=True)[:top_n]

    result = []
    for rank, (svc, svc_cost) in enumerate(top, 1):
        agg = defaultdict(float)
        for r in _ce_rows(ce, d1_date, d1_date, 'DAILY',
                          (_TAG_CREATED_BY, 'USAGE_TYPE'), service=svc):
            if r['cost'] <= 0.01:
                continue
            iam_user = _parse_iam_user(r['keys'][0])
            usage_type = r['keys'][1]
            agg[(iam_user, _humanize_usage_type(usage_type), usage_type)] += r['cost']
        breakdowns = [
            {
                'iam_user':    iu,
                'usage_human': uh,
                'usage_type':  ut,
                'cost_d1':     cost * _TAX_MULTIPLIER,
                'count':       1,     # CE 는 resource 수를 모른다
                'usage_hours': 0.0,   # count 를 모르면 "1대 평균" 을 낼 수 없다
            }
            for (iu, uh, ut), cost in agg.items()
        ]
        breakdowns.sort(key=lambda x: x['cost_d1'], reverse=True)
        result.append({
            'service':    svc,
            'cost_d1':    svc_cost * _TAX_MULTIPLIER,
            'rank':       rank,
            'breakdowns': breakdowns[:breakdown_top],
        })
    return result


def fetch_month_new_costs_ce(ce, d1_date: date) -> list:
    """
    Q15 대체. 판단 단위가 (service, usage_type) 이다 (CUR 은 IAM User 까지).
    iam_user 는 빈 문자열이라 "태그 없는 ..." 으로 표기된다.
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start > d1_date - timedelta(days=1):
        return []

    acc = defaultdict(lambda: {'cost_d1': 0.0, 'prior_cost': 0.0})
    for r in _ce_rows(ce, mtd_start, d1_date, 'DAILY', ('SERVICE', 'USAGE_TYPE')):
        col = 'cost_d1' if r['day'] == str(d1_date) else 'prior_cost'
        acc[tuple(r['keys'])][col] += r['cost']

    result = []
    for (svc, ut), v in acc.items():
        # 임계값은 CUR 쿼리처럼 세전 기준으로 판단한다.
        if v['prior_cost'] >= _NEW_COST_PRIOR_CUT or v['cost_d1'] < _NEW_COST_THRESHOLD:
            continue
        result.append({
            'service':     svc,
            'usage_type':  ut,
            'iam_user':    '',
            'usage_human': _humanize_usage_type(ut),
            'cost_d1':     v['cost_d1'] * _TAX_MULTIPLIER,
            'prior_cost':  v['prior_cost'] * _TAX_MULTIPLIER,
            'count':       1,
        })
    result.sort(key=lambda x: x['cost_d1'], reverse=True)
    return result[:_TOP_N]


def fetch_service_mtd_ce(ce, d1_date: date) -> tuple:
    """
    (mtd_total, service_mtd[Q16]). 기간은 이번 달 1일 ~ d1 (d1 포함).
    mtd_total 은 세전, service_mtd 는 CUR 쪽과 같이 ×_TAX_MULTIPLIER.
    """
    by_svc = defaultdict(float)
    for r in _ce_rows(ce, d1_date.replace(day=1), d1_date, 'MONTHLY', ('SERVICE',)):
        by_svc[r['keys'][0]] += r['cost']
    mtd_total = sum(by_svc.values())
    service_mtd = {s: c * _TAX_MULTIPLIER for s, c in by_svc.items() if c > 0.01}
    return mtd_total, service_mtd


def fetch_mtd_top_users_with_breakdown_ce(
    ce, d1_date: date, top_n: int = 5, breakdown_top: int = 5,
) -> list:
    """
    Q17 대체. (aws:createdBy × SERVICE) 로 나눈다. usage_type 축은 없다.
    태그 없는 비용은 사용자를 정할 수 없으므로 뺀다 (CUR 쪽도 빈 iam_user 는 뺀다).
    """
    acc = defaultdict(lambda: defaultdict(float))
    for r in _ce_rows(ce, d1_date.replace(day=1), d1_date, 'MONTHLY',
                      (_TAG_CREATED_BY, 'SERVICE')):
        user = _parse_iam_user(r['keys'][0])
        if user:
            acc[user][r['keys'][1]] += r['cost']

    users = sorted(
        ((u, sum(svcs.values()), svcs) for u, svcs in acc.items()),
        key=lambda x: x[1], reverse=True,
    )
    result = []
    for rank, (user, total, svcs) in enumerate(
            [u for u in users if u[1] > 0.1][:top_n], 1):
        breakdowns = [
            {'service': s, 'usage_type': '', 'usage_human': '',
             'cost': c * _TAX_MULTIPLIER, 'count': 1}
            for s, c in svcs.items() if c > 0.1
        ]
        breakdowns.sort(key=lambda x: x['cost'], reverse=True)
        result.append({
            'iam_user':   user,
            'mtd_total':  total * _TAX_MULTIPLIER,
            'rank':       rank,
            'breakdowns': breakdowns[:breakdown_top],
        })
    return result


# ---------------------------------------------------------------------------
# 일괄 수집
# ---------------------------------------------------------------------------

def collect_raw_ce(ce, d1_date: date, top_n: int, breakdown_top: int) -> dict:
    """
    analysis.collect_all 의 CUR 수집 단계와 같은 키를 CE 로 채운다.
    forecast / LLM 요약 / Slack 섹션 렌더링은 collect_all 이 공통으로 한다.
    """
    d2_date = d1_date - timedelta(days=1)
    d1_total, d2_total, service_rows = fetch_daily_totals_ce(ce, d1_date, d2_date)
    mtd_total, service_mtd = fetch_service_mtd_ce(ce, d1_date)
    return {
        'd1_total':        d1_total,
        'd2_total':        d2_total,
        'service_rows':    service_rows,
        'usage_type_rows': fetch_usage_type_diff_ce(ce, d1_date, d2_date),
        'resource_rows':   [],
        'top_services':    fetch_top_services_with_breakdown_ce(
            ce, d1_date, top_n=top_n, breakdown_top=breakdown_top),
        'new_costs':       fetch_month_new_costs_ce(ce, d1_date),
        'service_mtd':     service_mtd,
        'top_users_mtd':   fetch_mtd_top_users_with_breakdown_ce(
            ce, d1_date, top_n=top_n, breakdown_top=breakdown_top),
        'mtd_total':       mtd_total,
    }
