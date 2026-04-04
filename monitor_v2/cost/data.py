"""
monitor_v2/cost/data.py

Cost Explorer 데이터 수집 모듈.

수집 대상:
    1. 일일 서비스별 비용 (D-1 / D-2)                   → Main 1 요약
    2. 서비스 + aws:createdBy 태그별 비용 (D-1 + MTD)   → Thread 2 (IAM User별)
    3. 서비스 + 리전별 비용 (D-1 + MTD)                  → Thread 3 (서비스 리전 상세)
    4. MTD 총비용                                        → Main 1 헤더 월 누계
    5. 잔여 예측 비용 (전체 합계 + 서비스별)              → Main 1 / Thread 3 예측
"""
from pprint import pprint

import boto3
from datetime import date, timedelta
from calendar import monthrange
import logging

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 날짜 계산 헬퍼
# ---------------------------------------------------------------------------

def _build_day_period(base_date: date, days_ago: int) -> dict:
    """D-N 하루 TimePeriod. End는 exclusive."""
    target = base_date - timedelta(days=days_ago)
    #print("start", target.strftime('%Y-%m-%d'))
    #print("end", (target + timedelta(days=1)).strftime('%Y-%m-%d'))

    return {
        'Start': target.strftime('%Y-%m-%d'),
        'End':   (target + timedelta(days=1)).strftime('%Y-%m-%d'),
    }


def _build_mtd_period(base_date: date) -> dict:
    """당월 1일 ~ base_date (exclusive)."""
    start = base_date.replace(day=1)
    return {
        'Start': start.strftime('%Y-%m-%d'),
        'End':   base_date.strftime('%Y-%m-%d'),
    }



# ---------------------------------------------------------------------------
# API 호출 및 파싱
# ---------------------------------------------------------------------------

def _parse_first_groups(resp: dict) -> list:
    """ResultsByTime[0].Groups 안전 추출."""
    return resp.get('ResultsByTime', [{}])[0].get('Groups', [])


def fetch_daily_by_service(ce, period: dict) -> dict:
    """{service: float}"""
    resp = ce.get_cost_and_usage(
        TimePeriod=period,
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}],
    )
    return {
        g['Keys'][0]: float(g['Metrics']['UnblendedCost']['Amount'])
        for g in _parse_first_groups(resp)
    }


def fetch_daily_by_service_and_creator(ce, period: dict) -> dict:
    """
    SERVICE + aws:createdBy 태그별 비용.

    GroupBy 2개 제한 준수 (DIMENSION + TAG).

    Returns:
        {service: {creator_label: float}}
        미태깅 리소스의 creator_label = '(태그 없음 / 공용)'
    """
    resp = ce.get_cost_and_usage(
        TimePeriod=period,
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
            {'Type': 'TAG',       'Key': 'aws:createdBy'},
        ],
    )
    result = {}
    for group in _parse_first_groups(resp):
        service = group['Keys'][0]
        raw_tag = group['Keys'][1]          # "aws:createdBy$<value>"
        creator = raw_tag.split('$', 1)[1] if '$' in raw_tag else raw_tag
        creator = creator or 'aws:createdBy 태그 없음'
        amount  = float(group['Metrics']['UnblendedCost']['Amount'])
        result.setdefault(service, {})
        result[service][creator] = result[service].get(creator, 0.0) + amount
    return result


def fetch_daily_by_service_and_region(ce, period: dict) -> dict:
    """
    SERVICE + REGION별 비용. EC2 외 서비스 Thread 3용.

    Returns:
        {service: {region: float}}
    """
    resp = ce.get_cost_and_usage(
        TimePeriod=period,
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
            {'Type': 'DIMENSION', 'Key': 'REGION'},
        ],
    )
    result = {}
    for group in _parse_first_groups(resp):
        service = group['Keys'][0]
        region  = group['Keys'][1] or 'global'
        amount  = float(group['Metrics']['UnblendedCost']['Amount'])
        result.setdefault(service, {})
        result[service][region] = result[service].get(region, 0.0) + amount
    return result


def fetch_mtd_by_service_and_creator(ce, period: dict) -> dict:
    """
    MTD: SERVICE + aws:createdBy → {service: {creator: float}}.
    기간이 비면(당월 1일 실행) {} 반환.
    """
    if period['Start'] >= period['End']:
        return {}
    resp = ce.get_cost_and_usage(
        TimePeriod=period,
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
            {'Type': 'TAG',       'Key': 'aws:createdBy'},
        ],
    )
    result = {}
    for group in _parse_first_groups(resp):
        service = group['Keys'][0]
        raw_tag = group['Keys'][1]
        creator = raw_tag.split('$', 1)[1] if '$' in raw_tag else raw_tag
        creator = creator or 'aws:createdBy 태그 없음'
        amount  = float(group['Metrics']['UnblendedCost']['Amount'])
        result.setdefault(service, {})
        result[service][creator] = result[service].get(creator, 0.0) + amount
    return result


def fetch_mtd_by_service_and_region(ce, period: dict) -> dict:
    """
    MTD: SERVICE + REGION → {service: {region: float}}.
    기간이 비면(당월 1일 실행) {} 반환.
    """
    if period['Start'] >= period['End']:
        return {}
    resp = ce.get_cost_and_usage(
        TimePeriod=period,
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
            {'Type': 'DIMENSION', 'Key': 'REGION'},
        ],
    )
    result = {}
    for group in _parse_first_groups(resp):
        service = group['Keys'][0]
        region  = group['Keys'][1] or 'global'
        amount  = float(group['Metrics']['UnblendedCost']['Amount'])
        result.setdefault(service, {})
        result[service][region] = result[service].get(region, 0.0) + amount
    return result



def fetch_cost_forecast(ce, today_kst: date) -> float:
    """
    오늘부터 이달 말일까지 예상 비용 (UNBLENDED_COST, MONTHLY 예측).

    CE 예측 API가 실패하면(데이터 부족 등) 0.0 반환.
    """
    last_day = monthrange(today_kst.year, today_kst.month)[1]
    end_date = date(today_kst.year, today_kst.month, last_day) + timedelta(days=1)

    period = {
        'Start': today_kst.strftime('%Y-%m-%d'),
        'End':   end_date.strftime('%Y-%m-%d'),
    }
    try:
        resp = ce.get_cost_forecast(
            TimePeriod=period,
            Metric='UNBLENDED_COST',
            Granularity='MONTHLY',
            PredictionIntervalLevel=80,
        )
        return float(resp.get('Total', {}).get('Amount', '0'))
    except Exception as exc:
        log.warning("get_cost_forecast 실패 (무시): %s", exc)
        return 0.0


def fetch_mtd_total(ce, period: dict) -> float:
    """
    MTD 기간 총 비용 (GroupBy 없이 전체 합산).

    Start == End (당월 1일에 Lambda 실행) 이면 0 반환.
    """
    if period['Start'] >= period['End']:
        return 0.0
    resp = ce.get_cost_and_usage(
        TimePeriod=period,
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
    )
    total = 0.0
    for item in resp.get('ResultsByTime', []):
        total += float(
            item.get('Total', {}).get('UnblendedCost', {}).get('Amount', '0')
        )
    return total


# ---------------------------------------------------------------------------
# 일괄 수집 (lambda_handler에서 1회 호출)
# ---------------------------------------------------------------------------

def collect_all(today_kst: date) -> dict:
    """
    Main 1 + 스레드 전체에 필요한 Cost Explorer 데이터를 수집한다.

    CE 데이터 지연:
        Cost Explorer는 약 24~48시간 지연이 있어 당일/전일 데이터가 미집계 상태일 수 있다.
        따라서 리포트 기준일(d1_date)은 today_kst - 2일로 고정하고,
        forecast만 현재 날짜(today_kst) 기준으로 조회한다.

    Returns:
        {
            'd1_date':        date,  # 리포트 대상일 (today - 2일)
            'daily_d1':       dict,  # {service: float} d1_date
            'daily_d2':       dict,  # {service: float} d1_date - 1일
            'by_creator':     dict,  # {service: {creator: float}} d1_date
            'by_creator_mtd': dict,  # {service: {creator: float}} MTD (d1_date 기준 월)
            'by_region':      dict,  # {service: {region: float}} d1_date
            'by_region_mtd':  dict,  # {service: {region: float}} MTD (d1_date 기준 월)
            'mtd_this':       float, # d1_date 기준 월 MTD 총비용
            'forecast':       float, # today_kst~말일 잔여 예상 비용 (0.0이면 예측 불가)
        }
        ※ CE forecast API는 GroupBy 미지원 → 서비스별 예측은 MTD 비율로 비례 배분 (report.py)
    """
    ce = boto3.client('ce', region_name='us-east-1')

    # CE 데이터 2일 지연 → 리포트 기준일을 today - 2일로 설정
    d1_date = today_kst - timedelta(days=2)

    period_d1 = _build_day_period(today_kst, days_ago=2)
    period_d2 = _build_day_period(today_kst, days_ago=3)

    # MTD: d1_date 기준 월 1일 ~ d1_date 포함 (End exclusive = d1_date + 1)
    period_mtd_this = {
        'Start': d1_date.replace(day=1).strftime('%Y-%m-%d'),
        'End':   (d1_date + timedelta(days=1)).strftime('%Y-%m-%d'),
    }

    return {
        'd1_date':        d1_date,
        'daily_d1':       fetch_daily_by_service(ce, period_d1),
        'daily_d2':       fetch_daily_by_service(ce, period_d2),
        'by_creator':     fetch_daily_by_service_and_creator(ce, period_d1),
        'by_creator_mtd': fetch_mtd_by_service_and_creator(ce, period_mtd_this),
        'by_region':      fetch_daily_by_service_and_region(ce, period_d1),
        'by_region_mtd':  fetch_mtd_by_service_and_region(ce, period_mtd_this),
        'mtd_this':       fetch_mtd_total(ce, period_mtd_this),
        'forecast':       fetch_cost_forecast(ce, today_kst),
    }
