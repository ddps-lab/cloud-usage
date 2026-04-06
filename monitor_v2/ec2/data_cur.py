"""
monitor_v2/ec2/data_cur.py

EC2 데이터 수집 모듈 (Athena CUR 버전).

data.py 대비 변경점:
    - collect_ec2_cost_by_type : CE API → Athena CUR 쿼리로 교체
    - collect_instances / collect_unused_ebs / collect_unused_snapshots : 기존 data.py 재사용

collect_all() 반환 구조는 data.py 와 동일 → ec2/report.py 그대로 사용 가능.

Athena 쿼리:
    product_instance_type  ↔ CE INSTANCE_TYPE dimension
    product_region_code    ↔ CE REGION dimension
    product_instance_type != ''  ↔ CE 의 NoInstanceType 제외 조건
"""

import boto3
from datetime import date, timedelta
import logging

from .data import (
    collect_instances,
    collect_unused_ebs,
    collect_unused_snapshots,
)
from ..cost.data_cur import _run_query, _partition

log = logging.getLogger(__name__)


def collect_ec2_cost_by_type_cur(athena, d1_date: date) -> dict:
    """
    D-1 EC2 인스턴스 타입 + 리전별 비용 (Athena CUR).

    CE API의 GroupBy=[INSTANCE_TYPE, REGION] + NoInstanceType 제외와 동일한 결과.

    Returns:
        {instance_type: {region: float}}
    """
    year, month = _partition(d1_date)
    sql = f"""
        SELECT
            product_instance_type                                           AS instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global')            AS region,
            SUM(line_item_unblended_cost)                                   AS cost
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
          AND product_instance_type != ''
        GROUP BY
            product_instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global')
        HAVING SUM(line_item_unblended_cost) > 0
        ORDER BY cost DESC
    """
    rows   = _run_query(athena, sql)
    result = {}
    for r in rows:
        itype  = r.get('instance_type', '')
        region = r.get('region') or 'global'
        cost   = float(r.get('cost', 0) or 0)
        result.setdefault(itype, {})
        result[itype][region] = result[itype].get(region, 0.0) + cost
    return result


def collect_ec2_cost_by_type_mtd_cur(athena, d1_date: date) -> dict:
    """
    MTD EC2 인스턴스 타입 + 리전별 비용 (Athena CUR).

    Returns:
        {instance_type: {region: float}}
    """
    mtd_start = d1_date.replace(day=1)
    if mtd_start >= d1_date:
        return {}
    year, month = _partition(d1_date)
    sql = f"""
        SELECT
            product_instance_type                                           AS instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global')            AS region,
            SUM(line_item_unblended_cost)                                   AS cost
        FROM hyu_ddps_logs.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
          AND product_instance_type != ''
        GROUP BY
            product_instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global')
        HAVING SUM(line_item_unblended_cost) > 0
        ORDER BY cost DESC
    """
    rows   = _run_query(athena, sql)
    result = {}
    for r in rows:
        itype  = r.get('instance_type', '')
        region = r.get('region') or 'global'
        cost   = float(r.get('cost', 0) or 0)
        result.setdefault(itype, {})
        result[itype][region] = result[itype].get(region, 0.0) + cost
    return result


def collect_spot_cost_cur(athena, d1_date: date) -> tuple:
    """
    D-1 / D-2 / MTD Spot EC2 비용 합산 (Athena CUR).

    Spot 인스턴스는 line_item_usage_type에 'SpotUsage'가 포함된 행으로 식별한다.

    Returns:
        (spot_d1: float, spot_d2: float, spot_mtd: float)
    """
    d2_date   = d1_date - timedelta(days=1)
    mtd_start = d1_date.replace(day=1)

    def _query_spot(start: date, end: date) -> float:
        y, m = _partition(end)
        sql = f"""
            SELECT SUM(line_item_unblended_cost) AS spot_cost
            FROM hyu_ddps_logs.cur_logs
            WHERE year  = '{y}'
              AND month = '{m}'
              AND DATE(line_item_usage_start_date)
                  BETWEEN DATE('{start}') AND DATE('{end}')
              AND line_item_usage_type LIKE '%SpotUsage%'
        """
        rows = _run_query(athena, sql)
        return float(rows[0].get('spot_cost', 0) or 0) if rows else 0.0

    spot_d1  = _query_spot(d1_date, d1_date)
    spot_d2  = _query_spot(d2_date, d2_date)
    spot_mtd = _query_spot(mtd_start, d1_date) if mtd_start < d1_date else 0.0

    return spot_d1, spot_d2, spot_mtd


def collect_all(regions: list, account_id: str, d1_date: date) -> dict:
    """
    Main 2 + 스레드에 필요한 EC2 데이터를 수집한다.

    data.py의 collect_all()과 반환 구조 동일.
    EC2 비용 수집만 Athena CUR 로 교체하고 나머지는 EC2 API 그대로 사용.

    Args:
        regions:    describe_regions 로 조회한 리전 리스트
        account_id: STS get_caller_identity Account
        d1_date:    리포트 기준일 (cost/data_cur.collect_all 의 d1_date 와 일치)

    Returns:
        {
            'instances':        dict,  # {region: [inst, ...]}
            'unused_ebs':       list,
            'unused_snapshots': list,
            'type_cost':        dict,  # {itype: {region: float}} D-1
            'type_cost_mtd':    dict,  # {itype: {region: float}} MTD
            'spot_d1':          float, # Spot 당일 비용
            'spot_d2':          float, # Spot 전날 비용
            'spot_mtd':         float, # Spot 당월 누계 비용
        }
    """
    athena = boto3.client('athena', region_name='ap-northeast-2')
    spot_d1, spot_d2, spot_mtd = collect_spot_cost_cur(athena, d1_date)

    return {
        'instances':        collect_instances(regions),
        'unused_ebs':       collect_unused_ebs(regions),
        'unused_snapshots': collect_unused_snapshots(regions, account_id),
        'type_cost':        collect_ec2_cost_by_type_cur(athena, d1_date),
        'type_cost_mtd':    collect_ec2_cost_by_type_mtd_cur(athena, d1_date),
        'spot_d1':          spot_d1,
        'spot_d2':          spot_d2,
        'spot_mtd':         spot_mtd,
    }
