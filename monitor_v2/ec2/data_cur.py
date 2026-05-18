"""
monitor_v2/ec2/data_cur.py

EC2 데이터 수집 모듈 (Athena CUR 버전).

data.py 대비 변경점:
    - collect_ec2_cost_by_type : CE API → Athena CUR 쿼리로 교체
    - collect_instances / collect_unused_ebs / collect_unused_snapshots : 기존 data.py 재사용
    - 자원별 iam_user 는 CUR(resource_tags_aws_created_by 등) 로 보강
      → aws:createdBy 는 EC2 API Tags 에 노출되지 않는 system tag 이므로
        data.py 의 tag 기반 접근만으로는 항상 '' 가 됨. collect_all() 내부에서
        collect_resource_creators_cur() 결과를 주입해 보강한다.

collect_all() 반환 구조는 data.py 와 동일 → ec2/report.py 그대로 사용 가능.

Athena 쿼리:
    product_instance_type  ↔ CE INSTANCE_TYPE dimension
    product_region_code    ↔ CE REGION dimension
    product_instance_type != ''  ↔ CE 의 NoInstanceType 제외 조건
"""

import boto3
from botocore.exceptions import ClientError
from datetime import date, timedelta, datetime, timezone
import logging

from .data import (
    collect_instances,
    collect_unused_ebs,
    collect_unused_snapshots,
)
from ..cost.data_cur import (
    _run_query, _partition, _ATHENA_DATABASE, _ATHENA_REGION,
    CREATOR_CASE_SQL,
)

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
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
          AND product_instance_type != ''
        GROUP BY
            product_instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global')
        HAVING SUM(line_item_unblended_cost) > 0.01
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
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date)
              BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
          AND product_instance_type != ''
        GROUP BY
            product_instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global')
        HAVING SUM(line_item_unblended_cost) > 0.01
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
            FROM {_ATHENA_DATABASE}.cur_logs
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


def collect_instance_cost_cur(athena, d1_date: date) -> dict:
    """
    D-1 인스턴스 ID별 On-Demand EC2 실 비용 + 실 사용 시간 (Athena CUR, Q12).

    BoxUsage 행만 집계하므로 On-Demand 인스턴스만 포함된다.
    Spot 절감 추정:  usage_hours × describe_spot_price_history 결과

    Returns:
        {
            instance_id: {
                'cost':          float,  # 실 On-Demand 비용 (USD)
                'usage_hours':   float,  # 실 사용 시간 (h)
                'instance_type': str,
                'region':        str,
                'iam_user':      str,    # CUR aws:createdBy → 사용자명
            }
        }
    """
    year, month = _partition(d1_date)
    sql = f"""
        SELECT
            line_item_resource_id                                               AS instance_id,
            product_instance_type                                               AS instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global')                AS region,
            SPLIT_PART(COALESCE(resource_tags_aws_created_by, ''), ':', 3)     AS iam_user,
            SUM(line_item_usage_amount)                                         AS usage_hours,
            SUM(line_item_unblended_cost)                                       AS cost
        FROM {_ATHENA_DATABASE}.cur_logs
        WHERE year  = '{year}'
          AND month = '{month}'
          AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
          AND line_item_resource_id LIKE 'i-%'
          AND product_instance_type != ''
          AND line_item_usage_type  LIKE '%BoxUsage%'
        GROUP BY
            line_item_resource_id,
            product_instance_type,
            COALESCE(NULLIF(product_region_code, ''), 'global'),
            SPLIT_PART(COALESCE(resource_tags_aws_created_by, ''), ':', 3)
        HAVING SUM(line_item_unblended_cost) > 0
    """
    rows   = _run_query(athena, sql)
    result = {}
    for r in rows:
        iid = r.get('instance_id', '')
        if not iid:
            continue
        result[iid] = {
            'cost':          float(r.get('cost', 0) or 0),
            'usage_hours':   float(r.get('usage_hours', 0) or 0),
            'instance_type': r.get('instance_type', ''),
            'region':        r.get('region') or 'global',
            'iam_user':      r.get('iam_user', ''),
        }
    return result


def collect_spot_prices(regions: list, instance_types: list) -> dict:
    """
    D-1 기준 인스턴스 타입별 Spot 시간당 단가 (리전 내 AZ 평균).

    EC2 describe_spot_price_history API 사용 (무료).
    리전 내 여러 AZ의 가격을 평균 내어 리전 대표 단가로 사용한다.

    Args:
        regions:        조회할 리전 리스트 (On-Demand 인스턴스가 있는 리전)
        instance_types: 조회할 인스턴스 타입 리스트

    Returns:
        {instance_type: {region: float}}  ← 시간당 USD 평균
    """
    if not instance_types or not regions:
        return {}

    now      = datetime.now(timezone.utc)
    d1_end   = now.replace(hour=0, minute=0, second=0, microsecond=0)
    d1_start = d1_end - timedelta(days=1)

    result: dict = {}
    for region in regions:
        if region == 'global':
            continue
        try:
            ec2  = boto3.client('ec2', region_name=region)
            resp = ec2.describe_spot_price_history(
                StartTime=d1_start,
                EndTime=d1_end,
                InstanceTypes=instance_types,
                ProductDescriptions=['Linux/UNIX'],
            )
            by_type: dict = {}
            for item in resp.get('SpotPriceHistory', []):
                itype = item['InstanceType']
                price = float(item['SpotPrice'])
                by_type.setdefault(itype, []).append(price)

            for itype, prices in by_type.items():
                result.setdefault(itype, {})
                result[itype][region] = sum(prices) / len(prices)

        except ClientError:
            continue

    return result


def collect_resource_creators_cur(athena, d1_date: date) -> dict:
    """
    CUR(Athena) 에서 EC2 인스턴스 / EBS 볼륨 / Snapshot 의 생성자(IAM User)를 일괄 조회.

    배경:
        aws:createdBy 는 EC2/EBS API 응답의 Tags 에 노출되지 않는 system tag.
        Athena CUR 의 resource_tags_aws_created_by 컬럼에서만 접근 가능하다.
        data.py 의 tag 기반 접근으로는 항상 '' 만 반환되므로 본 함수로 보강한다.

    조회 윈도우:
        현재 월 + 직전 2개월 (총 3개월). stopped 인스턴스는 stop 이후 line_item 이
        생성되지 않으므로 충분히 거슬러 올라가야 마지막 활성기의 row 에서 태그 회수가
        가능하다. (CloudTrail lookup_events 의 ~90일 한계와 유사한 범위)

    선택 규칙 (동일 resource_id 에 여러 row 가 있을 경우):
        1순위 — resource_tags_aws_created_by 가 실제로 채워져 있는 row
        2순위 — 그 중 가장 최근 row

    Returns:
        {
            'i-0aaa...':  'jglee',
            'vol-0bb..':  '[Lambda] my-fn',
            'snap-0cc.':  '[EKS] cluster-x/ng-1',
            ...
        }
        CUR 에서 찾지 못한 resource_id 는 dict 에 포함되지 않음
        (호출 측에서 빈 문자열 fallback 처리).
    """
    months = []
    y, m = d1_date.year, d1_date.month
    for _ in range(3):
        months.append((str(y), str(m)))
        m -= 1
        if m <= 0:
            m = 12
            y -= 1

    where_partitions = " OR ".join(
        f"(year = '{yy}' AND month = '{mm}')" for yy, mm in months
    )

    sql = f"""
        WITH base AS (
            SELECT
                line_item_resource_id      AS resource_id,
                {CREATOR_CASE_SQL}         AS creator,
                CASE WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
                     THEN 0 ELSE 1 END     AS priority,
                line_item_usage_start_date AS ts
            FROM {_ATHENA_DATABASE}.cur_logs
            WHERE ({where_partitions})
              AND line_item_resource_id IS NOT NULL
              AND line_item_resource_id != ''
              AND (
                    line_item_resource_id LIKE 'i-%'
                 OR line_item_resource_id LIKE 'vol-%'
                 OR line_item_resource_id LIKE 'snap-%'
              )
        ),
        ranked AS (
            SELECT
                resource_id,
                creator,
                ROW_NUMBER() OVER (
                    PARTITION BY resource_id
                    ORDER BY priority ASC, ts DESC
                ) AS rn
            FROM base
        )
        SELECT resource_id, creator
        FROM ranked
        WHERE rn = 1
    """

    rows = _run_query(athena, sql)
    return {
        r['resource_id']: r.get('creator', '')
        for r in rows
        if r.get('resource_id')
    }


def _inject_creators(
    instances: dict, unused_ebs: list, unused_snapshots: list, creators: dict,
) -> None:
    """수집된 자원들의 iam_user 필드를 CUR creators 맵으로 in-place 덮어쓴다."""
    for region_instances in instances.values():
        for inst in region_instances:
            c = creators.get(inst.get('instance_id'))
            if c:
                inst['iam_user'] = c
    for vol in unused_ebs:
        c = creators.get(vol.get('volume_id'))
        if c:
            vol['iam_user'] = c
    for snap in unused_snapshots:
        c = creators.get(snap.get('snapshot_id'))
        if c:
            snap['iam_user'] = c


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
            'instance_cost':    dict,  # {instance_id: {'cost','usage_hours','instance_type','region','iam_user'}}
            'spot_prices':      dict,  # {instance_type: {region: float}}  시간당 Spot 단가
        }
    """
    athena = boto3.client('athena', region_name=_ATHENA_REGION)
    spot_d1, spot_d2, spot_mtd = collect_spot_cost_cur(athena, d1_date)

    instance_cost = collect_instance_cost_cur(athena, d1_date)

    # On-Demand 인스턴스 타입·리전 목록 추출 → Spot 가격 조회 대상
    od_types   = list({v['instance_type'] for v in instance_cost.values() if v['instance_type']})
    od_regions = list({v['region']        for v in instance_cost.values() if v['region'] != 'global'})
    spot_prices = collect_spot_prices(od_regions, od_types)

    # 자원 수집 — data.py 의 iam_user 는 EC2 API Tags 기반이라 항상 '' 다.
    instances        = collect_instances(regions)
    unused_ebs       = collect_unused_ebs(regions)
    unused_snapshots = collect_unused_snapshots(regions, account_id)

    # CUR 기반 creator 주입 — aws:createdBy 는 CUR 에서만 권위 있음.
    # 실패해도 EC2 리포트 전체를 죽이지 않도록 try/except 보호 (빈 iam_user 폴백).
    try:
        creators = collect_resource_creators_cur(athena, d1_date)
        _inject_creators(instances, unused_ebs, unused_snapshots, creators)
    except Exception as exc:
        log.warning("CUR creators lookup 실패 (iam_user 미보강): %s", exc)

    return {
        'instances':        instances,
        'unused_ebs':       unused_ebs,
        'unused_snapshots': unused_snapshots,
        'type_cost':        collect_ec2_cost_by_type_cur(athena, d1_date),
        'type_cost_mtd':    collect_ec2_cost_by_type_mtd_cur(athena, d1_date),
        'spot_d1':          spot_d1,
        'spot_d2':          spot_d2,
        'spot_mtd':         spot_mtd,
        'instance_cost':    instance_cost,
        'spot_prices':      spot_prices,
    }
