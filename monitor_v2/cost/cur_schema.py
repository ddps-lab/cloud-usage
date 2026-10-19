"""
monitor_v2/cost/cur_schema.py

cur_logs (Glue) 테이블에 없는 resource_tags_* 컬럼을 CUR manifest 기준으로 찾는다.

배경
    cur_logs 는 crawler 없이 손으로 만든 테이블이다. Billing 콘솔에서 cost
    allocation tag 를 새로 활성화하면 CUR Parquet 에는 resource_tags_* 컬럼이
    생기지만 Glue 테이블 스키마에는 저절로 추가되지 않는다. 그러면
    information_schema 에도 안 보이고, data_cur 의 creator CASE 는 '없는 컬럼'
    으로 보고 그 룰을 조용히 건너뛴다.

    실제 사례 (2026-10): aws:eks:cluster-name / aws:autoscaling:groupName /
    Owner 가 2026-09-10 에 활성화됐지만 테이블은 2026-04 스키마 그대로였다.
    그래서 owner_resolver 의 클러스터 -> IAM User 매핑이 SQL 에 들어가지 못했고,
    EKS 노드그룹이 띄운 인스턴스가 'AutoScaling' 이나
    '<service> - <usage_type>' 으로 표시됐다.

동작
    CUR 은 billing period 마다 <prefix>/<YYYYMMDD-YYYYMMDD>/<report>-Manifest.json
    을 쓰고, 그 안의 columns[] 에 Parquet 컬럼명이 그대로 있다.
    테이블 Location(s3://bucket/<prefix>/<report>) 에서 manifest 경로를 유도해
    최근 N개월 manifest 의 resourceTags 컬럼 중 테이블에 없는 것을 돌려준다.
    추가(ALTER TABLE)는 호출자(data_cur)가 Athena 로 실행한다.

    테이블은 partition projection 을 쓰므로 테이블 스키마만 바꾸면 모든 파티션에
    적용되고, Parquet 은 이름으로 읽으므로 옛 파일에서는 NULL 이 된다.

환경변수
    CUR_SCHEMA_SYNC_DISABLED  '1' 이면 동기화하지 않음

실패 처리
    어떤 실패도 리포트를 막지 않는다. 로그만 남기고 빈 리스트를 반환한다.
"""

import json
import logging
import os
import re
from datetime import date

import boto3

log = logging.getLogger(__name__)

_LOOKBACK_MONTHS = 3          # ec2/data_cur 의 creators 조회 윈도우와 같다
_COLUMN_RE       = re.compile(r'^resource_tags_[a-z0-9_]+$')


def _month_starts(today: date, months: int) -> list:
    """today 가 속한 달부터 거꾸로 months 개의 1일."""
    out, y, m = [], today.year, today.month
    for _ in range(months):
        out.append(date(y, m, 1))
        m -= 1
        if m <= 0:
            m, y = 12, y - 1
    return out


def _next_month(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _manifest_keys(location: str, today: date) -> tuple:
    """
    테이블 Location 에서 (bucket, [manifest key, ...]) 를 유도한다.

    s3://hyu-ddps-cur-reports/cur/hyu-ddps-cur/hyu-ddps-cur
      → bucket 'hyu-ddps-cur-reports'
      → cur/hyu-ddps-cur/20261001-20261101/hyu-ddps-cur-Manifest.json, ...
    """
    if not location.startswith('s3://'):
        raise ValueError(f"테이블 Location 이 s3:// 가 아님: {location}")
    bucket, _, path = location[len('s3://'):].rstrip('/').partition('/')
    prefix, _, report = path.rpartition('/')
    if not report:
        raise ValueError(f"테이블 Location 에서 report 이름을 못 찾음: {location}")

    keys = []
    for start in _month_starts(today, _LOOKBACK_MONTHS):
        period = f"{start:%Y%m%d}-{_next_month(start):%Y%m%d}"
        keys.append('/'.join(p for p in (prefix, period, f"{report}-Manifest.json") if p))
    return bucket, keys


def _manifest_tag_columns(s3, bucket: str, key: str) -> set:
    body = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    columns = json.loads(body.decode('utf-8')).get('columns', [])
    return {
        c['name'] for c in columns
        if c.get('category') == 'resourceTags'
        and _COLUMN_RE.match(c.get('name', ''))
    }


def find_missing_tag_columns(database: str, table: str,
                             available: set, region: str,
                             today: date | None = None) -> list:
    """
    CUR manifest 에는 있고 Glue 테이블에는 없는 resource_tags_* 컬럼 목록 (정렬됨).

    Args:
        available: information_schema 로 읽은 현재 테이블의 resource_tags_* 컬럼
    """
    if os.environ.get('CUR_SCHEMA_SYNC_DISABLED', '') == '1':
        return []

    today = today or date.today()
    try:
        glue = boto3.client('glue', region_name=region)
        location = glue.get_table(DatabaseName=database, Name=table)[
            'Table']['StorageDescriptor']['Location']
        bucket, keys = _manifest_keys(location, today)
    except Exception as e:                            # noqa: BLE001
        log.warning("CUR 스키마 점검 실패 (테이블 조회): %s", e)
        return []

    s3 = boto3.client('s3')
    in_manifest: set = set()
    for key in keys:
        try:
            in_manifest |= _manifest_tag_columns(s3, bucket, key)
        except s3.exceptions.NoSuchKey:
            continue                                  # 아직 없는 달
        except Exception as e:                        # noqa: BLE001
            log.warning("CUR manifest 읽기 실패 s3://%s/%s: %s", bucket, key, e)

    return sorted(in_manifest - set(available))


def add_columns_ddl(database: str, table: str, columns: list) -> str:
    """ALTER TABLE ... ADD COLUMNS DDL. 컬럼명은 _COLUMN_RE 로 검증된 것만 받는다."""
    bad = [c for c in columns if not _COLUMN_RE.match(c)]
    if bad:
        raise ValueError(f"허용되지 않는 컬럼명: {bad}")
    cols = ", ".join(f"`{c}` string" for c in columns)
    return f"ALTER TABLE `{database}`.`{table}` ADD COLUMNS ({cols})"
