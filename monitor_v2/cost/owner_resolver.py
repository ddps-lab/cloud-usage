"""
monitor_v2/cost/owner_resolver.py

CloudTrail 이벤트로 '클러스터 / ASG → 생성 IAM User' 매핑을 자동 구축한다.

배경
    aws:createdBy 태그는 <principal-type>:<unique-id>:<friendly-name> 형식이고
    3번째 필드의 의미가 principal-type 에 따라 달라진다.
        IAMUser:AIDA...:mhsong          → 'mhsong'      (사람)
        AssumedRole:AROA...:AutoScaling → 'AutoScaling' (역할 세션명 — 사람 아님)

    ASG / EKS 노드그룹이 만든 인스턴스는 후자라 태그만으로는 사람을 특정할 수
    없다. 반면 AWS 가 사람 개입 없이 자동으로 붙이는 아래 두 태그는 항상 있다.
        aws:eks:cluster-name       (EKS 워커 노드)
        aws:autoscaling:groupName  (ASG 가 띄운 인스턴스)

    이 모듈은 CloudTrail 에서 그 클러스터 / ASG 를 '만든 사람' 을 찾아
        {클러스터명: IAM User} / {ASG명: IAM User}
    매핑을 만든다. 연구자가 리소스에 태그를 붙이는 협조 없이도 IAM User 단위
    귀속이 가능해진다.

수집 이벤트
    eks:CreateCluster                    requestParameters.name
    eks:CreateNodegroup                  requestParameters.name  (클러스터명)
    autoscaling:CreateAutoScalingGroup   requestParameters.autoScalingGroupName

    ※ EKS 가 노드그룹용으로 만드는 ASG(eks-<cluster>-<ng>-<uuid>) 의 생성자는
      EKS 서비스 연결 역할이므로 사람이 아니다. 그런 항목은 IAMUser 필터에서
      자동 제외되고, 대신 클러스터 매핑이 그 비용을 담당한다.
    ※ 같은 이름이 여러 번 생성된 경우 eventTime 이 가장 최근인 생성자를 쓴다.

캐시
    lookup_events 는 느리고 90일 제한이 있으므로 결과를 S3 에 누적 저장한다.
    매 실행은 '마지막 스캔 시각 - 1일' 이후만 증분 조회하고 기존 매핑과
    병합한다(삭제 없음). 따라서 한 번 잡은 매핑은 90일이 지나도 유지된다.

환경변수
    OWNER_MAP_S3_URI         캐시 위치(s3://bucket/key). 미지정 시
                             ATHENA_OUTPUT_LOCATION 버킷의
                             monitor_v2/owner_map.json 을 사용
    OWNER_MAP_REGIONS        스캔 리전(콤마). 기본 'us-west-2,us-east-1,ap-northeast-2'
    OWNER_MAP_LOOKBACK_DAYS  캐시가 없을 때 초기 스캔 일수 (기본 90, 상한 90)
    OWNER_MAP_DISABLED       '1' 이면 빈 매핑을 반환 (기능 off)

실패 처리
    이 모듈의 어떤 실패도 리포트를 막지 않는다. 예외는 로그만 남기고 빈 매핑을
    반환하므로 기존 태그 fallback 체인이 그대로 동작한다.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

_DEFAULT_REGIONS   = ('us-west-2', 'us-east-1', 'ap-northeast-2')
_DEFAULT_CACHE_KEY = 'monitor_v2/owner_map.json'
_MAX_LOOKBACK_DAYS = 90          # CloudTrail lookup_events 보존 한계
_SCAN_OVERLAP      = timedelta(days=1)

# (EventName, 기대 eventSource, 저장 버킷, requestParameters 의 이름 필드)
_EVENT_SPECS = (
    ('CreateCluster',          'eks.amazonaws.com',         'clusters', 'name'),
    ('CreateNodegroup',        'eks.amazonaws.com',         'clusters', 'name'),
    ('CreateAutoScalingGroup', 'autoscaling.amazonaws.com', 'asgs',
                                                            'autoScalingGroupName'),
)

_EMPTY_CACHE = {'version': 1, 'scanned_until': None, 'clusters': {}, 'asgs': {}}

# 모듈 전역 캐시 — 콜드스타트당 1회만 CloudTrail 을 조회한다.
_OWNER_MAPS: tuple | None = None


# ---------------------------------------------------------------------------
# 설정 헬퍼
# ---------------------------------------------------------------------------

def _regions() -> list:
    raw = os.environ.get('OWNER_MAP_REGIONS', '')
    regions = [r.strip() for r in raw.split(',') if r.strip()]
    return regions or list(_DEFAULT_REGIONS)


def _cache_location() -> tuple | None:
    """(bucket, key) 반환. 위치를 못 정하면 None (캐시 없이 동작)."""
    uri = os.environ.get('OWNER_MAP_S3_URI', '').strip()
    if not uri:
        athena_out = os.environ.get('ATHENA_OUTPUT_LOCATION', '').strip()
        if not athena_out.startswith('s3://'):
            return None
        bucket = athena_out[len('s3://'):].split('/', 1)[0]
        return bucket, _DEFAULT_CACHE_KEY
    if not uri.startswith('s3://'):
        log.warning("OWNER_MAP_S3_URI 형식 오류(s3:// 아님): %s", uri)
        return None
    rest = uri[len('s3://'):]
    bucket, _, key = rest.partition('/')
    return bucket, (key or _DEFAULT_CACHE_KEY)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_iso(text) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.strptime(text, '%Y-%m-%dT%H:%M:%SZ').replace(
            tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# CloudTrail 스캔
# ---------------------------------------------------------------------------

def _extract_iam_user(user_identity: dict) -> str | None:
    """
    CloudTrail userIdentity 에서 '사람' 이름만 뽑는다.

    IAMUser → userName, Root → 'root'.
    AssumedRole / AWSService 등은 사람이 아니므로 None (매핑에서 제외).
    """
    id_type = (user_identity or {}).get('type', '')
    if id_type == 'IAMUser':
        return (user_identity.get('userName') or '').strip() or None
    if id_type == 'Root':
        return 'root'
    return None


def _scan_cloudtrail(regions: list, start_time: datetime) -> dict:
    """
    지정 리전들의 CloudTrail 에서 생성 이벤트를 읽어 매핑 후보를 만든다.

    Returns:
        {'clusters': {name: entry}, 'asgs': {name: entry}}
        entry = {'owner', 'event_time', 'source', 'region'}
        리전 단위 실패는 건너뛰고 나머지를 계속 수집한다.
    """
    found = {'clusters': {}, 'asgs': {}}

    for region in regions:
        try:
            ct = boto3.client('cloudtrail', region_name=region)
        except Exception as e:                        # noqa: BLE001
            log.warning("CloudTrail 클라이언트 생성 실패 %s: %s", region, e)
            continue

        for event_name, expect_source, bucket, name_field in _EVENT_SPECS:
            try:
                pages = ct.get_paginator('lookup_events').paginate(
                    LookupAttributes=[{
                        'AttributeKey':   'EventName',
                        'AttributeValue': event_name,
                    }],
                    StartTime=start_time,
                )
                for page in pages:
                    for event in page.get('Events', []):
                        _absorb_event(found, event, expect_source,
                                      bucket, name_field, event_name, region)
            except ClientError as e:
                log.warning("lookup_events 실패 %s/%s: %s",
                            region, event_name, e)
            except Exception as e:                    # noqa: BLE001
                log.warning("lookup_events 예외 %s/%s: %s",
                            region, event_name, e)

    return found


def _absorb_event(found: dict, event: dict, expect_source: str,
                  bucket: str, name_field: str,
                  event_name: str, region: str) -> None:
    """단일 CloudTrail 이벤트를 매핑 후보에 반영 (최신 eventTime 우선)."""
    try:
        detail = json.loads(event.get('CloudTrailEvent', '{}'))
    except (json.JSONDecodeError, TypeError):
        return

    # CreateCluster 는 ECS/Redshift/DAX 등에도 있는 이름이라 source 로 구분한다.
    if detail.get('eventSource') != expect_source:
        return

    owner = _extract_iam_user(detail.get('userIdentity', {}))
    if not owner:
        return                                        # 사람이 아니면 제외

    name = ((detail.get('requestParameters') or {}).get(name_field) or '').strip()
    if not name:
        return

    event_time = detail.get('eventTime') or ''
    current    = found[bucket].get(name)
    if current and current.get('event_time', '') >= event_time:
        return                                        # 더 최신 기록 유지

    found[bucket][name] = {
        'owner':      owner,
        'event_time': event_time,
        'source':     event_name,
        'region':     region,
    }


# ---------------------------------------------------------------------------
# S3 캐시
# ---------------------------------------------------------------------------

def _load_cache() -> dict:
    location = _cache_location()
    if location is None:
        return dict(_EMPTY_CACHE, clusters={}, asgs={})
    bucket, key = location
    try:
        body = boto3.client('s3').get_object(Bucket=bucket, Key=key)['Body']
        cache = json.loads(body.read().decode('utf-8'))
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code not in ('NoSuchKey', '404', 'NoSuchBucket', 'AccessDenied'):
            log.warning("owner_map 캐시 읽기 실패: %s", e)
        return dict(_EMPTY_CACHE, clusters={}, asgs={})
    except Exception as e:                            # noqa: BLE001
        log.warning("owner_map 캐시 파싱 실패: %s", e)
        return dict(_EMPTY_CACHE, clusters={}, asgs={})

    if not isinstance(cache, dict):
        return dict(_EMPTY_CACHE, clusters={}, asgs={})
    cache.setdefault('version', 1)
    cache.setdefault('scanned_until', None)
    for bucket_name in ('clusters', 'asgs'):
        if not isinstance(cache.get(bucket_name), dict):
            cache[bucket_name] = {}
    return cache


def _save_cache(cache: dict) -> None:
    location = _cache_location()
    if location is None:
        log.info("owner_map 캐시 위치 미설정 → 저장 생략")
        return
    bucket, key = location
    try:
        boto3.client('s3').put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(cache, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json',
        )
    except Exception as e:                            # noqa: BLE001
        log.warning("owner_map 캐시 저장 실패 (동작에는 영향 없음): %s", e)


def _merge(cache: dict, found: dict) -> dict:
    """스캔 결과를 캐시에 병합. 삭제는 하지 않고 최신 eventTime 만 갱신한다."""
    for bucket_name in ('clusters', 'asgs'):
        target = cache.setdefault(bucket_name, {})
        for name, entry in found.get(bucket_name, {}).items():
            current = target.get(name)
            if current and current.get('event_time', '') >= entry.get('event_time', ''):
                continue
            target[name] = entry
    return cache


# ---------------------------------------------------------------------------
# 공개 API
# ---------------------------------------------------------------------------

def refresh_owner_map() -> dict:
    """
    캐시를 읽어 증분 스캔하고 병합 결과를 저장한 뒤 반환한다.

    Returns:
        {'version', 'scanned_until', 'clusters', 'asgs'} 형태의 캐시 dict
    """
    cache    = _load_cache()
    now      = _now()
    scanned  = _parse_iso(cache.get('scanned_until'))
    lookback = _MAX_LOOKBACK_DAYS
    try:
        lookback = min(int(os.environ.get('OWNER_MAP_LOOKBACK_DAYS',
                                          _MAX_LOOKBACK_DAYS)),
                       _MAX_LOOKBACK_DAYS)
    except ValueError:
        pass

    floor = now - timedelta(days=lookback)
    start = max(scanned - _SCAN_OVERLAP, floor) if scanned else floor

    regions = _regions()
    log.info("owner_map 스캔: regions=%s start=%s (캐시 %d/%d건)",
             regions, _iso(start),
             len(cache.get('clusters', {})), len(cache.get('asgs', {})))

    found = _scan_cloudtrail(regions, start)
    cache = _merge(cache, found)
    cache['scanned_until'] = _iso(now)
    cache['updated_at']    = _iso(now)
    _save_cache(cache)

    log.info("owner_map 갱신 완료: clusters=%d asgs=%d (신규/갱신 %d/%d)",
             len(cache['clusters']), len(cache['asgs']),
             len(found['clusters']), len(found['asgs']))
    return cache


def get_owner_maps() -> tuple:
    """
    ({클러스터명: IAM User}, {ASG명: IAM User}) 반환. 모듈 전역 캐시.

    어떤 실패든 ({}, {}) 를 반환하므로 호출자는 예외를 처리할 필요가 없다.
    """
    global _OWNER_MAPS
    if _OWNER_MAPS is not None:
        return _OWNER_MAPS

    if os.environ.get('OWNER_MAP_DISABLED', '') == '1':
        log.info("OWNER_MAP_DISABLED=1 → 매핑 비활성화")
        _OWNER_MAPS = ({}, {})
        return _OWNER_MAPS

    try:
        cache = refresh_owner_map()
        clusters = {n: e['owner'] for n, e in cache.get('clusters', {}).items()
                    if isinstance(e, dict) and e.get('owner')}
        asgs     = {n: e['owner'] for n, e in cache.get('asgs', {}).items()
                    if isinstance(e, dict) and e.get('owner')}
        _OWNER_MAPS = (clusters, asgs)
    except Exception as e:                            # noqa: BLE001
        log.warning("owner_map 구축 실패 → 매핑 없이 진행: %s", e)
        _OWNER_MAPS = ({}, {})
    return _OWNER_MAPS


def reset_cache() -> None:
    """모듈 전역 캐시 초기화 (테스트용)."""
    global _OWNER_MAPS
    _OWNER_MAPS = None
