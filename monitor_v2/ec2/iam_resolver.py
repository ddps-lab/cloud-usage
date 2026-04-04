"""
monitor_v2/ec2/iam_resolver.py

CloudTrail lookup_events + RunInstances 이벤트를 통해
인스턴스 ID → IAM username 매핑을 구성한다.

제약:
    - CloudTrail lookup API는 최대 90일까지 조회 가능하나,
      이 시스템은 비용·실용성 고려해 60일 이내만 조회
    - 60일 초과 인스턴스 → 'Unknown (60일 초과)' 반환, DM 발송 불가
    - lookup_events는 리전별로 호출해야 하므로 순차 조회 (인스턴스 수가 많을수록 느림)

IAM → Slack 매핑:
    monitor_v2/iam_to_slack.json 파일에서 우선 로드.
    파일이 없을 경우 환경변수 IAM_SLACK_USER_MAP (JSON 문자열) 폴백.
    둘 다 없으면 DM 발송 스킵, 채널 Thread에만 표시.
"""

import json
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone


_IAM_TO_SLACK_PATH = os.path.join(os.path.dirname(__file__), '..', 'iam_to_slack.json')
try:
    with open(_IAM_TO_SLACK_PATH, encoding='utf-8') as _f:
        IAM_SLACK_MAP: dict = json.load(_f)
except (FileNotFoundError, json.JSONDecodeError):
    _RAW_MAP      = os.environ.get('IAM_SLACK_USER_MAP', '{}')
    IAM_SLACK_MAP = json.loads(_RAW_MAP)

LOOKBACK_DAYS = 60


def _extract_username(user_identity: dict) -> str:
    """
    CloudTrail userIdentity dict에서 식별 가능한 username을 추출한다.

    identity type 별 처리:
        IAMUser     → userIdentity.userName
        AssumedRole → ARN 마지막 슬래시 뒤 (세션명, 보통 이메일)
        Root        → 'root'
        기타        → 'Unknown'
    """
    id_type = user_identity.get('type', '')
    if id_type == 'IAMUser':
        return user_identity.get('userName', 'Unknown')
    if id_type == 'AssumedRole':
        arn = user_identity.get('arn', '')
        return arn.split('/')[-1] if '/' in arn else arn
    if id_type == 'Root':
        return 'root'
    return 'Unknown'


def build_instance_creator_map(instance_ids: list, regions: list) -> dict:
    """
    여러 리전의 CloudTrail에서 RunInstances 이벤트를 조회해
    {instance_id: iam_username} 매핑을 반환한다.

    Args:
        instance_ids: 조회할 인스턴스 ID 목록
        regions:      인스턴스가 존재하는 리전 목록

    Returns:
        {
            'i-0abc123': 'kim',
            'i-0def456': 'park@ddps.cloud',
            'i-0ghi789': 'Unknown (60일 초과)',
        }
    """
    if not instance_ids:
        return {}

    id_set      = set(instance_ids)
    creator_map = {}
    start_time  = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    for region in regions:
        if id_set <= set(creator_map.keys()):
            break

        try:
            ct        = boto3.client('cloudtrail', region_name=region)
            paginator = ct.get_paginator('lookup_events')

            for page in paginator.paginate(
                LookupAttributes=[
                    {'AttributeKey': 'EventName', 'AttributeValue': 'RunInstances'}
                ],
                StartTime=start_time,
            ):
                for event in page.get('Events', []):
                    for resource in event.get('Resources', []):
                        rid = resource.get('ResourceName', '')
                        if rid not in id_set or rid in creator_map:
                            continue

                        try:
                            detail  = json.loads(event.get('CloudTrailEvent', '{}'))
                            user_id = detail.get('userIdentity', {})
                            creator_map[rid] = _extract_username(user_id)
                        except (json.JSONDecodeError, KeyError):
                            creator_map[rid] = 'Unknown'

        except ClientError:
            continue

    for iid in id_set:
        if iid not in creator_map:
            creator_map[iid] = 'Unknown (60일 초과)'

    return creator_map


def get_slack_user_id(iam_username: str) -> str | None:
    """
    IAM username → Slack User ID 변환.

    assumed-role ARN의 경우 마지막 슬래시 뒤 세션명으로 조회.
    매핑 없으면 None 반환 (DM 발송 스킵).

    Args:
        iam_username: build_instance_creator_map 반환값

    Returns:
        'U01234567' 형식의 Slack User ID 또는 None
    """
    if not iam_username or iam_username.startswith('Unknown'):
        return None

    print("iam_username", iam_username)
    clean = iam_username.split('/')[-1]
    return IAM_SLACK_MAP.get(clean) or IAM_SLACK_MAP.get(iam_username)
