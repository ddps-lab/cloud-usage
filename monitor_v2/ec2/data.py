"""
monitor_v2/ec2/data.py

EC2 인스턴스 및 미사용 리소스 수집 모듈.

수집 대상:
    1. running / stopped / terminated 인스턴스 (모든 리전)
    2. 미사용 EBS 볼륨 (available 상태)
    3. 미사용 Snapshot (AMI 미참조, non-backup)
    4. D-1 기간 EC2 비용 인스턴스 타입 + 리전별

참고:
    - terminated 인스턴스는 종료 후 약 1시간만 describe_instances에서 조회됨.
      D-1 비용 리포트와 실제 인스턴스 목록이 일치하지 않을 수 있음.
    - Spot 인스턴스 식별: InstanceLifecycle == 'spot'
"""

import re
from pprint import pprint

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

_STATE_REASON_RE = re.compile(r'\((\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} GMT)\)')


def _parse_state_transition_time(reason: str):
    """StateTransitionReason 문자열에서 datetime 추출. 파싱 실패 시 None."""
    m = _STATE_REASON_RE.search(reason or '')
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S GMT').replace(tzinfo=timezone.utc)
    except ValueError:
        return None

_QUERY_STATES         = ['running', 'stopped', 'terminated']
DISPLAY_STATES        = {'running', 'stopped', 'terminated'}
EBS_MIN_AGE_HOURS     = 24
K8S_EBS_GRACE_DAYS    = 14
SNAPSHOT_ALERT_DAYS   = 60


# ---------------------------------------------------------------------------
# EC2 인스턴스 수집
# ---------------------------------------------------------------------------

def collect_instances(regions: list) -> dict:
    """
    모든 리전의 EC2 인스턴스를 수집한다.

    Args:
        regions: 조회할 리전 리스트 (session.get_available_regions('ec2'))

    Returns:
        {
            'ap-northeast-2': [
                {
                    'instance_id':            str,
                    'instance_type':          str,
                    'state':                  str,      # 'running' | 'stopped' | 'terminated'
                    'name':                   str,      # Tags의 Name 값, 없으면 ''
                    'launch_time':            datetime | None,
                    'purchase_option':        str,      # 'On-Demand' | 'Spot'
                    'tags':                   dict,     # 모든 태그 {key: value}
                    'iam_user':               str,      # aws:createdBy 태그 값, 없으면 ''
                    'state_transition_time':  datetime | None,  # stop/terminate 시각
                },
                ...
            ],
            ...
        }
        인스턴스가 없는 리전은 포함되지 않음.
    """
    result = {}

    for region in regions:
        try:
            ec2       = boto3.client('ec2', region_name=region)
            paginator = ec2.get_paginator('describe_instances')
            instances = []

            for page in paginator.paginate(
                Filters=[{'Name': 'instance-state-name', 'Values': _QUERY_STATES}]
            ):
                for reservation in page.get('Reservations', []):
                    for inst in reservation.get('Instances', []):
                        state = inst['State']['Name']
                        if state not in DISPLAY_STATES:
                            continue

                        all_tags  = {t['Key']: t['Value'] for t in inst.get('Tags', [])}
                        name      = all_tags.get('Name', '')
                        iam_user  = all_tags.get('aws:createdBy', '')

                        lifecycle = inst.get('InstanceLifecycle', 'normal')
                        purchase  = 'Spot' if lifecycle == 'spot' else 'On-Demand'

                        state_transition_time = _parse_state_transition_time(
                            inst.get('StateTransitionReason', '')
                        )

                        instances.append({
                            'instance_id':           inst['InstanceId'],
                            'instance_type':         inst['InstanceType'],
                            'state':                 state,
                            'name':                  name,
                            'launch_time':           inst.get('LaunchTime'),
                            'purchase_option':       purchase,
                            'tags':                  all_tags,
                            'iam_user':              iam_user,
                            'state_transition_time': state_transition_time,
                        })

            if instances:
                result[region] = instances

        except ClientError:
            continue

    return result


# ---------------------------------------------------------------------------
# 미사용 EBS 수집
# ---------------------------------------------------------------------------

def collect_unused_ebs(regions: list) -> list:
    """
    미사용(available 상태) EBS 볼륨을 수집한다.

    제외 조건 (설계 문서 기준):
        - 생성 1일 미만
        - kubernetes.io 태그 포함 + 생성 14일 미만
        - aws:backup:source-resource-arn 태그 포함

    Returns:
        [
            {
                'region':      str,
                'volume_id':   str,
                'volume_type': str,
                'size_gb':     int,
                'age_days':    int,
                'is_k8s':      bool,
            },
            ...
        ]
    """
    now     = datetime.now(timezone.utc)
    volumes = []

    for region in regions:
        try:
            ec2       = boto3.client('ec2', region_name=region)
            paginator = ec2.get_paginator('describe_volumes')

            for page in paginator.paginate(
                Filters=[{'Name': 'status', 'Values': ['available']}]
            ):
                for vol in page.get('Volumes', []):
                    create_time = vol.get('CreateTime')
                    if not create_time:
                        continue

                    age_hours = (now - create_time).total_seconds() / 3600
                    tags      = {t['Key']: t['Value'] for t in vol.get('Tags', [])}

                    if 'aws:backup:source-resource-arn' in tags:
                        continue
                    # if age_hours < EBS_MIN_AGE_HOURS:
                    #     continue

                    is_k8s   = any(k.startswith('kubernetes.io') for k in tags)
                    age_days = age_hours / 24

                    # if is_k8s and age_days < K8S_EBS_GRACE_DAYS:
                    #     continue

                    volumes.append({
                        'region':      region,
                        'volume_id':   vol['VolumeId'],
                        'volume_type': vol.get('VolumeType', ''),
                        'size_gb':     vol.get('Size', 0),
                        'age_days':    int(age_days),
                        'is_k8s':      is_k8s,
                        'iam_user':    tags.get('aws:createdBy', ''),
                    })

        except ClientError:
            continue

    return volumes


# ---------------------------------------------------------------------------
# 미사용 Snapshot 수집
# ---------------------------------------------------------------------------

def collect_unused_snapshots(regions: list, account_id: str) -> list:
    """
    AMI에서 참조되지 않는 미사용 Snapshot을 수집한다.

    제외 조건:
        - describe_images(Owners=['self'])의 BlockDeviceMappings에 포함된 SnapshotId
        - aws:backup:source-resource-arn 태그 포함
        - Description에 'Created by AWS Backup' 포함
        - State == 'pending'

    Returns:
        [
            {
                'region':      str,
                'snapshot_id': str,
                'size_gb':     int,
                'age_days':    int,
                'has_tags':    bool,
            },
            ...
        ]
    """
    now       = datetime.now(timezone.utc)
    snapshots = []

    for region in regions:
        try:
            ec2 = boto3.client('ec2', region_name=region)

            ami_snap_ids = set()
            ami_resp     = ec2.describe_images(Owners=['self'])

            for ami in ami_resp.get('Images', []):
                for bdm in ami.get('BlockDeviceMappings', []):
                    snap_id = bdm.get('Ebs', {}).get('SnapshotId')
                    if snap_id:
                        ami_snap_ids.add(snap_id)

            paginator = ec2.get_paginator('describe_snapshots')
            for page in paginator.paginate(OwnerIds=[account_id]):
                #print("Snapshots")
                #pprint(page.get('Snapshots', []))

                for snap in page.get('Snapshots', []):
                    if snap.get('State') == 'pending':
                        continue
                    if snap['SnapshotId'] in ami_snap_ids:
                        continue

                    tags = {t['Key']: t['Value'] for t in snap.get('Tags', [])}
                    desc = snap.get('Description', '')

                    if 'aws:backup:source-resource-arn' in tags:
                        continue
                    if 'Created by AWS Backup' in desc:
                        continue

                    start_time = snap.get('StartTime')
                    age_days   = int((now - start_time).total_seconds() / 86400) if start_time else 0

                    snapshots.append({
                        'region':      region,
                        'snapshot_id': snap['SnapshotId'],
                        'size_gb':     snap.get('VolumeSize', 0),
                        'age_days':    age_days,
                        'has_tags':    bool(tags),
                        'iam_user':    tags.get('aws:createdBy', ''),
                        'name':        tags.get('Name', ''),
                    })

        except ClientError:
            continue

    return snapshots


# ---------------------------------------------------------------------------
# EC2 비용 (인스턴스 타입별)
# ---------------------------------------------------------------------------

def collect_ec2_cost_by_type(ce, period: dict) -> dict:
    """
    D-1 기간 EC2 인스턴스 타입 + 리전별 비용.

    Filter: SERVICE = 'Amazon EC2'
    GroupBy: INSTANCE_TYPE + REGION (2개 제한 준수)

    Returns:
        {instance_type: {region: float}}
    """
    resp = ce.get_cost_and_usage(
        TimePeriod=period,
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'INSTANCE_TYPE'},
            {'Type': 'DIMENSION', 'Key': 'REGION'},
        ],
    )
    result = {}
    for group in resp.get('ResultsByTime', [{}])[0].get('Groups', []):
        itype  = group['Keys'][0]
        if itype == 'NoInstanceType':   # EC2 외 서비스(EBS, 데이터 전송 등)는 제외
            continue
        region = group['Keys'][1]
        amount = float(group['Metrics']['UnblendedCost']['Amount'])
        if amount <= 0:
            continue
        result.setdefault(itype, {})
        result[itype][region] = result[itype].get(region, 0.0) + amount
    return result


def collect_ec2_cost_by_type_mtd(ce, period_mtd: dict) -> dict:
    """
    MTD 기간 EC2 인스턴스 타입 + 리전별 비용.

    Returns:
        {instance_type: {region: float}}
    """
    if period_mtd['Start'] >= period_mtd['End']:
        return {}
    resp = ce.get_cost_and_usage(
        TimePeriod=period_mtd,
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'INSTANCE_TYPE'},
            {'Type': 'DIMENSION', 'Key': 'REGION'},
        ],
    )
    result = {}
    for group in resp.get('ResultsByTime', [{}])[0].get('Groups', []):
        itype  = group['Keys'][0]
        if itype == 'NoInstanceType':
            continue
        region = group['Keys'][1]
        amount = float(group['Metrics']['UnblendedCost']['Amount'])
        if amount <= 0:
            continue
        result.setdefault(itype, {})
        result[itype][region] = result[itype].get(region, 0.0) + amount
    return result


# ---------------------------------------------------------------------------
# 일괄 수집
# ---------------------------------------------------------------------------

def collect_all(regions: list, account_id: str, ce, period_d1: dict) -> dict:
    """
    Main 2 + 스레드에 필요한 EC2 데이터를 수집한다.

    Args:
        regions:    session.get_available_regions('ec2')
        account_id: STS get_caller_identity Account
        ce:         boto3 ce 클라이언트
        period_d1:  D-1 TimePeriod dict

    Returns:
        {
            'instances':        dict,  # {region: [inst, ...]}
            'unused_ebs':       list,
            'unused_snapshots': list,
            'type_cost':        dict,  # {itype: {region: float}} D-1
            'type_cost_mtd':    dict,  # {itype: {region: float}} MTD
        }
    """
    from datetime import date as _date
    d1_date    = _date.fromisoformat(period_d1['Start'])
    period_mtd = {
        'Start': d1_date.replace(day=1).strftime('%Y-%m-%d'),
        'End':   period_d1['End'],
    }
    return {
        'instances':        collect_instances(regions),
        'unused_ebs':       collect_unused_ebs(regions),
        'unused_snapshots': collect_unused_snapshots(regions, account_id),
        'type_cost':        collect_ec2_cost_by_type(ce, period_d1),
        'type_cost_mtd':    collect_ec2_cost_by_type_mtd(ce, period_mtd),
    }
