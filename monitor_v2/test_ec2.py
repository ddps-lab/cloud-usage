"""
monitor_v2/test_ec2.py

ec2/data.py 단독 테스트 — Slack 발송 없이 수집 결과를 print.

lambda_handler.py 와 동일하게 cost_data에서 period_d1을 계산한 뒤
ec2/data.py의 collect_all()을 호출하고, 반환된 4개 키를 섹션별로 출력한다.

사용법:
    uv run python -m monitor_v2.test_ec2
    또는
    python -m monitor_v2.test_ec2
"""
import os
import sys
import boto3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from pprint import pprint

# 프로젝트 루트(cloud-usage/)를 경로에 추가 → monitor_v2 패키지 import 가능
sys.path.insert(0, str(Path(__file__).parent.parent))

from print_test.utils.environment import setup_environment
from monitor_v2.cost.data import collect_all as collect_cost_data
from monitor_v2.ec2.data  import collect_all as collect_ec2_data

SEP  = "─" * 70
SEP2 = "=" * 70
KST  = timezone(timedelta(hours=9))

REGION_ALIAS = {
    'ap-northeast-2': '서울',
    'ap-northeast-1': '도쿄',
    'us-east-1':      '버지니아',
    'us-west-2':      '오레곤',
    'eu-west-1':      '아일랜드',
    'eu-central-1':   '프랑크푸르트',
    'ap-southeast-1': '싱가포르',
}


def _region_label(region: str) -> str:
    alias = REGION_ALIAS.get(region, '')
    return f"{region} ({alias})" if alias else region


def _runtime_str(launch_time, end_time=None) -> str:
    """launch_time ~ end_time(없으면 now) 기간을 Xd Yh Zm 형식으로 반환."""
    if not launch_time:
        return 'N/A'
    ref        = end_time if end_time else datetime.now(timezone.utc)
    total_secs = int((ref - launch_time).total_seconds())
    if total_secs < 0:
        return 'N/A'
    days, rem = divmod(total_secs, 86400)
    h, rem    = divmod(rem, 3600)
    m, _      = divmod(rem, 60)
    if days > 0:
        return f"{days}d {h:02d}h {m:02d}m"
    return f"{h:02d}h {m:02d}m"


def _fmt_kst(dt) -> str:
    """UTC datetime을 'YYYY-MM-DD HH:MM KST' 문자열로 변환."""
    if not dt:
        return 'N/A'
    kst_dt = dt.astimezone(KST)
    return kst_dt.strftime('%Y-%m-%d %H:%M KST')


def _fmt_user_tags(tags: dict) -> str:
    """aws: 접두어 태그와 Name 태그를 제외한 사용자 태그를 'K=V, ...' 형식으로 반환."""
    items = [
        f"{k}={v}"
        for k, v in sorted(tags.items())
        if not k.startswith('aws:') and k != 'Name'
    ]
    return ', '.join(items) if items else ''


def _fmt_instances(instances_by_region: dict) -> None:
    """인스턴스 목록을 리전 → 상태 계층으로 출력."""
    total_count = sum(len(v) for v in instances_by_region.values())
    print(f"  총 {total_count}개 인스턴스 (인스턴스가 있는 리전만 표시)\n")

    for region, instances in sorted(instances_by_region.items()):
        print(f"  📍 {_region_label(region)}  ({len(instances)}개)")

        by_state = {}
        for inst in instances:
            by_state.setdefault(inst['state'], []).append(inst)

        for state in ['running', 'stopped', 'terminated']:
            if state not in by_state:
                continue
            print(f"    ▶ {state} ({len(by_state[state])}개)")
            for inst in by_state[state]:
                name  = inst['name'] or inst['instance_id']
                itype = inst['instance_type']
                pur   = inst['purchase_option']

                # 실행 시간 계산
                end_time = inst['state_transition_time'] if state != 'running' else None
                runtime  = _runtime_str(inst['launch_time'], end_time)

                # IAM 사용자 (aws:createdBy 파싱)
                raw_user = inst.get('iam_user', '')
                iam_user = raw_user.split(':')[-1] if raw_user.startswith('IAMUser') else raw_user

                # 시작/종료 시각
                launch_str = _fmt_kst(inst['launch_time'])
                end_str    = _fmt_kst(inst['state_transition_time']) if state != 'running' else None

                # 사용자 태그
                user_tags = _fmt_user_tags(inst.get('tags', {}))

                print(f"      {name:<30}  {inst['instance_id']}  {itype:<14}  [{pur}]")

                if state == 'running':
                    print(f"        시작: {launch_str}  │ 실행: {runtime}"
                          + (f"  │ 👤 {iam_user}" if iam_user else ''))
                else:
                    end_label = '종료' if state == 'terminated' else '중지'
                    print(f"        시작: {launch_str}"
                          + (f"  {end_label}: {end_str}" if end_str else '')
                          + f"  │ 실행: {runtime}"
                          + (f"  │ 👤 {iam_user}" if iam_user else ''))

                if user_tags:
                    print(f"        태그: {user_tags}")
        print()


def _fmt_type_cost(type_cost: dict) -> None:
    """인스턴스 타입별 비용을 총합 내림차순으로 출력."""
    type_totals = {itype: sum(r.values()) for itype, r in type_cost.items()}
    for itype, total in sorted(type_totals.items(), key=lambda x: x[1], reverse=True):
        if total <= 0:
            continue
        print(f"  {itype:<20}  ${total:>10,.4f}")
        for region, cost in sorted(type_cost[itype].items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                print(f"    ├─ {_region_label(region):<35}  ${cost:,.4f}")
    print()


def _fmt_unused_ebs(unused_ebs: list) -> None:
    if not unused_ebs:
        print("  없음\n")
        return
    for vol in sorted(unused_ebs, key=lambda x: x['age_days'], reverse=True):
        marker   = '☄️ (kubernetes)' if vol['is_k8s'] else '⚠️'
        print(
            f"  {vol['region']:<18}  {vol['volume_id']}  "
            f"{vol['volume_type']:<6}  {vol['size_gb']:>5}GB  "
            f"{vol['age_days']:>4}일  {marker}"
        )
    print()


def _fmt_unused_snapshots(unused_snapshots: list) -> None:
    ALERT_DAYS = 60
    if not unused_snapshots:
        print("  없음\n")
        return
    for snap in sorted(unused_snapshots, key=lambda x: x['age_days'], reverse=True):
        if snap['age_days'] >= ALERT_DAYS:
            marker = '🚨 60일 초과'
        elif snap['has_tags']:
            marker = '☄️ 태그 있음'
        else:
            marker = '⚠️'
        print(
            f"  {snap['region']:<18}  {snap['snapshot_id']}  "
            f"{snap['size_gb']:>5}GB  {snap['age_days']:>4}일  {marker}"
        )
    print()


def main():
    print("\n" + SEP2)
    print("  monitor_v2 / ec2/data.py — 단독 테스트")
    print(SEP2)

    setup_environment()

    today_kst = datetime.now(KST).date() - timedelta(days=1)
    print(f"\n  기준 날짜 (today_kst): {today_kst}")
    print(f"  리포트 대상 (D-1):     {today_kst - timedelta(days=1)}\n")

    d1_date = today_kst
    period_d1 = {
        'Start': d1_date.strftime('%Y-%m-%d'),
        'End':   (d1_date + timedelta(days=1)).strftime('%Y-%m-%d'),
    }
    print(f"  D-1 period: {period_d1['Start']} ~ {period_d1['End']} (End exclusive)\n")

    # ── EC2 수집 준비 ─────────────────────────────────────────────────────────
    profile = os.environ.get('AWS_PROFILE', 'default')
    session = boto3.Session(profile_name=profile)
    ce = boto3.client('ce', region_name='us-east-1')
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    ec2_client = session.client('ec2', region_name='ap-northeast-2')
    response = ec2_client.describe_regions(
        Filters=[{
            'Name': 'opt-in-status',
            'Values': ['opt-in-not-required', 'opted-in']
        }]
    )
    ec2_regions = [r['RegionName'] for r in response['Regions']]

    print(f"  AWS Account ID: {account_id}")
    print(f"  조회 리전 수:   {len(ec2_regions)}개\n")

    print("▶ ec2/data.py collect_all() 호출 중 (전 리전 순차 조회, 시간 소요)...")
    ec2_data = collect_ec2_data(ec2_regions, account_id, ce, period_d1)
    print("  완료\n")

    # ── 1. 인스턴스 목록 ─────────────────────────────────────────────────────
    print(SEP)
    print(f"[1] EC2 인스턴스 (running / stopped / terminated)")
    print(SEP)
    if ec2_data['instances']:
        _fmt_instances(ec2_data['instances'])
    else:
        print("  인스턴스 없음\n")

    # ── 2. 인스턴스 타입별 D-1 비용 ─────────────────────────────────────────
    print(SEP)
    print(f"[2] 인스턴스 타입별 D-1 비용  (period: {period_d1['Start']})")
    print(SEP)
    if ec2_data['type_cost']:
        _fmt_type_cost(ec2_data['type_cost'])
    else:
        print("  EC2 비용 없음 (해당 일 미사용)\n")

    # ── 3. 미사용 EBS ────────────────────────────────────────────────────────
    print(SEP)
    print(f"[3] 미사용 EBS 볼륨  ({len(ec2_data['unused_ebs'])}개)")
    print(SEP)
    _fmt_unused_ebs(ec2_data['unused_ebs'])

    # ── 4. 미사용 Snapshot ───────────────────────────────────────────────────
    print(SEP)
    print(f"[4] 미사용 Snapshot  ({len(ec2_data['unused_snapshots'])}개)")
    print(SEP)
    _fmt_unused_snapshots(ec2_data['unused_snapshots'])

    # ── 5. 반환 dict 요약 ────────────────────────────────────────────────────
    print(SEP)
    print(f"[5] collect_all() 반환 dict 키 목록")
    print(SEP)
    instances = ec2_data['instances']
    print(f"  'instances':        dict  (활성 리전: {len(instances)}개)")
    for region, insts in sorted(instances.items()):
        print(f"    {_region_label(region)}: {len(insts)}개")
    print(f"  'unused_ebs':       list  ({len(ec2_data['unused_ebs'])}개)")
    print(f"  'unused_snapshots': list  ({len(ec2_data['unused_snapshots'])}개)")
    print(f"  'type_cost':        dict  (인스턴스 타입: {len(ec2_data['type_cost'])}종)")
    print()

    print(SEP2)
    print("  완료 — Slack 발송 없음")
    print(SEP2 + "\n")


if __name__ == "__main__":
    main()
