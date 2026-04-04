"""
monitor_v2/ec2/report.py

Main 2 메시지 (EC2 전용) + 스레드 3개를 Block Kit으로 순차 발송하고 조건부 DM을 발송한다.

발송 순서:
    1. Main 2   — EC2 총 비용 (당일/전일/MTD) + 비용 발생 리전 + Top 5 인스턴스 타입 + Top 5 IAM User
    2. Thread 1 — 전체 인스턴스 상세 (리전별 코드 블록, Slack 자동 '더 보기' 토글)
    3. Thread 2 — 미사용 리소스 목록 (EBS + Snapshot)
    4. Thread 3 — IAM User별 EC2 비용 분석 + 예상 비용

환경변수:
    ACCOUNT_NAME: AWS 계정 별칭 (표시용)
"""

import os
from datetime import datetime, timedelta, timezone
from ..utils.blocks import (
    header as _header, section as _section, divider as _divider,
    context as _context, md_table_blocks as _md_table_blocks,
    table_section as _table_section,
    calc_change, fmt_change, EC2_SERVICES,
)
from .iam_resolver import build_instance_creator_map, get_slack_user_id
from ..slack import client as slack

ACCOUNT_NAME        = os.environ.get('ACCOUNT_NAME', 'hyu-ddps')
STOPPED_DM_HOURS    = 24
SNAPSHOT_ALERT_DAYS = 60
KST                 = timezone(timedelta(hours=9))


def _region_label(region: str) -> str:
    return f"{region}"


def _uptime_str(launch_time) -> str:
    """업타임 hh:mm:ss 형식."""
    if not launch_time:
        return '-'
    now    = datetime.now(timezone.utc)
    delta  = now - launch_time
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _fmt_dt(dt) -> str:
    """datetime → KST 날짜+시간 문자열. None이면 '-'."""
    if not dt:
        return '-'
    return dt.astimezone(KST).strftime('%Y-%m-%d %H:%M KST')


def _shorten_creator(creator: str) -> str:
    """
    - IAMUser:xxx:alice       → alice
    - AssumedRole:xxx:SvcName → AssumedRole:SvcName
    - arn:aws:...             → 마지막 / 이후 토큰
    - 그 외                    → 원본
    """
    if creator.startswith('arn:aws:'):
        return creator.split('/')[-1]
    parts = creator.split(':')
    if creator.startswith('IAMUser'):
        return parts[-1]
    if creator.startswith('AssumedRole') and len(parts) >= 3:
        return f"{parts[0]}:{parts[2]}"
    return creator


def _ec2_by_user(by_creator: dict) -> dict:
    """EC2 서비스만 필터링한 creator별 합산 비용."""
    totals = {}
    for svc, creators in by_creator.items():
        if svc in EC2_SERVICES:
            for creator, cost in creators.items():
                totals[creator] = totals.get(creator, 0.0) + cost
    return totals


# ---------------------------------------------------------------------------
# Main 2 본문
# ---------------------------------------------------------------------------

def _build_main2(
    d1_date,
    ec2_type_cost: dict,
    ec2_d1: float,
    ec2_d2: float,
    ec2_mtd: float,
    ec2_user_mtd: dict,
) -> list:
    """
    비용이 발생한 리전만 표시한다 (stopped 전용 리전 = $0, 미포함).
    Top 5 IAM User는 MTD EC2 비용 기준으로 표시.
    """
    d2_date = d1_date - timedelta(days=1)
    d, p    = calc_change(ec2_d1, ec2_d2)

    # 비용이 발생한 리전만
    region_totals: dict = {}
    for itype, regions in ec2_type_cost.items():
        for region, cost in regions.items():
            region_totals[region] = region_totals.get(region, 0.0) + cost
    sorted_regions = sorted(region_totals.items(), key=lambda x: x[1], reverse=True)

    type_totals  = {itype: sum(r.values()) for itype, r in ec2_type_cost.items()}
    sorted_types = sorted(type_totals.items(), key=lambda x: x[1], reverse=True)[:5]

    top5_users = sorted(ec2_user_mtd.items(), key=lambda x: x[1], reverse=True)[:5]

    region_rows = [[r, f"${c:,.2f}"] for r, c in sorted_regions]
    type_rows   = [
        [str(rank), t, f"${c:,.2f}"]
        for rank, (t, c) in enumerate(sorted_types, 1)
    ]
    user_rows = [
        [str(rank), _shorten_creator(c), f"${cost:,.2f}"]
        for rank, (c, cost) in enumerate(top5_users, 1)
        if cost > 0
    ] or [["(데이터 없음)", "", ""]]

    return [
        _header(f"EC2 Instance Report  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _divider(),
        *_table_section(
            "*[ EC2 비용 ]*",
            ["날짜", "비용", "변화"],
            [
                [str(d1_date), f"${ec2_d1:,.2f}", ""],
                [str(d2_date), f"${ec2_d2:,.2f}", fmt_change(d, p)],
                ["당월 누계",   f"${ec2_mtd:,.2f}", ""],
            ],
        ),
        _divider(),
        *_table_section(
            f"*[ 비용이 발생한 활성 Region ({len(region_totals)}개) ]*",
            ["리전", "비용"],
            region_rows,
        ),
        _divider(),
        *_table_section("*[ Top 5 인스턴스 타입 ]*", ["#", "타입", "비용"], type_rows),
        _divider(),
        *_table_section("*[ Top 5 IAM User (EC2 MTD) ]*", ["#", "사용자", "비용"], user_rows),
        _divider(),
        _context("전체 인스턴스 상세는 스레드에서 확인하세요."),
    ]


# ---------------------------------------------------------------------------
# Thread 1: 전체 인스턴스 상세 — 리전별 코드 블록
# ---------------------------------------------------------------------------

def _format_region_instances_blocks(
    region: str,
    instances: list,
    creator_map: dict,
    dm_targets: list,
    now: datetime,
) -> list:
    """
    리전의 인스턴스를 region >> 구매유형 >> 상태 계층으로 Markdown 테이블 블록으로 포매팅.
    업타임: hh:mm:ss (running), 실행시점 + 종료시점 KST 표시.
    """
    by_purchase: dict = {}
    for inst in instances:
        by_purchase.setdefault(inst['purchase_option'], {}).setdefault(inst['state'], []).append(inst)

    blocks = []

    for purchase in ['On-Demand', 'Spot']:
        if purchase not in by_purchase:
            continue

        for state in ['running', 'stopped', 'terminated']:
            if state not in by_purchase[purchase]:
                continue
            inst_list = by_purchase[purchase][state]
            rows      = []

            for inst in inst_list:
                iid     = inst['instance_id']
                itype   = inst['instance_type']
                name    = inst['name'] or iid
                creator = creator_map.get(iid, 'Unknown')
                short   = _shorten_creator(creator)

                launch_str = _fmt_dt(inst.get('launch_time'))

                if state == 'running':
                    uptime   = _uptime_str(inst.get('launch_time'))
                    time_col = f"업타임: {uptime}"
                else:
                    t        = inst.get('state_transition_time') or inst.get('launch_time')
                    stop_str = _fmt_dt(t)
                    time_col = f"종료: {stop_str}"

                    if state == 'stopped' and t:
                        stopped_hours = (now - t).total_seconds() / 3600
                        if stopped_hours >= STOPPED_DM_HOURS:
                            time_col += f" (!{int(stopped_hours)}h 경과)"
                            dm_targets.append({
                                'creator':     creator,
                                'instance_id': iid,
                                'name':        name,
                                'reason':      f'stopped {int(stopped_hours)}시간 경과',
                            })

                rows.append([name, iid, itype, launch_str, time_col, short])

            blocks.extend(_table_section(
                f"*[{purchase}]  {state} ({len(inst_list)}개)*",
                ["이름", "ID", "타입", "시작", "업타임/종료", "생성자"],
                rows,
            ))

    return blocks


# ---------------------------------------------------------------------------
# Thread 2: 미사용 리소스 목록
# ---------------------------------------------------------------------------

def _build_thread2_unused(
    unused_ebs: list,
    unused_snapshots: list,
    stopped_instances: list,
    creator_map: dict,
) -> list:
    ebs_rows = [
        [
            vol['region'],
            vol['volume_id'],
            vol['volume_type'],
            f"{vol['size_gb']}GB",
            f"{vol['age_days']}일",
            ('kubernetes  ' if vol['is_k8s'] else '') + (
                _shorten_creator(vol['iam_user']) if vol.get('iam_user') else ''
            ),
        ]
        for vol in sorted(unused_ebs, key=lambda x: x['age_days'], reverse=True)
    ] or [["없음", "", "", "", "", ""]]

    snap_rows = [
        [
            snap['region'],
            snap['snapshot_id'],
            f"{snap['size_gb']}GB",
            f"{snap['age_days']}일",
            snap.get('name') or '태그 없음',
        ]
        for snap in sorted(unused_snapshots, key=lambda x: x['age_days'], reverse=True)
    ] or [["없음", "", "", "", ""]]

    def _stopped_sort_key(inst):
        t = inst.get('state_transition_time') or inst.get('launch_time')
        return t if t else datetime.min.replace(tzinfo=timezone.utc)

    stopped_rows = [
        [
            inst['region'],
            inst['name'] or inst['instance_id'],
            inst['instance_id'],
            inst['instance_type'],
            inst['purchase_option'],
            _fmt_dt(inst.get('state_transition_time') or inst.get('launch_time')),
            _shorten_creator(
                creator_map.get(inst['instance_id'])
                or inst.get('iam_user')
                or 'Unknown'
            ),
        ]
        for inst in sorted(stopped_instances, key=_stopped_sort_key)
    ] or [["없음", "", "", "", "", "", ""]]

    return [
        _header("Thread 2  |  미사용 리소스"),
        _divider(),
        *_table_section(
            f"*[ Stopped 인스턴스 ({len(stopped_instances)}개) ]*",
            ["리전", "이름", "ID", "타입", "구매", "중지 시각", "생성자"],
            stopped_rows,
        ),
        _divider(),
        *_table_section(
            f"*[ 미사용 EBS 볼륨 ({len(unused_ebs)}개) ]*",
            ["리전", "볼륨 ID", "타입", "크기", "경과", "비고 (생성자)"],
            ebs_rows,
        ),
        _divider(),
        *_table_section(
            f"*[ 미사용 Snapshot ({len(unused_snapshots)}개) ]*",
            ["리전", "스냅샷 ID", "크기", "경과", "비고 (Name 태그)"],
            snap_rows,
        ),
    ]


# ---------------------------------------------------------------------------
# Thread 3: IAM User별 EC2 비용 분석 + 예상 비용
# ---------------------------------------------------------------------------

def _build_thread3_ec2_by_user(
    by_creator: dict,
    by_creator_mtd: dict,
    ec2_mtd: float,
    mtd_this: float,
    forecast: float,
    d1_date=None,
) -> list:
    d1_totals  = _ec2_by_user(by_creator)
    mtd_totals = _ec2_by_user(by_creator_mtd)

    # EC2가 전체 MTD에서 차지하는 비율로 EC2 잔여 예측 산출
    ec2_forecast = (forecast * ec2_mtd / mtd_this) if (forecast > 0 and mtd_this > 0) else 0.0

    all_creators = set(d1_totals) | set(mtd_totals)
    sorted_rows  = sorted(
        [(c, d1_totals.get(c, 0.0), mtd_totals.get(c, 0.0)) for c in all_creators],
        key=lambda x: x[2],
        reverse=True,
    )

    table_rows = []
    for c, d1, mtd in sorted_rows:
        if d1 <= 0 and mtd <= 0:
            continue
        if ec2_forecast > 0 and ec2_mtd > 0:
            user_fc   = ec2_forecast * (mtd / ec2_mtd)
            projected = mtd + user_fc
            fc_str    = f"${projected:,.2f} _(+${user_fc:,.2f})_"
        else:
            fc_str = "-"
        table_rows.append([_shorten_creator(c), f"${d1:,.4f}", f"${mtd:,.2f}", fc_str])

    if not table_rows:
        table_rows = [["(데이터 없음)", "", "", ""]]

    date_label = str(d1_date) if d1_date else "당일"
    blocks = [
        _header("Thread 3  |  IAM User별 EC2 비용"),
        _divider(),
        *_table_section(
            f"*[ EC2 비용 ({date_label} / MTD / 이달 예상) ]*",
            ["사용자", date_label, "MTD", "이달 예상"],
            table_rows,
        ),
    ]
    if ec2_forecast <= 0:
        blocks.append(_context("* 잔여 예측 없음 — CE forecast API 요청 실패 또는 당월 1일"))
    return blocks


# ---------------------------------------------------------------------------
# DM 발송
# ---------------------------------------------------------------------------

def _send_dms(dm_targets: list) -> None:
    for target in dm_targets:
        slack_uid = get_slack_user_id(target['creator'])
        if not slack_uid:
            print(f"[DM 스킵] IAM_SLACK_USER_MAP 미등록: {target['creator']}")
            continue

        msg = (
            f"미사용 리소스 알림\n"
            f"인스턴스: {target['name']} ({target['instance_id']})\n"
            f"사유: {target['reason']}\n"
            f"정리해주시길 부탁드립니다."
        )
        slack.send_dm(slack_uid, msg)


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------

def send_main2_report(cost_data: dict, ec2_data: dict) -> None:
    """
    Main 2 + 스레드 3개를 Block Kit으로 순차 발송하고, 조건부 DM을 발송한다.

    Thread 1은 리전별로 분리 발송한다. 각 리전은 코드 블록(rich_text_preformatted)으로
    표시되며, 내용이 길면 Slack이 자동으로 '간략히 보기 / 더 보기' 토글을 추가한다.

    Args:
        cost_data: cost/data.py collect_all()의 반환값
        ec2_data:  ec2/data.py collect_all()의 반환값
    """
    d1_date = cost_data['d1_date']
    ec2_d1  = sum(v for k, v in cost_data['daily_d1'].items() if k in EC2_SERVICES)
    ec2_d2  = sum(v for k, v in cost_data['daily_d2'].items() if k in EC2_SERVICES)
    ec2_mtd = sum(
        sum(regions.values())
        for svc, regions in cost_data.get('by_region_mtd', {}).items()
        if svc in EC2_SERVICES
    )
    ec2_user_mtd = _ec2_by_user(cost_data.get('by_creator_mtd', {}))

    # Main 2
    main2_ts = slack.post_blocks(
        _build_main2(d1_date, ec2_data['type_cost'], ec2_d1, ec2_d2, ec2_mtd, ec2_user_mtd),
        fallback_text=f"EC2 Instance Report {d1_date} / {ACCOUNT_NAME}",
    )

    # creator_map 조회 (CloudTrail 기반)
    all_instance_ids = [
        inst['instance_id']
        for instances in ec2_data['instances'].values()
        for inst in instances
    ]
    regions     = list(ec2_data['instances'].keys())
    creator_map = build_instance_creator_map(all_instance_ids, regions)

    # Thread 1: 헤더 메시지
    total_instances = sum(len(v) for v in ec2_data['instances'].values())
    slack.post_blocks(
        [
            _header(f"Thread 1  |  EC2 인스턴스 상세  |  {ACCOUNT_NAME}"),
            _divider(),
            _context(
                f"활성 리전: {len(ec2_data['instances'])}개  |  "
                f"총 인스턴스: {total_instances}개  |  "
                f"각 리전은 코드 블록으로 표시됩니다 (내용이 길면 Slack이 자동으로 더 보기 추가)"
            ),
        ],
        fallback_text="Thread 1: EC2 인스턴스 상세",
        thread_ts=main2_ts,
    )

    # Thread 1: 리전별 Markdown 테이블 발송
    now        = datetime.now(timezone.utc)
    dm_targets = []

    for region, instances in sorted(ec2_data['instances'].items()):
        region_blocks = [_section(f"*{_region_label(region)}*")]
        region_blocks.extend(_format_region_instances_blocks(region, instances, creator_map, dm_targets, now))

        slack.post_blocks(
            region_blocks,
            fallback_text=_region_label(region),
            thread_ts=main2_ts,
        )

    # Thread 2: 미사용 리소스
    stopped_instances = [
        {'region': region, **inst}
        for region, instances in ec2_data['instances'].items()
        for inst in instances
        if inst['state'] == 'stopped'
    ]
    slack.post_blocks(
        _build_thread2_unused(
            ec2_data['unused_ebs'],
            ec2_data['unused_snapshots'],
            stopped_instances,
            creator_map,
        ),
        fallback_text="Thread 2: 미사용 리소스",
        thread_ts=main2_ts,
    )

    # Thread 3: IAM User별 EC2 비용
    slack.post_blocks(
        _build_thread3_ec2_by_user(
            cost_data.get('by_creator', {}),
            cost_data.get('by_creator_mtd', {}),
            ec2_mtd,
            cost_data.get('mtd_this', 0.0),
            cost_data.get('forecast', 0.0),
            d1_date=d1_date,
        ),
        fallback_text="Thread 3: IAM User별 EC2 비용",
        thread_ts=main2_ts,
    )

    _send_dms(dm_targets)
