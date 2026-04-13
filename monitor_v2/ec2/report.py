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

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pprint import pprint

import boto3

from ..utils.blocks import (
    header as _header, section as _section, divider as _divider,
    context as _context, md_table_blocks as _md_table_blocks,
    table_section as _table_section, fields_section as _fields_section,
    split_by_aggregate as _split_by_aggregate,
    calc_change, fmt_change, EC2_SERVICES,
)
from .iam_resolver import build_instance_creator_map, get_slack_user_id
from ..slack import client as slack

ACCOUNT_NAME        = os.environ.get('ACCOUNT_NAME', 'hyu-ddps')
STOPPED_DM_HOURS    = 24
SNAPSHOT_ALERT_DAYS = 60
KST                 = timezone(timedelta(hours=9))

_BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'amazon.nova-micro-v1:0')
_BEDROCK_REGION   = os.environ.get('BEDROCK_REGION', 'us-east-1')

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spot 절감 AI 요약 (CUR 버전 전용)
# ---------------------------------------------------------------------------

_SPOT_SYSTEM_PROMPT = """\
당신은 AWS EC2 비용 최적화 분석 도우미입니다.
사용자가 On-Demand 인스턴스 사용 현황과 Spot 전환 시 절감 추정액을 제공하면,
한국어로 간결하게 요약합니다.

=== 출력 형식 ===

첫 줄: "어제 On-Demand 총비용 $X.XX, Spot 전환 시 약 $Y.YY 절감 가능 (약 Z%)입니다."
빈 줄
절감 기회 상위 항목 (절감액 큰 순, 최대 5개):
사용자명 — 인스턴스타입  실 비용 $X.XX → Spot 추정 ~$Y.YY  (▼ Z%)

=== 규칙 ===
절감액이 $0.01 미만인 항목은 제외하세요.
생성자 이름은 반드시 포함하세요.
금액은 양수만 표시하세요.
인스턴스 ID(i-xxx) 포함 금지.
마크다운(** * #) 사용 금지."""


def _build_spot_user_message(
    instance_cost: dict,
    spot_prices: dict,
    d1_date,
) -> str | None:
    """
    Bedrock에 전달할 Spot 절감 분석 user message 구성.
    절감 기회가 전혀 없으면 None 반환.
    """
    total_od   = 0.0
    total_spot = 0.0
    lines      = []

    for iid, ic in instance_cost.items():
        itype     = ic.get('instance_type', '')
        region    = ic.get('region', '')
        od_cost   = ic.get('cost', 0.0)
        usage_hrs = ic.get('usage_hours', 0.0)
        iam_user  = ic.get('iam_user', '') or '(생성자 미상)'

        if not itype or not region or usage_hrs <= 0:
            continue

        sp_hr = spot_prices.get(itype, {}).get(region)
        if not sp_hr:
            continue

        spot_est = usage_hrs * sp_hr
        savings  = od_cost - spot_est

        total_od   += od_cost
        total_spot += spot_est

        if savings > 0.01:
            pct = savings / od_cost * 100 if od_cost > 0 else 0
            lines.append({
                'iam_user': iam_user,
                'itype':    itype,
                'region':   region,
                'od_cost':  od_cost,
                'spot_est': spot_est,
                'savings':  savings,
                'pct':      pct,
            })

    if not lines:
        return None

    lines.sort(key=lambda x: x['savings'], reverse=True)

    total_savings = total_od - total_spot
    total_pct     = (total_savings / total_od * 100) if total_od > 0 else 0

    detail_text = '\n'.join(
        f"- {l['iam_user']}: {l['itype']} ({l['region']}) | "
        f"실 비용: ${l['od_cost']:.2f} | Spot 추정: ~${l['spot_est']:.2f} | "
        f"절감 가능: ~${l['savings']:.2f} ({l['pct']:.0f}%)"
        for l in lines[:10]
    )

    return (
        f"어제({d1_date}) EC2 On-Demand 인스턴스 사용 현황\n"
        f"전체 On-Demand 비용: ${total_od:.2f}\n"
        f"Spot 전환 시 예상 비용: ~${total_spot:.2f}\n"
        f"절감 가능 총액: ~${total_savings:.2f} ({total_pct:.0f}%)\n\n"
        f"인스턴스별 현황:\n{detail_text}\n\n"
        f"위 데이터를 요약하세요."
    )


def _generate_spot_ai_summary(
    instance_cost: dict,
    spot_prices: dict,
    d1_date,
) -> str:
    """
    Nova Micro로 Spot 절감 기회 요약 생성.
    절감 기회 없거나 Bedrock 실패 시 빈 문자열 반환.
    """
    user_message = _build_spot_user_message(instance_cost, spot_prices, d1_date)
    if not user_message:
        return ''

    try:
        bedrock = boto3.client('bedrock-runtime', region_name=_BEDROCK_REGION)
        body    = json.dumps({
            'system':   [{'text': _SPOT_SYSTEM_PROMPT}],
            'messages': [{'role': 'user', 'content': [{'text': user_message}]}],
            'inferenceConfig': {'max_new_tokens': 400, 'temperature': 0},
        })
        resp   = bedrock.invoke_model(
            modelId=_BEDROCK_MODEL_ID,
            body=body,
            contentType='application/json',
            accept='application/json',
        )
        result = json.loads(resp['body'].read())
        return result['output']['message']['content'][0]['text'].strip()

    except Exception as e:
        log.error("Spot AI 요약 Bedrock 호출 실패: %s", e)
        return ''


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

def _arrow(delta: float) -> str:
    return "▲" if delta >= 0 else "▼"


def _top5_type_blocks(type_cost: dict) -> list:
    """인스턴스 타입 합계 Top 5 section 블록 반환."""
    totals = {itype: sum(r.values()) for itype, r in type_cost.items()}
    sorted_types = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:5]
    return [
        _section(f"*{rank}. {t}* — `${c:,.2f}`")
        for rank, (t, c) in enumerate(sorted_types, 1)
    ] or [_section("_(데이터 없음)_")]


def _build_main2(
    d1_date,
    ec2_type_cost: dict,
    ec2_type_cost_mtd: dict,
    ec2_d1: float,
    ec2_d2: float,
    ec2_mtd: float,
    ec2_user_mtd: dict,
    spot_d1: float = 0.0,
    spot_d2: float = 0.0,
    spot_mtd: float = 0.0,
    spot_ai_summary: str = '',
) -> list:
    """
    비용이 발생한 리전만 표시한다 (stopped 전용 리전 = $0, 미포함).
    Top 5 인스턴스 타입: D-1 기준 + MTD 기준 각각 표시.
    Top 5 IAM User는 MTD EC2 비용 기준으로 표시.
    """
    d2_date = d1_date - timedelta(days=1)
    d, p    = calc_change(ec2_d1, ec2_d2)
    sd, sp  = calc_change(spot_d1, spot_d2)

    # 비용이 발생한 리전만 (D-1 type_cost 기준)
    region_totals: dict = {}
    for itype, regions in ec2_type_cost.items():
        for region, cost in regions.items():
            region_totals[region] = region_totals.get(region, 0.0) + cost
    sorted_regions = sorted(region_totals.items(), key=lambda x: x[1], reverse=True)

    top5_users = sorted(ec2_user_mtd.items(), key=lambda x: x[1], reverse=True)[:5]

    # EC2 비용 fields
    cost_fields = [
        f"*{d1_date}*\n`${ec2_d1:,.2f}`",
        f"*{d2_date}*\n`${ec2_d2:,.2f}` _{_arrow(d)} {fmt_change(d, p)}_",
    ]

    # Spot 비용 fields
    spot_fields = [
        f"*{d1_date}*\n`${spot_d1:,.2f}`",
        f"*{d2_date}*\n`${spot_d2:,.2f}` _{_arrow(sd)} {fmt_change(sd, sp)}_",
    ]

    # 리전 fields (2열 그리드, 10개씩 분할)
    region_field_items = [f"*{r}*\n`${c:,.2f}`" for r, c in sorted_regions]
    region_blocks = (
        [
            _fields_section(region_field_items[i:i + 10])
            for i in range(0, len(region_field_items), 10)
        ]
        if region_field_items
        else [_section("_(비용 발생 리전 없음)_")]
    )

    # Top 5 User sections
    user_blocks = [
        _section(f"*{rank}. {_shorten_creator(c)}* — `${cost:,.2f}`")
        for rank, (c, cost) in enumerate(top5_users, 1)
        if cost > 0
    ] or [_section("_(데이터 없음)_")]

    blocks = [
        _header(f"EC2 Instance Report  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _section("*[ EC2 비용 ]*"),
        _fields_section(cost_fields),
        _section(f"*당월 누계* — `${ec2_mtd:,.2f}`"),
        _divider(),
        _section("*[ Spot Instance 비용 ]*"),
        _fields_section(spot_fields),
        _section(f"*당월 누계* — `${spot_mtd:,.2f}`"),
        _divider(),
        _section(f"*[ 비용이 발생한 활성 Region ({len(region_totals)}개) ]*"),
        *region_blocks,
        _divider(),
        _section(f"*[ Top 5 인스턴스 타입  {d1_date} ]*"),
        *_top5_type_blocks(ec2_type_cost),
        _divider(),
        _section("*[ Top 5 인스턴스 타입  MTD ]*"),
        *_top5_type_blocks(ec2_type_cost_mtd),
        _divider(),
        _section("*[ Top 5 IAM User (EC2 MTD) ]*"),
        *user_blocks,
        _divider(),
        _context("전체 인스턴스 상세는 스레드에서 확인하세요."),
    ]

    if spot_ai_summary:
        blocks.extend([
            _divider(),
            _section("*[ Spot 전환 절감 기회 AI 요약 ]*"),
            _section(spot_ai_summary),
        ])

    return blocks


# ---------------------------------------------------------------------------
# Thread 1: 전체 인스턴스 상세 — 리전별 코드 블록
# ---------------------------------------------------------------------------

def _cost_comparison(
    iid: str,
    itype: str,
    region: str,
    purchase: str,
    instance_cost: dict,
    spot_prices: dict,
) -> str:
    """
    On-Demand 인스턴스의 실 비용 vs Spot 추정 비용 비교 문자열.

    공식: spot_estimate = CUR usage_hours × describe_spot_price_history 리전 평균 단가
    절감액이 없거나 데이터 부족 시 실 비용만 표시.
    """
    if purchase != 'On-Demand':
        return '-'
    ic = instance_cost.get(iid)
    if not ic or ic.get('usage_hours', 0) <= 0:
        return '-'
    od_cost = ic['cost']
    sp_hr   = spot_prices.get(itype, {}).get(region)
    if not sp_hr:
        return f"${od_cost:.2f}"
    spot_est = ic['usage_hours'] * sp_hr
    savings  = od_cost - spot_est
    if savings <= 0.01:
        return f"${od_cost:.2f}"
    pct = savings / od_cost * 100 if od_cost > 0 else 0
    return f"${od_cost:.2f}|Spot~${spot_est:.2f}(▼{pct:.0f}%)"


def _format_region_instances_blocks(
    region: str,
    instances: list,
    creator_map: dict,
    dm_targets: list,
    now: datetime,
    instance_cost: dict | None = None,
    spot_prices: dict | None = None,
) -> list:
    """
    리전의 인스턴스를 region >> 구매유형 >> 상태 계층으로 Markdown 테이블 블록으로 포매팅.
    업타임: hh:mm:ss (running), 실행시점 + 종료시점 KST 표시.

    Args (CUR 전용):
        instance_cost: {instance_id: {'cost','usage_hours','instance_type','region','iam_user'}}
        spot_prices:   {instance_type: {region: float}}  시간당 Spot 단가
        두 인자가 모두 존재할 때 "비용 비교" 컬럼이 테이블에 추가된다.
    """
    has_cost_cmp = bool(instance_cost)

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

                row = [name, iid, itype, launch_str, time_col, short]
                if has_cost_cmp:
                    row.append(_cost_comparison(
                        iid, itype, region, purchase,
                        instance_cost or {}, spot_prices or {},
                    ))
                rows.append(row)

            headers = ["이름", "ID", "타입", "시작", "업타임/종료", "생성자"]
            if has_cost_cmp:
                headers.append("비용 비교")

            blocks.extend(_table_section(
                f"*[{purchase}]  {state} ({len(inst_list)}개)*",
                headers,
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
    #print("cost_data")
    #pprint(cost_data)

    #print("ec2_data")
    #pprint(ec2_data)
    ec2_d1  = sum(v for k, v in cost_data['daily_d1'].items() if k in EC2_SERVICES)
    ec2_d2  = sum(v for k, v in cost_data['daily_d2'].items() if k in EC2_SERVICES)
    ec2_mtd = sum(
        sum(regions.values())
        for svc, regions in cost_data.get('by_region_mtd', {}).items()
        if svc in EC2_SERVICES
    )
    ec2_user_mtd      = _ec2_by_user(cost_data.get('by_creator_mtd', {}))
    ec2_type_cost_mtd = ec2_data.get('type_cost_mtd', {})
    spot_d1           = ec2_data.get('spot_d1', 0.0)
    spot_d2           = ec2_data.get('spot_d2', 0.0)
    spot_mtd          = ec2_data.get('spot_mtd', 0.0)
    instance_cost     = ec2_data.get('instance_cost', {})
    spot_prices       = ec2_data.get('spot_prices', {})

    # Spot 절감 AI 요약 (CUR 버전에서만 — instance_cost 존재 시)
    spot_ai_summary = ''
    if instance_cost and spot_prices:
        spot_ai_summary = _generate_spot_ai_summary(instance_cost, spot_prices, d1_date)

    # Main 2
    main2_ts = slack.post_blocks(
        _build_main2(
            d1_date,
            ec2_data['type_cost'],
            ec2_type_cost_mtd,
            ec2_d1, ec2_d2, ec2_mtd,
            ec2_user_mtd,
            spot_d1, spot_d2, spot_mtd,
            spot_ai_summary,
        ),
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
                f"각 리전은 코드 블록으로 표시됩니다."
            ),
        ],
        fallback_text="Thread 1: EC2 인스턴스 상세",
        thread_ts=main2_ts,
    )

    # Thread 1: 리전별 Markdown 테이블 발송
    now        = datetime.now(timezone.utc)
    dm_targets = []

    for region, instances in sorted(ec2_data['instances'].items()):
        region_blocks = [_header(_region_label(region))]
        region_blocks.extend(_format_region_instances_blocks(
            region, instances, creator_map, dm_targets, now,
            instance_cost, spot_prices,
        ))

        for batch in _split_by_aggregate(region_blocks):
            slack.post_blocks(
                batch,
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
