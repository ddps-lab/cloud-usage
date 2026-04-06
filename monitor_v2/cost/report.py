"""
monitor_v2/cost/report.py

Main 1 메시지 + 스레드 3개를 Block Kit으로 순차 발송한다.

발송 순서:
    1. Main 1   — 전체 비용 요약 + Top 5 서비스
    2. Thread 1 — 계정 전체 서비스 비용 목록
    3. Thread 2 — IAM User별 비용 분석 (aws:createdBy)
    4. Thread 3 — 서비스별 리전 상세 (EC2 포함 전체 서비스)

테이블 렌더링:
    - {"type": "markdown"} 블록으로 Markdown 테이블 렌더링
    - 모든 행을 페이지 구분 없이 표시 (3000자 초과 시 자동 분할)

환경변수:
    ACCOUNT_NAME: AWS 계정 별칭 (표시용)
"""

import os
from datetime import date, timedelta
from ..slack import client as slack
from ..utils.blocks import (
    header as _header, section as _section, divider as _divider,
    context as _context, md_table_blocks as _md_table_blocks,
    table_section as _table_section, fields_section as _fields_section,
    calc_change, fmt_change, EC2_SERVICES,
)

ACCOUNT_NAME = os.environ.get('ACCOUNT_NAME', 'hyu-ddps')
TOP_N        = 5


def _top_n(costs: dict, n: int = TOP_N) -> list:
    return sorted(
        [(s, c) for s, c in costs.items() if c > 0],
        key=lambda x: x[1], reverse=True
    )[:n]


# ---------------------------------------------------------------------------
# Main 1
# ---------------------------------------------------------------------------

def _arrow(delta: float) -> str:
    return "▲" if delta >= 0 else "▼"


def _build_main1(
    d1_date: date, daily_d1: dict, daily_d2: dict,
    mtd_this: float, forecast: float = 0.0,
) -> list:
    total_d1 = sum(daily_d1.values())
    total_d2 = sum(daily_d2.values())
    d2_date  = d1_date - timedelta(days=1)
    d_yday, p_yday = calc_change(total_d1, total_d2)
    projected      = mtd_this + forecast

    # 일일 비용
    daily_fields = [
        f"*{d1_date}*\n`${total_d1:,.2f}`",
        f"*{d2_date}*\n`${total_d2:,.2f}` _{_arrow(d_yday)} {fmt_change(d_yday, p_yday)}_",
    ]

    # 월 누계
    forecast_str = (
        f"`${projected:,.2f}` _(잔여 +${forecast:,.2f})_" if forecast > 0
        else "_(예측 데이터 없음)_"
    )
    mtd_fields = [
        f"*이번달 누계*\n`${mtd_this:,.2f}`",
        f"*이달 예상*\n{forecast_str}",
    ]

    # Top 5 서비스
    top5_blocks = []
    for rank, (service, cost) in enumerate(_top_n(daily_d1), 1):
        d1, p1 = calc_change(cost, daily_d2.get(service, 0.0))
        top5_blocks.append(_section(
            f"*{rank}. {service}* — `${cost:,.2f}`\n어제대비 {_arrow(d1)} {fmt_change(d1, p1)}"
        ))

    return [
        _header(f"AWS Cost Report  |  {d1_date}  |  {ACCOUNT_NAME}"),
        _section("*[ 일일 비용 ]*"),
        _fields_section(daily_fields),
        _divider(),
        _section("*[ 월 누계 ]*"),
        _fields_section(mtd_fields),
        _divider(),
        _section(f"*[ Top {TOP_N} 서비스  {d1_date} ]*"),
        *top5_blocks,
        _divider(),
        _context("자세한 내용은 스레드에서 확인하세요."),
    ]


# ---------------------------------------------------------------------------
# Thread 1: 전체 서비스 비용 목록
# ---------------------------------------------------------------------------

def _build_thread1(d1_date: date, daily_d1: dict, daily_d2: dict) -> list:
    total_d1 = sum(daily_d1.values())
    total_d2 = sum(daily_d2.values())
    d, p     = calc_change(total_d1, total_d2)

    summary_rows = [
        [str(d1_date),                     f"${total_d1:,.2f}", ""],
        [str(d1_date - timedelta(days=1)), f"${total_d2:,.2f}", fmt_change(d, p)],
    ]
    svc_rows = [
        [service, f"${cost:,.4f}"]
        for service, cost in sorted(daily_d1.items(), key=lambda x: x[1], reverse=True)
        if cost > 0
    ]

    return [
        _header(f"Thread 1  |  서비스별 전체 비용  |  {ACCOUNT_NAME}"),
        _divider(),
        *_table_section("*[ 합계 ]*",      ["날짜", "비용", "변화"], summary_rows),
        _divider(),
        *_table_section("*[ 서비스 목록 ]*", ["서비스", "비용"],       svc_rows),
    ]


# ---------------------------------------------------------------------------
# Thread 2: IAM User별 비용 분석
# ---------------------------------------------------------------------------

def _shorten_creator(creator: str) -> str:
    """
    creator 문자열을 표시용으로 단축한다.

    - IAMUser:xxx:alice          → alice          (마지막 토큰)
    - AssumedRole:xxx:SvcName    → AssumedRole:SvcName  (1번째 + 3번째 토큰)
    - 그 외                       → 원본 그대로
    """
    parts = creator.split(':')
    if creator.startswith('IAMUser'):
        return parts[-1]
    if creator.startswith('AssumedRole') and len(parts) >= 3:
        return f"{parts[0]}:{parts[2]}"
    return creator


def _creator_rollup(data: dict) -> tuple:
    totals, services = {}, {}
    for service, creators in data.items():
        for creator, cost in creators.items():
            totals[creator] = totals.get(creator, 0.0) + cost
            services.setdefault(creator, {})
            services[creator][service] = services[creator].get(service, 0.0) + cost
    return totals, services

def _creator_table_rows(totals: dict, services: dict) -> list:
    rows = []
    for creator, total in sorted(totals.items(), key=lambda x: x[1], reverse=True):
        if total <= 0:
            continue
        short = _shorten_creator(creator)
        rows.append([f"**{short}**", "_(합계)_", f"**${total:,.2f}**"])
        for svc, cost in sorted(services[creator].items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                rows.append(["", svc, f"${cost:,.2f}"])
    return rows or [["(데이터 없음)", "", ""]]


def _build_thread2(by_creator: dict, by_creator_mtd: dict, forecast: float) -> list:
    d1_totals,  d1_svc  = _creator_rollup(by_creator)
    mtd_totals, mtd_svc = _creator_rollup(by_creator_mtd)
    total_mtd            = sum(mtd_totals.values())

    d1_rows = _creator_table_rows(d1_totals, d1_svc)

    if not mtd_totals:
        mtd_rows = [["(데이터 없음)", "", ""]]
    else:
        mtd_rows = []
        for creator, mtd_total in sorted(mtd_totals.items(), key=lambda x: x[1], reverse=True):
            if mtd_total <= 0:
                continue
            short = _shorten_creator(creator)
            mtd_rows.append([f"**{short}**", "_(MTD 합계)_", f"**${mtd_total:,.2f}**"])
            if forecast > 0 and total_mtd > 0:
                creator_fc = forecast * (mtd_total / total_mtd)
                projected  = mtd_total + creator_fc
                mtd_rows.append(["", "이달 예상", f"${projected:,.2f} _(+${creator_fc:,.2f})_"])
            for svc, cost in sorted(mtd_svc[creator].items(), key=lambda x: x[1], reverse=True):
                if cost > 0:
                    mtd_rows.append(["", svc, f"${cost:,.2f}"])

    headers = ["IAM User", "서비스", "비용"]
    blocks  = [
        _header("Thread 2  |  IAM User별 비용 분석"),
        _divider(),
        *_table_section("*[ 당일 ]*",          headers, d1_rows),
        _divider(),
        *_table_section("*[ 당월 누계 (MTD) ]*", headers, mtd_rows),
    ]
    if forecast <= 0:
        blocks.append(_context("* 잔여 예측 없음 — 현재 날짜가 아니므로 CE forecast API 요청 실패"))
    return blocks


# ---------------------------------------------------------------------------
# Thread 3: 서비스별 리전 상세
# ---------------------------------------------------------------------------

def _region_table_rows_d1(service_region_dict: dict, d2_ref: dict) -> list:
    filtered    = {s: r for s, r in service_region_dict.items() if sum(r.values()) > 0}
    sorted_svcs = sorted(filtered.items(), key=lambda x: sum(x[1].values()), reverse=True)
    rows = []
    for service, regions in sorted_svcs:
        total = sum(regions.values())
        d, p  = calc_change(total, d2_ref.get(service, 0.0))
        rows.append([f"**{service}**", "_(합계)_", f"**${total:,.2f}**", fmt_change(d, p)])
        for region, cost in sorted(regions.items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                rows.append(["", region, f"${cost:,.2f}", ""])
    return rows or [["(데이터 없음)", "", "", ""]]

def _region_table_rows_mtd(service_region_dict: dict, forecast: float) -> list:
    filtered    = {s: r for s, r in service_region_dict.items() if sum(r.values()) > 0}
    sorted_svcs = sorted(filtered.items(), key=lambda x: sum(x[1].values()), reverse=True)
    total_mtd   = sum(sum(r.values()) for r in filtered.values())
    rows = []
    for service, regions in sorted_svcs:
        svc_mtd = sum(regions.values())
        if forecast > 0 and total_mtd > 0:
            svc_fc    = forecast * (svc_mtd / total_mtd)
            projected = svc_mtd + svc_fc
            fc_str    = f"${projected:,.2f} _(+${svc_fc:,.2f})_"
        else:
            fc_str = ""
        rows.append([f"**{service}**", "_(MTD)_", f"**${svc_mtd:,.2f}**", fc_str])
        for region, cost in sorted(regions.items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                rows.append(["", region, f"${cost:,.2f}", ""])
    return rows or [["(데이터 없음)", "", "", ""]]


def _build_thread3(
    by_region: dict, by_region_mtd: dict,
    daily_d2: dict, forecast: float, d1_date: date = None,
) -> list:
    d1_rows  = _region_table_rows_d1(by_region, daily_d2)
    mtd_rows = _region_table_rows_mtd(by_region_mtd, forecast)
    d1_label = str(d1_date) if d1_date else "당일"
    headers  = ["서비스", "리전", "비용", "비교"]
    mtd_headers = ["서비스", "리전", "비용", "예상 비용"]

    blocks = [
        _header("Thread 3  |  서비스별 리전 상세"),
        _divider(),
        *_table_section(f"*[ {d1_label} ]*",       headers, d1_rows),
        _divider(),
        *_table_section("*[ 당월 누계 (MTD) ]*",    mtd_headers, mtd_rows),
    ]
    if forecast <= 0:
        blocks.append(_context("* 잔여 예측 없음 — 현재 날짜가 아니므로 CE forecast API 요청 실패"))
    return blocks


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------

def send_main1_report(cost_data: dict) -> None:
    """
    Main 1 + 스레드 3개를 Block Kit으로 순차 발송한다.

    Args:
        cost_data: cost/data.py collect_all()의 반환값
    """
    d1_date        = cost_data['d1_date']
    daily_d1       = cost_data['daily_d1']
    daily_d2       = cost_data['daily_d2']
    by_creator     = cost_data['by_creator']
    by_region      = cost_data['by_region']
    mtd_this       = cost_data['mtd_this']
    by_creator_mtd = cost_data.get('by_creator_mtd', {})
    by_region_mtd  = cost_data.get('by_region_mtd', {})
    forecast       = cost_data.get('forecast', 0.0)

    main_ts = slack.post_blocks(
        _build_main1(d1_date, daily_d1, daily_d2, mtd_this, forecast),
        fallback_text=f"AWS Cost Report {d1_date} / {ACCOUNT_NAME}",
    )
    slack.post_blocks(
        _build_thread1(d1_date, daily_d1, daily_d2),
        fallback_text="Thread 1: 서비스별 전체 비용",
        thread_ts=main_ts,
    )
    slack.post_blocks(
        _build_thread2(by_creator, by_creator_mtd, forecast),
        fallback_text="Thread 2: IAM User별 비용 분석",
        thread_ts=main_ts,
    )
    slack.post_blocks(
        _build_thread3(by_region, by_region_mtd, daily_d2, forecast, d1_date),
        fallback_text="Thread 3: 서비스별 리전 상세",
        thread_ts=main_ts,
    )
